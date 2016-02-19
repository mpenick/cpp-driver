/*
  Copyright (c) 2014-2015 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "session.hpp"

#include "config.hpp"
#include "constants.hpp"
#include "logger.hpp"
#include "prepare_request.hpp"
#include "request_handler.hpp"
#include "resolver.hpp"
#include "scoped_lock.hpp"
#include "timer.hpp"
#include "external_types.hpp"

extern "C" {

CassSession* cass_session_new() {
  return CassSession::to(new cass::Session());
}

void cass_session_free(CassSession* session) {
  // This attempts to close the session because the joining will
  // hang indefinitely otherwise. This causes minimal delay
  // if the session is already closed.
  cass::SharedRefPtr<cass::Future> future(new cass::SessionFuture());
  session->close_async(future.get(), true);
  future->wait();

  delete session->from();
}

CassFuture* cass_session_connect(CassSession* session, CassCluster* cluster) {
  return cass_session_connect_keyspace(session, cluster, "");
}

CassFuture* cass_session_connect_keyspace(CassSession* session,
                                          CassCluster* cluster,
                                          const char* keyspace) {
  return cass_session_connect_keyspace_n(session,
                                         cluster,
                                         keyspace,
                                         strlen(keyspace));
}

CassFuture* cass_session_connect_keyspace_n(CassSession* session,
                                            CassCluster* cluster,
                                            const char* keyspace,
                                            size_t keyspace_length) {
  cass::SessionFuture* connect_future = new cass::SessionFuture();
  connect_future->inc_ref();
  session->connect_async(cass::SharedRefPtr<cass::Cluster>(cluster->from()),
                         std::string(keyspace, keyspace_length),
                         connect_future);
  return CassFuture::to(connect_future);
}

CassFuture* cass_session_close(CassSession* session) {
  cass::SessionFuture* close_future = new cass::SessionFuture();
  close_future->inc_ref();
  session->close_async(close_future);
  return CassFuture::to(close_future);
}

CassFuture* cass_session_prepare(CassSession* session, const char* query) {
  return cass_session_prepare_n(session, query, strlen(query));
}

CassFuture* cass_session_prepare_n(CassSession* session,
                                   const char* query,
                                   size_t query_length) {
  return CassFuture::to(session->prepare(query, query_length));
}

CassFuture* cass_session_execute(CassSession* session,
                                 const CassStatement* statement) {
  return CassFuture::to(session->execute(statement->from()));
}

CassFuture* cass_session_execute_batch(CassSession* session, const CassBatch* batch) {
  return CassFuture::to(session->execute(batch->from()));
}

const CassSchemaMeta* cass_session_get_schema_meta(const CassSession* session) {
  return CassSchemaMeta::to(new cass::Metadata::SchemaSnapshot(session->metadata().schema_snapshot()));
}

void  cass_session_get_metrics(const CassSession* session,
                               CassMetrics* metrics) {
  const cass::Metrics* internal_metrics = session->metrics();

  cass::Metrics::Histogram::Snapshot requests_snapshot;
  internal_metrics->request_latencies.get_snapshot(&requests_snapshot);

  metrics->requests.min = requests_snapshot.min;
  metrics->requests.max = requests_snapshot.max;
  metrics->requests.mean = requests_snapshot.mean;
  metrics->requests.stddev = requests_snapshot.stddev;
  metrics->requests.median = requests_snapshot.median;
  metrics->requests.percentile_75th = requests_snapshot.percentile_75th;
  metrics->requests.percentile_95th = requests_snapshot.percentile_95th;
  metrics->requests.percentile_98th = requests_snapshot.percentile_98th;
  metrics->requests.percentile_99th = requests_snapshot.percentile_99th;
  metrics->requests.percentile_999th = requests_snapshot.percentile_999th;

  metrics->requests.one_minute_rate = internal_metrics->request_rates.one_minute_rate();
  metrics->requests.five_minute_rate = internal_metrics->request_rates.five_minute_rate();
  metrics->requests.fifteen_minute_rate = internal_metrics->request_rates.fifteen_minute_rate();
  metrics->requests.mean_rate = internal_metrics->request_rates.mean_rate();


  metrics->stats.total_connections = internal_metrics->total_connections.sum();
  metrics->stats.available_connections = internal_metrics->available_connections.sum();
  metrics->stats.exceeded_write_bytes_water_mark = internal_metrics->exceeded_write_bytes_water_mark.sum();
  metrics->stats.exceeded_pending_requests_water_mark = internal_metrics->exceeded_pending_requests_water_mark.sum();

  metrics->errors.connection_timeouts = internal_metrics->connection_timeouts.sum();
  metrics->errors.pending_request_timeouts = internal_metrics->pending_request_timeouts.sum();
  metrics->errors.request_timeouts = internal_metrics->request_timeouts.sum();
}

} // extern "C"

namespace cass {

Session::Session()
    : state_(SESSION_STATE_CLOSED)
    , pending_resolve_count_(0) {
  for (size_t i = 0; i < 41; ++i) {
    io_worker_counters[i].store(0);
  }
  uv_mutex_init(&state_mutex_);
}

Session::~Session() {
  //join();
  uv_mutex_destroy(&state_mutex_);
}

void Session::clear(const SharedRefPtr<Cluster>& cluster) {
  cluster_ = cluster;
  connect_future_.reset();
  close_future_.reset();
  io_workers_.clear();
  request_queue_.reset(
        new MPMCQueue<RequestHandler*>(cluster->config().queue_size_io()));
  overwhelmed_request_queue_.reset(
        new MPMCQueue<RequestHandler*>(cluster->config().queue_size_io()));
  pending_resolve_count_ = 0;
}

int Session::init() {
  int rc = 0;
  for (unsigned int i = 0; i < cluster_->config().thread_count_io(); ++i) {
    SharedRefPtr<IOWorker> io_worker(new IOWorker(this));
    int rc = io_worker->init();
    if (rc != 0) return rc;
    io_workers_.push_back(io_worker);
  }
  return rc;
}

void Session::broadcast_keyspace_change(const std::string& keyspace,
                                        const IOWorker* calling_io_worker) {
  // This can run on an IO worker thread. This is thread-safe because the IO workers
  // vector never changes after initialization and IOWorker::set_keyspace() uses a mutex.
  // This also means that calling "USE <keyspace>" frequently is an anti-pattern.
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    if (*it == calling_io_worker) continue;
      (*it)->set_keyspace(keyspace);
  }
}

bool Session::notify_worker_ready_async() {
  return cluster_->notify_worker_ready_async(this);
}

bool Session::notify_worker_closed_async() {
  return cluster_->notify_worker_closed_async(this);
}

bool Session::notify_up_async(const Address& address) {
  return cluster_->notify_up_async(address);
}

bool Session::notify_down_async(const Address& address) {
  return cluster_->notify_down_async(address);
}

void Session::connect_async(const SharedRefPtr<Cluster>& cluster,
                            const std::string& keyspace,
                            Future* future) {
  ScopedMutex l(&state_mutex_);

  if (state_.load(MEMORY_ORDER_RELAXED) != SESSION_STATE_CLOSED) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                      "Already connecting, connected or closed");
    return;
  }

  clear(cluster);

  if (init() != 0) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                      "Error initializing session");
    return;
  }

  state_.store(SESSION_STATE_CONNECTING, MEMORY_ORDER_RELAXED);
  connect_future_.reset(future);

  if (!keyspace.empty()) {
    broadcast_keyspace_change(keyspace, NULL);
  }

  cluster_->add_session(this);
}

void Session::close_async(Future* future, bool force) {
  ScopedMutex l(&state_mutex_);

  State state = state_.load(MEMORY_ORDER_RELAXED);
  bool wait_for_connect_to_finish = (force && state == SESSION_STATE_CONNECTING);
  if (state != SESSION_STATE_CONNECTED && !wait_for_connect_to_finish) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CLOSE,
                      "Already closing or closed");
    return;
  }

  state_.store(SESSION_STATE_CLOSING, MEMORY_ORDER_RELAXED);
  close_future_.reset(future);

  if (!wait_for_connect_to_finish) {
    close();
  }
}

void Session::close() {
  cluster_->remove_session(this);
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->close_async();
  }
}

void Session::execute(RequestHandler* request_handler) {
  if (state_.load(MEMORY_ORDER_ACQUIRE) != SESSION_STATE_CONNECTED) {
    request_handler->on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                              "Session is not connected");
    return;
  }


  if (!request_queue_->enqueue(request_handler)) {
    request_handler->on_error(CASS_ERROR_LIB_REQUEST_QUEUE_FULL,
                              "The request queue has reached capacity");
    return;
  }


  Atomic<size_t>& counter = io_worker_counters[reinterpret_cast<size_t>(uv_thread_self()) % 41];
  MPMCQueue<RequestHandler*>::memory_fence();
  io_workers_[counter.fetch_add(1) % io_workers_.size()]->send();
}

Future* Session::prepare(const char* statement, size_t length) {
  PrepareRequest* prepare = new PrepareRequest();
  prepare->set_query(statement, length);

  ResponseFuture* future = new ResponseFuture(cluster_->metadata());
  future->inc_ref(); // External reference
  future->statement.assign(statement, length);

  RequestHandler* request_handler = new RequestHandler(prepare, future, NULL);
  request_handler->inc_ref(); // IOWorker reference

  execute(request_handler);

  return future;
}

bool Session::dequeue_request(RequestHandler*& request_handler) {
  return request_queue_->dequeue(request_handler);
  //if (!overwhelmed_request_queue_->dequeue(request_handler)) {
  //  return request_queue_->dequeue(request_handler);
  //}
  return true;
}

bool Session::enqueue_overwhelmed_request(RequestHandler* request_handler) {
  return overwhelmed_request_queue_->enqueue(request_handler);
}

void Session::on_add(const SharedRefPtr<Host>& host) {
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->add_pool_async(host->address());
  }
}

void Session::on_remove(const SharedRefPtr<Host>& host) {
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->remove_pool_async(host->address(), true);
  }
}

void Session::on_up(const SharedRefPtr<Host>& host) {
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->add_pool_async(host->address());
  }
}

void Session::on_down(const SharedRefPtr<Host>& host, bool cancel_reconnect) {
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->remove_pool_async(host->address(), cancel_reconnect);
  }
}

Future* Session::execute(const RoutableRequest* request) {
  ResponseFuture* future = new ResponseFuture(cluster_->metadata());
  future->inc_ref(); // External reference

  RetryPolicy* retry_policy
      = request->retry_policy() != NULL ? request->retry_policy()
                                        : config().retry_policy();

  RequestHandler* request_handler = new RequestHandler(request,
                                                       future,
                                                       retry_policy);
  request_handler->inc_ref(); // IOWorker reference

  execute(request_handler);

  return future;
}

void Session::on_ready(const SharedRefPtr<Host>& connected_host, const HostMap& hosts) {
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->set_protocol_version(cluster_->protocol_version());
    (*it)->run();
  }
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->ready_async(connected_host, hosts);
  }
  if (config().core_connections_per_host() == 0) {
    // Special case for internal testing. Not allowed by API
    LOG_DEBUG("Session connected with no core IO connections");
  }
}

void Session::on_error(CassError code, const std::string& message) {
  ScopedMutex l(&state_mutex_);
  state_.store(SESSION_STATE_CLOSING, MEMORY_ORDER_RELAXED);
  close();
  if (connect_future_) {
    connect_future_->set_error(code, message);
    connect_future_.reset();
  }
}

void Session::on_worker_ready() {
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    if (!(*it)->is_ready()) return;
  }

  ScopedMutex l(&state_mutex_);
  if (state_.load(MEMORY_ORDER_RELAXED) == SESSION_STATE_CONNECTING) {
    state_.store(SESSION_STATE_CONNECTED, MEMORY_ORDER_RELAXED);
  } else if (state_.load(MEMORY_ORDER_RELAXED) == SESSION_STATE_CLOSING) { // We recieved a 'force' close event
    close();
  }
  if (connect_future_) {
    connect_future_->set();
    connect_future_.reset();
  }
}

bool Session::on_worker_closed() {
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    if (!(*it)->is_closed()) return false;
  }
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->join();
  }
  return true;
}

void Session::on_close() {
  ScopedMutex l(&state_mutex_);
  state_.store(SESSION_STATE_CLOSED, MEMORY_ORDER_RELAXED);
  if (close_future_) {
    close_future_->set();
    close_future_.reset();
  }
}

} // namespace cass
