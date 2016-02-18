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

#ifndef __CASS_SESSION_HPP_INCLUDED__
#define __CASS_SESSION_HPP_INCLUDED__

#include "config.hpp"
#include "cluster.hpp"
#include "event_thread.hpp"
#include "future.hpp"
#include "host.hpp"
#include "io_worker.hpp"
#include "load_balancing.hpp"
#include "metadata.hpp"
#include "metrics.hpp"
#include "mpmc_queue.hpp"
#include "ref_counted.hpp"
#include "row.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"

#include <list>
#include <memory>
#include <set>
#include <string>
#include <uv.h>
#include <vector>

namespace cass {

class RequestHandler;
class Future;
class IOWorker;
class Resolver;
class Request;

struct SessionEvent {
  enum Type {
    INVALID,
    CONNECT,
    NOTIFY_READY,
    NOTIFY_WORKER_CLOSED,
    NOTIFY_UP,
    NOTIFY_DOWN
  };

  SessionEvent()
    : type(INVALID) {}

  Type type;
  Address address;
};

class Session {
public:
  enum State {
    SESSION_STATE_CONNECTING,
    SESSION_STATE_CONNECTED,
    SESSION_STATE_CLOSING,
    SESSION_STATE_CLOSED
  };

  Session();
  ~Session();

  const Config& config() const { return cluster_->config(); }

  Metrics* metrics() const { return cluster_->metrics(); }

  void broadcast_keyspace_change(const std::string& keyspace,
                                 const IOWorker* calling_io_worker);

  bool notify_worker_ready_async();
  bool notify_worker_closed_async();
  bool notify_up_async(const Address& address);
  bool notify_down_async(const Address& address);

  void connect_async(const SharedRefPtr<Cluster>& cluster, const std::string& keyspace, Future* future);
  void close_async(Future* future, bool force = false);

  Future* prepare(const char* statement, size_t length);
  Future* execute(const RoutableRequest* statement);

  const Metadata& metadata() const { return cluster_->metadata(); }

  int protocol_version() const {
    return cluster_->protocol_version();
  }

  void on_add(const SharedRefPtr<Host>& host);
  void on_remove(const SharedRefPtr<Host>& host);
  void on_up(const SharedRefPtr<Host>& host);
  void on_down(const SharedRefPtr<Host>& host, bool cancel_reconnect);

  void on_ready(const SharedRefPtr<Host>& connected_host, const HostMap& hosts);
  void on_error(CassError code, const std::string& message);

  void on_worker_ready();
  bool on_worker_closed();

  void on_close();

private:
  void clear(const SharedRefPtr<Cluster>& cluster);
  int init();

  void close();

  void execute(RequestHandler* request_handler);

#if 0
#if UV_VERSION_MAJOR == 0
  static void on_execute(uv_async_t* data, int status);
#else
  static void on_execute(uv_async_t* data);
#endif
#endif

  void on_reconnect(Timer* timer);

private:
  typedef std::vector<SharedRefPtr<IOWorker> > IOWorkerVec;

  Atomic<State> state_;
  uv_mutex_t state_mutex_;

  SharedRefPtr<Cluster> cluster_;
  ScopedRefPtr<Future> connect_future_;
  ScopedRefPtr<Future> close_future_;

  IOWorkerVec io_workers_;
  ScopedPtr<MPMCQueue<RequestHandler*> > request_queue_;
  int pending_resolve_count_;
};

class SessionFuture : public Future {
public:
  SessionFuture()
      : Future(CASS_FUTURE_TYPE_SESSION) {}
};

} // namespace cass

#endif
