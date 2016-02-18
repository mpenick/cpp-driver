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

#include "cluster.hpp"

#include "collection_iterator.hpp"
#include "constants.hpp"
#include "event_response.hpp"
#include "load_balancing.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "query_request.hpp"
#include "result_iterator.hpp"
#include "error_response.hpp"
#include "result_response.hpp"
#include "session.hpp"
#include "timer.hpp"

#include "dc_aware_policy.hpp"
#include "logger.hpp"
#include "round_robin_policy.hpp"
#include "external_types.hpp"
#include "utils.hpp"
#include "resolver.hpp"


#include <iomanip>
#include <sstream>
#include <vector>

#define SELECT_LOCAL "SELECT data_center, rack, release_version FROM system.local WHERE key='local'"
#define SELECT_LOCAL_TOKENS "SELECT data_center, rack, release_version, partitioner, tokens FROM system.local WHERE key='local'"
#define SELECT_PEERS "SELECT peer, data_center, rack, release_version, rpc_address FROM system.peers"
#define SELECT_PEERS_TOKENS "SELECT peer, data_center, rack, release_version, rpc_address, tokens FROM system.peers"

#define SELECT_KEYSPACES_20 "SELECT * FROM system.schema_keyspaces"
#define SELECT_COLUMN_FAMILIES_20 "SELECT * FROM system.schema_columnfamilies"
#define SELECT_COLUMNS_20 "SELECT * FROM system.schema_columns"
#define SELECT_USERTYPES_21 "SELECT * FROM system.schema_usertypes"
#define SELECT_FUNCTIONS_22 "SELECT * FROM system.schema_functions"
#define SELECT_AGGREGATES_22 "SELECT * FROM system.schema_aggregates"

#define SELECT_KEYSPACES_30 "SELECT * FROM system_schema.keyspaces"
#define SELECT_TABLES_30 "SELECT * FROM system_schema.tables"
#define SELECT_COLUMNS_30 "SELECT * FROM system_schema.columns"
#define SELECT_USERTYPES_30 "SELECT * FROM system_schema.types"
#define SELECT_FUNCTIONS_30 "SELECT * FROM system_schema.functions"
#define SELECT_AGGREGATES_30 "SELECT * FROM system_schema.aggregates"

extern "C" {

CassCluster* cass_cluster_new() {
  cass::Cluster* cluster = new cass::Cluster();
  cluster->inc_ref();
  return CassCluster::to(cluster);
}

void cass_cluster_free(CassCluster* cluster) {
  cluster->dec_ref();
}

CassError cass_cluster_set_port(CassCluster* cluster,
                                int port) {
  if (port <= 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_port(port);
  return CASS_OK;
}

void cass_cluster_set_ssl(CassCluster* cluster,
                          CassSsl* ssl) {
  cluster->config().set_ssl_context(ssl->from());
}

CassError cass_cluster_set_protocol_version(CassCluster* cluster,
                                            int protocol_version) {
  if (protocol_version < 1) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_protocol_version(protocol_version);
  return CASS_OK;
}

CassError cass_cluster_set_num_threads_io(CassCluster* cluster,
                                          unsigned num_threads) {
  if (num_threads == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_thread_count_io(num_threads);
  return CASS_OK;
}

CassError cass_cluster_set_queue_size_io(CassCluster* cluster,
                                         unsigned queue_size) {
  if (queue_size == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_queue_size_io(queue_size);
  return CASS_OK;
}

CassError cass_cluster_set_queue_size_event(CassCluster* cluster,
                                            unsigned queue_size) {
  if (queue_size == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_queue_size_event(queue_size);
  return CASS_OK;
}

CassError cass_cluster_set_contact_points(CassCluster* cluster,
                                          const char* contact_points) {
  size_t contact_points_length
      = contact_points == NULL ? 0 : strlen(contact_points);
  return cass_cluster_set_contact_points_n(cluster,
                                           contact_points,
                                           contact_points_length);
}

CassError cass_cluster_set_contact_points_n(CassCluster* cluster,
                                            const char* contact_points,
                                            size_t contact_points_length) {
  if (contact_points_length == 0) {
    cluster->config().contact_points().clear();
  } else {
    cass::explode(std::string(contact_points, contact_points_length),
      cluster->config().contact_points());
  }
  return CASS_OK;
}

CassError cass_cluster_set_core_connections_per_host(CassCluster* cluster,
                                                     unsigned num_connections) {
  if (num_connections == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_core_connections_per_host(num_connections);
  return CASS_OK;
}

CassError cass_cluster_set_max_connections_per_host(CassCluster* cluster,
                                                    unsigned num_connections) {
  if (num_connections == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_connections_per_host(num_connections);
  return CASS_OK;
}

void cass_cluster_set_reconnect_wait_time(CassCluster* cluster,
                                          unsigned wait_time_ms) {
  cluster->config().set_reconnect_wait_time(wait_time_ms);
}

CassError cass_cluster_set_max_concurrent_creation(CassCluster* cluster,
                                                   unsigned num_connections) {
  if (num_connections == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_concurrent_creation(num_connections);
  return CASS_OK;
}

CassError cass_cluster_set_max_concurrent_requests_threshold(CassCluster* cluster,
                                                             unsigned num_requests) {
  if (num_requests == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_concurrent_requests_threshold(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_max_requests_per_flush(CassCluster* cluster,
                                                  unsigned num_requests) {
  if (num_requests == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_requests_per_flush(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_write_bytes_high_water_mark(CassCluster* cluster,
                                                       unsigned num_bytes) {
  if (num_bytes == 0 ||
      num_bytes < cluster->config().write_bytes_low_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_write_bytes_high_water_mark(num_bytes);
  return CASS_OK;
}

CassError cass_cluster_set_write_bytes_low_water_mark(CassCluster* cluster,
                                                      unsigned num_bytes) {
  if (num_bytes == 0 ||
      num_bytes > cluster->config().write_bytes_high_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_write_bytes_low_water_mark(num_bytes);
  return CASS_OK;
}

CassError cass_cluster_set_pending_requests_high_water_mark(CassCluster* cluster,
                                                            unsigned num_requests) {
  if (num_requests == 0 ||
      num_requests < cluster->config().pending_requests_low_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_pending_requests_high_water_mark(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_pending_requests_low_water_mark(CassCluster* cluster,
                                                           unsigned num_requests) {
  if (num_requests == 0 ||
      num_requests > cluster->config().pending_requests_high_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_pending_requests_low_water_mark(num_requests);
  return CASS_OK;
}

void cass_cluster_set_connect_timeout(CassCluster* cluster,
                                      unsigned timeout_ms) {
  cluster->config().set_connect_timeout(timeout_ms);
}

void cass_cluster_set_request_timeout(CassCluster* cluster,
                                      unsigned timeout_ms) {
  cluster->config().set_request_timeout(timeout_ms);
}

void cass_cluster_set_credentials(CassCluster* cluster,
                                  const char* username,
                                  const char* password) {
  return cass_cluster_set_credentials_n(cluster,
                                        username, strlen(username),
                                        password, strlen(password));
}

void cass_cluster_set_credentials_n(CassCluster* cluster,
                                    const char* username,
                                    size_t username_length,
                                    const char* password,
                                    size_t password_length) {
  cluster->config().set_credentials(std::string(username, username_length),
                                    std::string(password, password_length));
}

void cass_cluster_set_load_balance_round_robin(CassCluster* cluster) {
  cluster->config().set_load_balancing_policy(new cass::RoundRobinPolicy());
}

CassError cass_cluster_set_load_balance_dc_aware(CassCluster* cluster,
                                                 const char* local_dc,
                                                 unsigned used_hosts_per_remote_dc,
                                                 cass_bool_t allow_remote_dcs_for_local_cl) {
  if (local_dc == NULL) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  return cass_cluster_set_load_balance_dc_aware_n(cluster,
                                                  local_dc,
                                                  strlen(local_dc),
                                                  used_hosts_per_remote_dc,
                                                  allow_remote_dcs_for_local_cl);
}

CassError cass_cluster_set_load_balance_dc_aware_n(CassCluster* cluster,
                                                 const char* local_dc,
                                                 size_t local_dc_length,
                                                 unsigned used_hosts_per_remote_dc,
                                                 cass_bool_t allow_remote_dcs_for_local_cl) {
  if (local_dc == NULL || local_dc_length == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_load_balancing_policy(
        new cass::DCAwarePolicy(std::string(local_dc, local_dc_length),
                                used_hosts_per_remote_dc,
                                !allow_remote_dcs_for_local_cl));
  return CASS_OK;
}

void cass_cluster_set_token_aware_routing(CassCluster* cluster,
                                          cass_bool_t enabled) {
  cluster->config().set_token_aware_routing(enabled == cass_true);
  // Token-aware routing relies on up-to-date schema information
  if (enabled == cass_true) {
    cluster->config().set_use_schema(true);
  }
}

void cass_cluster_set_latency_aware_routing(CassCluster* cluster,
                                            cass_bool_t enabled) {
  cluster->config().set_latency_aware_routing(enabled == cass_true);
}

void cass_cluster_set_latency_aware_routing_settings(CassCluster* cluster,
                                                     cass_double_t exclusion_threshold,
                                                     cass_uint64_t scale_ms,
                                                     cass_uint64_t retry_period_ms,
                                                     cass_uint64_t update_rate_ms,
                                                     cass_uint64_t min_measured) {
  cass::LatencyAwarePolicy::Settings settings;
  settings.exclusion_threshold = exclusion_threshold;
  settings.scale_ns = scale_ms * 1000 * 1000;
  settings.retry_period_ns = retry_period_ms * 1000 * 1000;
  settings.update_rate_ms = update_rate_ms;
  settings.min_measured = min_measured;
  cluster->config().set_latency_aware_routing_settings(settings);
}

void cass_cluster_set_whitelist_filtering(CassCluster* cluster,
                                          const char* hosts) {
  size_t hosts_length
      = hosts == NULL ? 0 : strlen(hosts);
  cass_cluster_set_whitelist_filtering_n(cluster,
                                         hosts,
                                         hosts_length);
}

void cass_cluster_set_whitelist_filtering_n(CassCluster* cluster,
                                            const char* hosts,
                                            size_t hosts_length) {
  if (hosts_length == 0) {
    cluster->config().whitelist().clear();
  } else {
    cass::explode(std::string(hosts, hosts_length),
                  cluster->config().whitelist());
  }
}

void cass_cluster_set_tcp_nodelay(CassCluster* cluster,
                                  cass_bool_t enabled) {
  cluster->config().set_tcp_nodelay(enabled == cass_true);
}

void cass_cluster_set_tcp_keepalive(CassCluster* cluster,
                                    cass_bool_t enabled,
                                    unsigned delay_secs) {
  cluster->config().set_tcp_keepalive(enabled == cass_true, delay_secs);
}

void cass_cluster_set_connection_heartbeat_interval(CassCluster* cluster,
                                               unsigned interval_secs) {
  cluster->config().set_connection_heartbeat_interval_secs(interval_secs);
}

void cass_cluster_set_connection_idle_timeout(CassCluster* cluster,
                                               unsigned timeout_secs) {
  cluster->config().set_connection_idle_timeout_secs(timeout_secs);
}

void cass_cluster_set_retry_policy(CassCluster* cluster,
                                   CassRetryPolicy* retry_policy) {
  cluster->config().set_retry_policy(retry_policy);
}

void cass_cluster_set_timestamp_gen(CassCluster* cluster,
                                    CassTimestampGen* timestamp_gen) {
  cluster->config().set_timestamp_gen(timestamp_gen);
}

void cass_cluster_set_use_schema(CassCluster* cluster,
                                 cass_bool_t enabled) {
  cluster->config().set_use_schema(enabled == cass_true);
  // Token-aware routing relies on up-to-date schema information
  if (enabled == cass_false) {
    cluster->config().set_token_aware_routing(false);
  }
}

} // extern "C"

namespace cass {

class ControlStartupQueryPlan : public QueryPlan {
public:
  ControlStartupQueryPlan(const HostMap& hosts)
    : hosts_(hosts)
    , it_(hosts_.begin()) {}

  virtual SharedRefPtr<Host> compute_next() {
    if (it_ == hosts_.end()) return SharedRefPtr<Host>();
    const SharedRefPtr<Host>& host = it_->second;
    ++it_;
    return host;
  }

private:
  const HostMap hosts_;
  HostMap::const_iterator it_;
};

bool Cluster::determine_address_for_peer_host(const Address& connected_address,
                                              const Value* peer_value,
                                              const Value* rpc_value,
                                              Address* output) {
  Address peer_address;
  Address::from_inet(peer_value->data(), peer_value->size(),
                     connected_address.port(), &peer_address);
  if (rpc_value->size() > 0) {
    Address::from_inet(rpc_value->data(), rpc_value->size(),
                       connected_address.port(), output);
    if (connected_address.compare(*output) == 0 ||
        connected_address.compare(peer_address) == 0) {
      LOG_DEBUG("system.peers on %s contains a line with rpc_address for itself. "
                "This is not normal, but is a known problem for some versions of DSE. "
                "Ignoring this entry.", connected_address.to_string(false).c_str());
      return false;
    }
    if (bind_any_ipv4_.compare(*output) == 0 ||
        bind_any_ipv6_.compare(*output) == 0) {
      LOG_WARN("Found host with 'bind any' for rpc_address; using listen_address (%s) to contact instead. "
               "If this is incorrect you should configure a specific interface for rpc_address on the server.",
               peer_address.to_string(false).c_str());
      *output = peer_address;
    }
  } else {
    LOG_WARN("No rpc_address for host %s in system.peers on %s. "
             "Ignoring this entry.", peer_address.to_string(false).c_str(),
             connected_address.to_string(false).c_str());
    return false;
  }
  return true;
}

Cluster::Cluster()
  : state_(CLUSTER_STATE_CLOSED)
  , control_state_(CONTROL_STATE_NEW)
  , control_connection_(NULL)
  , protocol_version_(0)
  , should_query_tokens_(false)
  , pending_resolve_count_(0)
  , current_host_mark_(true)
  , closing_session_(NULL) {
  // TODO(mpenick): Fix this
  metrics_.reset(new Metrics(128));
  uv_mutex_init(&state_mutex_);
}

Cluster::~Cluster() {
  uv_mutex_destroy(&state_mutex_);
  join();
}

const SharedRefPtr<Host> Cluster::connected_host() const {
  HostMap::const_iterator it = hosts_.find(current_host_address_);
  if (it == hosts_.end()) {
    return SharedRefPtr<Host>();
  }
  return it->second;
}

int Cluster::init() {
  return EventThread<ClusterEvent>::init(config_.queue_size_event());
}

void Cluster::clear() {
  control_state_ = CONTROL_STATE_NEW;
  load_balancing_policy_.reset(config_.load_balancing_policy());
  metadata_.clear();
  control_connection_ = NULL;
  reconnect_timer_.stop();
  query_plan_.reset();
  protocol_version_ = 0;
  last_connection_error_.clear();
  should_query_tokens_ = false;
  current_host_mark_ = true;
  pending_resolve_count_ = 0;

  state_.store(CLUSTER_STATE_CLOSED, MEMORY_ORDER_RELAXED);
  hosts_.clear();
  closing_session_ = NULL;
}

void Cluster::add_session(Session* session) {
  ScopedMutex l(&state_mutex_);
  if (state_.load(MEMORY_ORDER_RELAXED) == CLUSTER_STATE_CONNECTED) {
    session->on_ready(connected_host(), hosts_);
    return;
  }

  sessions_.push_back(session);

  if (sessions_.size() == 1 &&
      state_.load(MEMORY_ORDER_RELAXED) == CLUSTER_STATE_CLOSED) {
    clear();

    if (init() != 0) {
      session->on_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                        "Error initializing cluster");
      return;
    }

    state_.store(CLUSTER_STATE_CONNECTING, MEMORY_ORDER_RELAXED);

    ClusterEvent event;
    event.type = ClusterEvent::CONNECT;
    if (!send_event_async(event)) {
      session->on_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                        "Unable to enqueue connected event");
      return;
    }

    LOG_DEBUG("Issued connect event");

    // If this is a reconnect then the old thread needs to be
    // joined before creating a new thread.
    join();

    run();
  }
}

void Cluster::remove_session(Session* session) {
  ScopedMutex l(&state_mutex_);

  if (sessions_.size() == 1) {
    closing_session_ = session;
    state_.store(CLUSTER_STATE_CLOSING, MEMORY_ORDER_RELAXED);
  }

  std::vector<Session*>::iterator it = std::find(sessions_.begin(), sessions_.end(), session);
  assert(it != sessions_.end());
  sessions_.erase(it);
}

void Cluster::connect() {
  if (hosts_.empty()) { // No hosts lock necessary (only called on session thread)
    on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
             "No hosts provided or no hosts resolved");
    return;
  }

  query_plan_.reset(new ControlStartupQueryPlan(hosts_)); // No hosts lock necessary (read-only)
  protocol_version_ = config_.protocol_version();
  should_query_tokens_ = config_.token_aware_routing();
  if (protocol_version_ < 0) {
    protocol_version_ = CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
  }

  if (config_.use_schema()) {
    set_event_types(CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE |
                    CASS_EVENT_SCHEMA_CHANGE);
  } else {
    set_event_types(CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE);
  }

  reconnect(false);
}

void Cluster::resolve_and_connect() {
  int port = config_.port();

  const ContactPointList& contact_points = config_.contact_points();
  for (ContactPointList::const_iterator it = contact_points.begin(),
       end = contact_points.end();
       it != end; ++it) {
    const std::string& seed = *it;
    Address address;
    if (Address::from_string(seed, port, &address)) {
      add_host(address);
    } else {
      pending_resolve_count_++;
      Resolver::resolve(loop(), seed, port, this, on_resolve);
    }
  }

  if (pending_resolve_count_ == 0) {
    connect();
  }
}

void Cluster::schedule_reconnect(uint64_t ms) {
  reconnect_timer_.start(loop(),
                         ms,
                         this,
                         Cluster::on_reconnect);
}

void Cluster::reconnect(bool retry_current_host) {
  if (control_state_ == CONTROL_STATE_CLOSED) {
    return;
  }

  if (!retry_current_host) {
    if (!query_plan_->compute_next(&current_host_address_)) {
      if (control_state_ == CONTROL_STATE_READY) {
        schedule_reconnect(1000); // TODO(mpenick): Configurable?
      } else {
        on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                 "No hosts available for the control connection");
      }
      return;
    }
  }

  if (control_connection_ != NULL) {
    control_connection_->close();
  }

  control_connection_ = new Connection(loop(),
                               config_,
                               metrics_.get(),
                               current_host_address_,
                               "", // No keyspace
                               protocol_version_,
                               this);
  control_connection_->connect();
}

void Cluster::on_connection_ready(Connection* connection) {
  LOG_DEBUG("Connection ready on host %s",
            connection->address().to_string().c_str());

  // A protocol version is need to encode/decode maps properly
  metadata_.set_protocol_version(protocol_version_);

  // The control connection has to refresh meta when there's a reconnect because
  // events could have been missed while not connected.
  query_meta_hosts();
}

void Cluster::on_connection_close(Connection* connection) {
  bool retry_current_host = false;

  if (control_state_ != CONTROL_STATE_CLOSED) {
    LOG_WARN("Lost control connection on host %s", connection->address_string().c_str());
  }

  // This pointer to the connection is no longer valid once it's closed
  control_connection_ = NULL;

  if (control_state_ == CONTROL_STATE_NEW) {
    if (connection->is_invalid_protocol()) {
      if (protocol_version_ <= 1) {
        LOG_ERROR("Host %s does not support any valid protocol version",
                  connection->address_string().c_str());
        on_error(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL,
                 "Not even protocol version 1 is supported");
        return;
      }
      LOG_WARN("Host %s does not support protocol version %d. "
               "Trying protocol version %d...",
               connection->address_string().c_str(),
               protocol_version_,
               protocol_version_ - 1);
      protocol_version_--;
      retry_current_host = true;
    } else if (connection->is_auth_error()) {
      on_error(CASS_ERROR_SERVER_BAD_CREDENTIALS,
               connection->error_message());
      return;
    } else if (connection->is_ssl_error()) {
      on_error(connection->ssl_error_code(),
               connection->error_message());
      return;
    }
  }

  reconnect(retry_current_host);

  ScopedMutex l(&state_mutex_);
  if (state_.load(MEMORY_ORDER_RELAXED) == CLUSTER_STATE_CLOSING) {
    if (closing_session_ != NULL) {
      closing_session_->on_close();
      closing_session_ = NULL;
    }

    if (!sessions_.empty()) {
      clear();
      state_.store(CLUSTER_STATE_CONNECTING, MEMORY_ORDER_RELAXED);
      resolve_and_connect();
    } else {
      state_.store(CLUSTER_STATE_CLOSED, MEMORY_ORDER_RELAXED);
      close_handles();
    }
  }
}

void Cluster::on_connection_event(EventResponse* response) {
  switch (response->event_type()) {
    case CASS_EVENT_TOPOLOGY_CHANGE: {
      std::string address_str = response->affected_node().to_string();
      switch (response->topology_change()) {
        case EventResponse::NEW_NODE: {
          LOG_INFO("New node %s added", address_str.c_str());
          SharedRefPtr<Host> host = get_host(response->affected_node());
          if (!host) {
            host = add_host(response->affected_node());
            refresh_node_info(host, true, true);
          }
          break;
        }

        case EventResponse::REMOVED_NODE: {
          LOG_INFO("Node %s removed", address_str.c_str());
          SharedRefPtr<Host> host = get_host(response->affected_node());
          if (host) {
            on_remove(host);
            metadata_.remove_host(host);
          } else {
            LOG_DEBUG("Tried to remove host %s that doesn't exist", address_str.c_str());
          }
          break;
        }

        case EventResponse::MOVED_NODE:
          LOG_INFO("Node %s moved", address_str.c_str());
          SharedRefPtr<Host> host = get_host(response->affected_node());
          if (host) {
            refresh_node_info(host, false, true);
          } else {
            LOG_DEBUG("Move event for host %s that doesn't exist", address_str.c_str());
            metadata_.remove_host(host);
          }
          break;
      }
      break;
    }

    case CASS_EVENT_STATUS_CHANGE: {
      std::string address_str = response->affected_node().to_string();
      switch (response->status_change()) {
        case EventResponse::UP: {
          LOG_INFO("Node %s is up", address_str.c_str());
          on_up(response->affected_node());
          break;
        }

        case EventResponse::DOWN: {
          LOG_INFO("Node %s is down", address_str.c_str());
          on_down(response->affected_node());
          break;
        }
      }
      break;
    }

    case CASS_EVENT_SCHEMA_CHANGE:
      LOG_DEBUG("Schema change (%d): %.*s %.*s\n",
                response->schema_change(),
                (int)response->keyspace().size(), response->keyspace().data(),
                (int)response->target().size(), response->target().data());
      switch (response->schema_change()) {
        case EventResponse::CREATED:
        case EventResponse::UPDATED:
          switch (response->schema_change_target()) {
            case EventResponse::KEYSPACE:
              refresh_keyspace(response->keyspace());
              break;
            case EventResponse::TABLE:
              refresh_table(response->keyspace(), response->target());
              break;
            case EventResponse::TYPE:
              refresh_type(response->keyspace(), response->target());
              break;
            case EventResponse::FUNCTION:
            case EventResponse::AGGREGATE:
              refresh_function(response->keyspace(),
                               response->target(),
                               response->arg_types(),
                               response->schema_change_target() == EventResponse::AGGREGATE);
              break;
          }
          break;

        case EventResponse::DROPPED:
          switch (response->schema_change_target()) {
            case EventResponse::KEYSPACE:
              metadata_.drop_keyspace(response->keyspace().to_string());
              break;
            case EventResponse::TABLE:
              metadata_.drop_table(response->keyspace().to_string(),
                                   response->target().to_string());
              break;
            case EventResponse::TYPE:
              metadata_.drop_user_type(response->keyspace().to_string(),
                                       response->target().to_string());
              break;
            case EventResponse::FUNCTION:
              metadata_.drop_function(response->keyspace().to_string(),
                                      Metadata::full_function_name(response->target().to_string(),
                                                                   to_strings(response->arg_types())));
              break;
            case EventResponse::AGGREGATE:
              metadata_.drop_aggregate(response->keyspace().to_string(),
                                       Metadata::full_function_name(response->target().to_string(),
                                                                    to_strings(response->arg_types())));
              break;
          }
          break;

      }
      break;

    default:
      assert(false);
      break;
  }
}

void Cluster::query_meta_hosts() {
  ScopedRefPtr<ControlMultipleRequestHandler<UnusedData> > handler(
        new ControlMultipleRequestHandler<UnusedData>(this, Cluster::on_query_hosts, UnusedData()));
  handler->execute_query(SELECT_LOCAL_TOKENS);
  handler->execute_query(SELECT_PEERS_TOKENS);
}

void Cluster::on_query_hosts(Cluster* cluster,
                             const UnusedData& data,
                             const MultipleRequestHandler::ResponseVec& responses) {
  Connection* connection = cluster->control_connection_;
  if (connection == NULL) {
    return;
  }

  bool is_initial_connection = (cluster->control_state_ == CONTROL_STATE_NEW);

  // If the 'system.local' table is empty the connection isn't used as a control
  // connection because at least one node's information is required (itself). An
  // empty 'system.local' can happen during the bootstrapping process on some
  // versions of Cassandra. If this happens we defunct the connection and move
  // to the next node in the query plan.
  {
    SharedRefPtr<Host> host = cluster->get_host(connection->address());
    if (host) {
      host->set_mark(cluster->current_host_mark_);

      ResultResponse* local_result =
          static_cast<ResultResponse*>(responses[0].get());

      if (local_result->row_count() > 0) {
        local_result->decode_first_row();
        cluster->update_node_info(host, &local_result->first_row());
        cluster->metadata_.set_cassandra_version(host->cassandra_version());
      } else {
        LOG_WARN("No row found in %s's local system table",
                 connection->address_string().c_str());
        connection->defunct();
        return;
      }
    } else {
      LOG_WARN("Host %s from local system table not found",
               connection->address_string().c_str());
      connection->defunct();
      return;
    }
  }

  {
    ResultResponse* peers_result =
        static_cast<ResultResponse*>(responses[1].get());
    peers_result->decode_first_row();
    ResultIterator rows(peers_result);
    while (rows.next()) {
      Address address;
      const Row* row = rows.row();
      if (!determine_address_for_peer_host(connection->address(),
                                           row->get_by_name("peer"),
                                           row->get_by_name("rpc_address"),
                                           &address)) {
        continue;
      }

      SharedRefPtr<Host> host = cluster->get_host(address);
      bool is_new = false;
      if (!host) {
        is_new = true;
        host = cluster->add_host(address);
      }

      host->set_mark(cluster->current_host_mark_);

      cluster->update_node_info(host, rows.row());
      if (is_new && !is_initial_connection) {
        cluster->on_add(host);
      }
    }
  }

  cluster->purge_hosts(is_initial_connection);

  if (cluster->config_.use_schema()) {
    cluster->query_meta_schema();
  } else if (is_initial_connection) {
    cluster->control_state_ = CONTROL_STATE_READY;
    cluster->on_connection_ready();
    // Create a new query plan that considers all the new hosts from the
    // "system" tables.
    cluster->reset_query_plan();
  }
}

//TODO: query and callbacks should be in Metadata
// punting for now because of tight coupling of Session and CC state
void Cluster::query_meta_schema() {
  ScopedRefPtr<ControlMultipleRequestHandler<UnusedData> > handler(
        new ControlMultipleRequestHandler<UnusedData>(this, Cluster::on_query_meta_schema, UnusedData()));

  if (metadata_.cassandra_version() >= VersionNumber(3, 0, 0)) {
    handler->execute_query(SELECT_KEYSPACES_30);
    handler->execute_query(SELECT_TABLES_30);
    handler->execute_query(SELECT_COLUMNS_30);
    handler->execute_query(SELECT_USERTYPES_30);
    handler->execute_query(SELECT_FUNCTIONS_30);
    handler->execute_query(SELECT_AGGREGATES_30);
  } else {
    handler->execute_query(SELECT_KEYSPACES_20);
    handler->execute_query(SELECT_COLUMN_FAMILIES_20);
    handler->execute_query(SELECT_COLUMNS_20);
    if (metadata_.cassandra_version() >= VersionNumber(2, 1, 0)) {
      handler->execute_query(SELECT_USERTYPES_21);
    }
    if (metadata_.cassandra_version() >= VersionNumber(2, 2, 0)) {
      handler->execute_query(SELECT_FUNCTIONS_22);
      handler->execute_query(SELECT_AGGREGATES_22);
    }
  }
}

void Cluster::on_query_meta_schema(Cluster* cluster,
                                   const UnusedData& unused,
                                   const MultipleRequestHandler::ResponseVec& responses) {
  Connection* connection = cluster->control_connection_;
  if (connection == NULL) {
    return;
  }

  cluster->metadata_.clear_and_update_back();

  bool is_initial_connection = (cluster->control_state_ == CONTROL_STATE_NEW);

  cluster->metadata_.update_keyspaces(static_cast<ResultResponse*>(responses[0].get()));
  cluster->metadata_.update_tables(static_cast<ResultResponse*>(responses[1].get()),
      static_cast<ResultResponse*>(responses[2].get()));

  if (cluster->metadata_.cassandra_version() >= VersionNumber(2, 1, 0)) {
    cluster->metadata_.update_user_types(static_cast<ResultResponse*>(responses[3].get()));
  }

  if (cluster->metadata_.cassandra_version() >= VersionNumber(2, 2, 0)) {
    cluster->metadata_.update_functions(static_cast<ResultResponse*>(responses[4].get()));
    cluster->metadata_.update_aggregates(static_cast<ResultResponse*>(responses[5].get()));
  }

  cluster->metadata_.swap_to_back_and_update_front();
  if (cluster->should_query_tokens_) cluster->metadata_.build();

  if (is_initial_connection) {
    cluster->control_state_ = CONTROL_STATE_READY;
    cluster->on_connection_ready();
    // Create a new query plan that considers all the new hosts from the
    // "system" tables.
    cluster->reset_query_plan();
  }
}

void Cluster::refresh_node_info(SharedRefPtr<Host> host,
                                bool is_new_node,
                                bool query_tokens) {
  if (control_connection_ == NULL) {
    return;
  }

  bool is_connected_host = host->address().compare(control_connection_->address()) == 0;

  std::string query;
  ControlHandler<RefreshNodeData>::ResponseCallback response_callback;

  bool token_query = should_query_tokens_ && (host->was_just_added() || query_tokens);
  if (is_connected_host || !host->listen_address().empty()) {
    if (is_connected_host) {
      query.assign(token_query ? SELECT_LOCAL_TOKENS : SELECT_LOCAL);
    } else {
      query.assign(token_query ? SELECT_PEERS_TOKENS : SELECT_PEERS);
      query.append(" WHERE peer = '");
      query.append(host->listen_address());
      query.append("'");
    }
    response_callback = Cluster::on_refresh_node_info;
  } else {
    query.assign(token_query ? SELECT_PEERS_TOKENS : SELECT_PEERS);
    response_callback = Cluster::on_refresh_node_info_all;
  }

  LOG_DEBUG("refresh_node_info: %s", query.c_str());

  RefreshNodeData data(host, is_new_node);
  ScopedRefPtr<ControlHandler<RefreshNodeData> > handler(
        new ControlHandler<RefreshNodeData>(new QueryRequest(query),
                                            this,
                                            response_callback,
                                            data));
  if (!control_connection_->write(handler.get())) {
    LOG_ERROR("No more stream available while attempting to refresh node info");
  }
}

void Cluster::on_refresh_node_info(Cluster* cluster,
                                   const RefreshNodeData& data,
                                   Response* response) {
  Connection* connection = cluster->control_connection_;
  if (connection == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    LOG_ERROR("No row found for host %s in %s's local/peers system table. "
              "%s will be ignored.",
              host_address_str.c_str(),
              connection->address_string().c_str(),
              host_address_str.c_str());
    return;
  }
  result->decode_first_row();
  cluster->update_node_info(data.host, &result->first_row());

  if (data.is_new_node) {
    cluster->on_add(data.host);
  }
}

void Cluster::on_refresh_node_info_all(Cluster* cluster,
                                       const RefreshNodeData& data,
                                       Response* response) {
  Connection* connection = cluster->control_connection_;
  if (connection == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    LOG_ERROR("No row found for host %s in %s's peers system table. "
              "%s will be ignored.",
              host_address_str.c_str(),
              connection->address_string().c_str(),
              host_address_str.c_str());
    return;
  }

  result->decode_first_row();
  ResultIterator rows(result);
  while (rows.next()) {
    const Row* row = rows.row();
    Address address;
    bool is_valid_address
        = determine_address_for_peer_host(connection->address(),
                                          row->get_by_name("peer"),
                                          row->get_by_name("rpc_address"),
                                          &address);
    if (is_valid_address && data.host->address().compare(address) == 0) {
      cluster->update_node_info(data.host, row);
      if (data.is_new_node) {
        cluster->on_add(data.host);
      }
      break;
    }
  }
}

void Cluster::update_node_info(SharedRefPtr<Host> host, const Row* row) {
  const Value* v;

  std::string rack;
  row->get_string_by_name("rack", &rack);

  std::string dc;
  row->get_string_by_name("data_center", &dc);

  std::string release_version;
  row->get_string_by_name("release_version", &release_version);

  // This value is not present in the "system.local" query
  v = row->get_by_name("peer");
  if (v != NULL) {
    Address listen_address;
    Address::from_inet(v->data(), v->size(),
                       control_connection_->address().port(),
                       &listen_address);
    host->set_listen_address(listen_address.to_string());
  }

  // TODO(mpenick): Need a "on_move()" event
  if ((!rack.empty() && rack != host->rack()) ||
      (!dc.empty() && dc != host->dc())) {
    if (!host->was_just_added()) {
      load_balancing_policy_->on_remove(host);
    }
    host->set_rack_and_dc(rack, dc);
    if (!host->was_just_added()) {
      load_balancing_policy_->on_add(host);
    }
  }

  VersionNumber cassandra_version;
  if (cassandra_version.parse(release_version)) {
    host->set_cassaandra_version(cassandra_version);
  } else {
    LOG_WARN("Invalid release version string \"%s\" on host %s",
             release_version.c_str(),
             host->address().to_string().c_str());
  }

  if (should_query_tokens_) {
    bool is_connected_host = control_connection_ != NULL && host->address().compare(control_connection_->address()) == 0;
    std::string partitioner;
    if (is_connected_host && row->get_string_by_name("partitioner", &partitioner)) {
      metadata_.set_partitioner(partitioner);
    }
    v = row->get_by_name("tokens");
    if (v != NULL) {
      CollectionIterator i(v);
      TokenStringList tokens;
      while (i.next()) {
        tokens.push_back(i.value()->to_string_ref());
      }
      if (!tokens.empty()) {
        metadata_.update_host(host, tokens);
      }
    }
  }
}

void Cluster::refresh_keyspace(const StringRef& keyspace_name) {
  std::string query;

  if (metadata_.cassandra_version() >= VersionNumber(3, 0, 0)) {
    query.assign(SELECT_KEYSPACES_30);
  }  else {
    query.assign(SELECT_KEYSPACES_20);
  }
  query.append(" WHERE keyspace_name='")
      .append(keyspace_name.data(), keyspace_name.size())
      .append("'");

  LOG_DEBUG("Refreshing keyspace %s", query.c_str());

  control_connection_->write(
        new ControlHandler<std::string>(new QueryRequest(query),
                                        this,
                                        Cluster::on_refresh_keyspace,
                                        keyspace_name.to_string()));
}

void Cluster::on_refresh_keyspace(Cluster* cluster,
                                  const std::string& keyspace_name,
                                  Response* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response);
  if (result->row_count() == 0) {
    LOG_ERROR("No row found for keyspace %s in system schema table.",
              keyspace_name.c_str());
    return;
  }
  cluster->metadata_.update_keyspaces(result);
}

void Cluster::refresh_table(const StringRef& keyspace_name,
                            const StringRef& table_name) {
  std::string cf_query;
  std::string col_query;

  if (metadata_.cassandra_version() >= VersionNumber(3, 0, 0)) {
    cf_query.assign(SELECT_TABLES_30);
    cf_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND table_name='").append(table_name.data(), table_name.size()).append("'");

    col_query.assign(SELECT_COLUMNS_30);
    col_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND table_name='").append(table_name.data(), table_name.size()).append("'");
  } else {
    cf_query.assign(SELECT_COLUMN_FAMILIES_20);
    cf_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND columnfamily_name='").append(table_name.data(), table_name.size()).append("'");

    col_query.assign(SELECT_COLUMNS_20);
    col_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND columnfamily_name='").append(table_name.data(), table_name.size()).append("'");
  }

  LOG_DEBUG("Refreshing table %s; %s", cf_query.c_str(), col_query.c_str());

  ScopedRefPtr<ControlMultipleRequestHandler<RefreshTableData> > handler(
        new ControlMultipleRequestHandler<RefreshTableData>(this,
                                                            Cluster::on_refresh_table,
                                                            RefreshTableData(keyspace_name.to_string(), table_name.to_string())));
  handler->execute_query(cf_query);
  handler->execute_query(col_query);
}

void Cluster::on_refresh_table(Cluster* cluster,
                               const RefreshTableData& data,
                               const MultipleRequestHandler::ResponseVec& responses) {
  ResultResponse* column_family_result = static_cast<ResultResponse*>(responses[0].get());
  if (column_family_result->row_count() == 0) {
    LOG_ERROR("No row found for column family %s.%s in system schema table.",
              data.keyspace_name.c_str(), data.table_name.c_str());
    return;
  }

  cluster->metadata_.update_tables(
        static_cast<ResultResponse*>(responses[0].get()),
      static_cast<ResultResponse*>(responses[1].get()));
}


void Cluster::refresh_type(const StringRef& keyspace_name,
                           const StringRef& type_name) {

  std::string query;
  if (metadata_.cassandra_version() >= VersionNumber(3, 0, 0)) {
    query.assign(SELECT_USERTYPES_30);
  } else {
    query.assign(SELECT_USERTYPES_21);
  }

  query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
      .append("' AND type_name='").append(type_name.data(), type_name.size()).append("'");

  LOG_DEBUG("Refreshing type %s", query.c_str());

  control_connection_->write(
        new ControlHandler<std::pair<std::string, std::string> >(new QueryRequest(query),
                                                                 this,
                                                                 Cluster::on_refresh_type,
                                                                 std::make_pair(keyspace_name.to_string(), type_name.to_string())));
}

void Cluster::on_refresh_type(Cluster* cluster,
                              const std::pair<std::string, std::string>& keyspace_and_type_names,
                              Response* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response);
  if (result->row_count() == 0) {
    LOG_ERROR("No row found for keyspace %s and type %s in system schema.",
              keyspace_and_type_names.first.c_str(),
              keyspace_and_type_names.second.c_str());
    return;
  }
  cluster->metadata_.update_user_types(result);
}

void Cluster::refresh_function(const StringRef& keyspace_name,
                               const StringRef& function_name,
                               const StringRefVec& arg_types,
                               bool is_aggregate) {

  std::string query;
  if (metadata_.cassandra_version() >= VersionNumber(3, 0, 0)) {
    if (is_aggregate) {
      query.assign(SELECT_AGGREGATES_30);
      query.append(" WHERE keyspace_name=? AND aggregate_name=? AND argument_types=?");
    } else {
      query.assign(SELECT_FUNCTIONS_30);
      query.append(" WHERE keyspace_name=? AND function_name=? AND argument_types=?");
    }
  } else {
    if (is_aggregate) {
      query.assign(SELECT_AGGREGATES_22);
      query.append(" WHERE keyspace_name=? AND aggregate_name=? AND signature=?");
    } else {
      query.assign(SELECT_FUNCTIONS_22);
      query.append(" WHERE keyspace_name=? AND function_name=? AND signature=?");
    }
  }

  LOG_DEBUG("Refreshing %s %s in keyspace %s",
            is_aggregate ? "aggregate" : "function",
            Metadata::full_function_name(function_name.to_string(), to_strings(arg_types)).c_str(),
            std::string(keyspace_name.data(), keyspace_name.length()).c_str());

  SharedRefPtr<QueryRequest> request(new QueryRequest(query, 3));
  SharedRefPtr<Collection> signature(new Collection(CASS_COLLECTION_TYPE_LIST, arg_types.size()));

  for (StringRefVec::const_iterator i = arg_types.begin(),
       end = arg_types.end();
       i != end;
       ++i) {
    signature->append(CassString(i->data(), i->size()));
  }

  request->set(0, CassString(keyspace_name.data(), keyspace_name.size()));
  request->set(1, CassString(function_name.data(), function_name.size()));
  request->set(2, signature.get());

  control_connection_->write(
        new ControlHandler<RefreshFunctionData>(request.get(),
                                                this,
                                                Cluster::on_refresh_function,
                                                RefreshFunctionData(keyspace_name, function_name, arg_types, is_aggregate)));
}

void Cluster::on_refresh_function(Cluster* cluster,
                                  const RefreshFunctionData& data,
                                  Response* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response);
  if (result->row_count() == 0) {
    LOG_ERROR("No row found for keyspace %s and %s %s",
              data.keyspace.c_str(),
              data.is_aggregate ? "aggregate" : "function",
              Metadata::full_function_name(data.function, data.arg_types).c_str());
    return;
  }
  if (data.is_aggregate) {
    cluster->metadata_.update_aggregates(result);
  } else {
    cluster->metadata_.update_functions(result);
  }
}

bool Cluster::handle_query_invalid_response(Response* response) {
  if (check_error_or_invalid_response("ControlConnection", CQL_OPCODE_RESULT,
                                      response)) {
    if (control_connection_ != NULL) {
      control_connection_->defunct();
    }
    return true;
  }
  return false;
}

void Cluster::handle_query_failure(CassError code, const std::string& message) {
  // TODO(mpenick): This is a placeholder and might not be the right action for
  // all error scenarios
  if (control_connection_ != NULL) {
    control_connection_->defunct();
  }
}

void Cluster::handle_query_timeout() {
  // TODO(mpenick): Is this the best way to handle a timeout?
  if (control_connection_ != NULL) {
    control_connection_->defunct();
  }
}

bool Cluster::notify_worker_ready_async(Session* session) {
  ClusterEvent event;
  event.type = ClusterEvent::NOTIFY_WORKER_READY;
  event.session = session;
  return send_event_async(event);
}

bool Cluster::notify_worker_closed_async(Session* session) {
  ClusterEvent event;
  event.type = ClusterEvent::NOTIFY_WORKER_CLOSED;
  event.session = session;
  return send_event_async(event);
}

bool Cluster::notify_up_async(const Address& address) {
  ClusterEvent event;
  event.type = ClusterEvent::NOTIFY_UP;
  event.address = address;
  return send_event_async(event);
}

bool Cluster::notify_down_async(const Address& address) {
  ClusterEvent event;
  event.type = ClusterEvent::NOTIFY_DOWN;
  event.address = address;
  return send_event_async(event);
}


void Cluster::on_reconnect(Timer* timer) {
  Cluster* cluster = static_cast<Cluster*>(timer->data());
  cluster->reset_query_plan();
  cluster->reconnect(false);
}

SharedRefPtr<Host> Cluster::get_host(const Address& address) const {
  // Lock hosts. This can be called on a non-session thread.
  ScopedMutex l(&state_mutex_);
  HostMap::const_iterator it = hosts_.find(address);
  if (it == hosts_.end()) {
    return SharedRefPtr<Host>();
  }
  return it->second;
}

SharedRefPtr<Host> Cluster::add_host(const Address& address) {
  LOG_DEBUG("Adding new host: %s", address.to_string().c_str());
  SharedRefPtr<Host> host(new Host(address, !current_host_mark_));
  { // Lock hosts
    ScopedMutex l(&state_mutex_);
    hosts_[address] = host;
  }
  return host;
}

void Cluster::purge_hosts(bool is_initial_connection) {
  // Hosts lock not held for reading (only called on session thread)
  HostMap::iterator it = hosts_.begin();
  while (it != hosts_.end()) {
    if (it->second->mark() != current_host_mark_) {
      HostMap::iterator to_remove_it = it++;

      std::string address_str = to_remove_it->first.to_string();
      if (is_initial_connection) {
        LOG_WARN("Unable to reach contact point %s", address_str.c_str());
        { // Lock hosts
          ScopedMutex l(&state_mutex_);
          hosts_.erase(to_remove_it);
        }
      } else {
        LOG_WARN("Host %s removed", address_str.c_str());
        on_remove(to_remove_it->second);
      }
    } else {
      ++it;
    }
  }
  current_host_mark_ = !current_host_mark_;
}

void Cluster::reset_query_plan() {
  query_plan_.reset(load_balancing_policy_->new_query_plan(
                      "", NULL, metadata_.token_map(), NULL));
}

void Cluster::close_handles() {
  EventThread<ClusterEvent>::close_handles();
  load_balancing_policy_->close_handles();
}

void Cluster::on_add(const SharedRefPtr<Host>& host) {
  host->set_up();

  if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
    return;
  }

  load_balancing_policy_->on_add(host);

  ScopedMutex l(&state_mutex_);
  for (std::vector<Session*>::iterator it = sessions_.begin(),
       end = sessions_.end(); it != end; ++it) {
    (*it)->on_add(host);
  }
}

void Cluster::on_remove(const SharedRefPtr<Host>& host) {
  load_balancing_policy_->on_remove(host);

  ScopedMutex l(&state_mutex_);
  SharedRefPtr<Host> temp(host);
  hosts_.erase(host->address());
  for (std::vector<Session*>::iterator it = sessions_.begin(),
       end = sessions_.end(); it != end; ++it) {
    (*it)->on_remove(host);
  }
}

void Cluster::on_up(const Address& address) {
  SharedRefPtr<Host> host = get_host(address);
  if (host) {
    if (host->is_up()) return;

    // Immediately mark the node as up and asynchronously attempt
    // to refresh the node's information. This is done because
    // a control connection may not be available because it's
    // waiting for a node to be marked as up.

    host->set_up();

    if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
      return;
    }

    load_balancing_policy_->on_up(host);

    {
      ScopedMutex l(&state_mutex_);
      for (std::vector<Session*>::iterator it = sessions_.begin(),
           end = sessions_.end(); it != end; ++it) {
        (*it)->on_up(host);
      }
    }

    refresh_node_info(host, false);
  } else {
    host = add_host(address);
    refresh_node_info(host, true);
  }
}

void Cluster::on_down(const Address& address) {
  SharedRefPtr<Host> host = get_host(address);
  if (host) {
    if (host->is_down()) return;

    host->set_down();
    load_balancing_policy_->on_down(host);

    bool cancel_reconnect = false;
    if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
      // This permanently removes a host from all IO workers by stopping
      // any attempt to reconnect to that host.
      cancel_reconnect = true;
    }

    {
      ScopedMutex l(&state_mutex_);
      for (std::vector<Session*>::iterator it = sessions_.begin(),
           end = sessions_.end(); it != end; ++it) {
        (*it)->on_down(host, cancel_reconnect);
      }
    }
  } else {
    LOG_DEBUG("Tried to down host %s that doesn't exist", address.to_string().c_str());
  }
}

void Cluster::on_connection_ready() {
  // No hosts lock necessary (only called on session thread and read-only)
  SharedRefPtr<Host> current_host(connected_host());

  load_balancing_policy_->init(current_host, hosts_);
  load_balancing_policy_->register_handles(loop());

  {
    ScopedMutex l(&state_mutex_);
    for (std::vector<Session*>::iterator it = sessions_.begin(),
         end = sessions_.end(); it != end; ++it) {
      (*it)->on_ready(current_host, hosts_);
    }
  }
}

void Cluster::on_error(CassError code, const std::string& message) {
  ScopedMutex l(&state_mutex_);
  for (std::vector<Session*>::iterator it = sessions_.begin(),
       end = sessions_.end(); it != end; ++it) {
    (*it)->on_error(code, message);
  }
}

void Cluster::on_resolve(Resolver* resolver) {
  Cluster* cluster = static_cast<Cluster*>(resolver->data());
  if (resolver->is_success()) {
    cluster->add_host(resolver->address());
  } else {
    LOG_ERROR("Unable to resolve host %s:%d\n",
              resolver->host().c_str(), resolver->port());
  }
  if (--cluster->pending_resolve_count_ == 0) {
    cluster->connect();
  }
}

void Cluster::on_run() {

}

void Cluster::on_after_run(){

}

void Cluster::on_event(const ClusterEvent& event) {
  switch (event.type) {
    case ClusterEvent::CONNECT:
      resolve_and_connect();
      break;

    case ClusterEvent::NOTIFY_WORKER_READY:
      event.session->on_worker_ready();
      break;

    case ClusterEvent::NOTIFY_WORKER_CLOSED:
      if (event.session->on_worker_closed()) {
        if (event.session == closing_session_) {
          control_state_ = CONTROL_STATE_CLOSED;
          if (control_connection_ != NULL) {
            control_connection_->close();
          }
          reconnect_timer_.stop();
        } else {
          event.session->on_close();
        }
      }
      break;

    case ClusterEvent::NOTIFY_UP:
      on_up(event.address);
      break;

    case ClusterEvent::NOTIFY_DOWN:
      on_down(event.address);
      break;

    default:
      assert(false);
      break;
  }
}

template<class T>
void Cluster::ControlMultipleRequestHandler<T>::on_set(
    const MultipleRequestHandler::ResponseVec& responses) {
  bool has_error = false;
  for (MultipleRequestHandler::ResponseVec::const_iterator it = responses.begin(),
       end = responses.end(); it != end; ++it) {
    if (cluster_->handle_query_invalid_response(it->get())) {
      has_error = true;
    }
  }
  if (has_error) return;
  response_callback_(cluster_, data_, responses);
}

Address Cluster::bind_any_ipv4_("0.0.0.0", 0);
Address Cluster::bind_any_ipv6_("::", 0);

} // namespace cass
