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

#ifndef __CASS_CLUSTER_HPP_INCLUDED__
#define __CASS_CLUSTER_HPP_INCLUDED__

#include "address.hpp"
#include "config.hpp"
#include "connection.hpp"
#include "event_thread.hpp"
#include "token_map.hpp"
#include "handler.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "multiple_request_handler.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"

namespace cass {

class EventResponse;
class Request;
class Resolver;
class Row;
class Session;
class Timer;
class Value;

struct ClusterEvent {
  enum Type {
    INVALID,
    CONNECT,
    NOTIFY_WORKER_READY,
    NOTIFY_WORKER_CLOSED,
    NOTIFY_UP,
    NOTIFY_DOWN
  };

  ClusterEvent()
    : type(INVALID) {}

  Type type;
  Session* session;
  Address address;
};

class Cluster : public RefCounted<Cluster>,
                public EventThread<ClusterEvent>,
                public Connection::Listener {
public:
  static bool determine_address_for_peer_host(const Address& connected_address,
                                              const Value* peer_value,
                                              const Value* rpc_value,
                                              Address* output);

  enum State {
    CLUSTER_STATE_CONNECTING,
    CLUSTER_STATE_CONNECTED,
    CLUSTER_STATE_CLOSING,
    CLUSTER_STATE_CLOSED
  };

  enum ControlState {
    CONTROL_STATE_NEW,
    CONTROL_STATE_READY,
    CONTROL_STATE_CLOSED
  };

  Cluster();
  virtual ~Cluster();

  int protocol_version() const { return protocol_version_; }

  Config& config() { return config_; }
  const Config& config() const { return config_; }

  const Metadata& metadata() const { return metadata_; }

  Metrics* metrics() const { return metrics_.get(); }

  const SharedRefPtr<Host> connected_host() const;

  void add_session(Session* session);
  void remove_session(Session* session);

  bool notify_worker_ready_async(Session* session);
  bool notify_worker_closed_async(Session* session);
  bool notify_up_async(const Address& address);
  bool notify_down_async(const Address& address);

private:
  template<class T>
  class ControlMultipleRequestHandler : public MultipleRequestHandler {
  public:
    typedef void (*ResponseCallback)(Cluster*, const T&, const MultipleRequestHandler::ResponseVec&);

    ControlMultipleRequestHandler(Cluster* cluster,
                                  ResponseCallback response_callback,
                                  const T& data)
        : MultipleRequestHandler(cluster->control_connection_)
        , cluster_(cluster)
        , response_callback_(response_callback)
        , data_(data) {}

    virtual void on_set(const MultipleRequestHandler::ResponseVec& responses);

    virtual void on_error(CassError code, const std::string& message) {
      cluster_->handle_query_failure(code, message);
    }

    virtual void on_timeout() {
      cluster_->handle_query_timeout();
    }

  private:
    Cluster* cluster_;
    ResponseCallback response_callback_;
    T data_;
  };

  struct RefreshTableData {
    RefreshTableData(const std::string& keyspace_name,
                     const std::string& table_name)
      : keyspace_name(keyspace_name)
      , table_name(table_name) {}
    std::string keyspace_name;
    std::string table_name;
  };

  struct UnusedData {};

  template<class T>
  class ControlHandler : public Handler {
  public:
    typedef void (*ResponseCallback)(Cluster*, const T&, Response*);

    ControlHandler(const Request* request,
                   Cluster* cluster,
                   ResponseCallback response_callback,
                   const T& data)
      : Handler(request)
      , cluster_(cluster)
      , response_callback_(response_callback)
      , data_(data) {}

    virtual void on_set(ResponseMessage* response) {
      Response* response_body = response->response_body().get();
      if (cluster_->handle_query_invalid_response(response_body)) {
        return;
      }
      response_callback_(cluster_, data_, response_body);
    }

    virtual void on_error(CassError code, const std::string& message) {
      cluster_->handle_query_failure(code, message);
    }

    virtual void on_timeout() {
      cluster_->handle_query_timeout();
    }

  private:
    Cluster* cluster_;
    ResponseCallback response_callback_;
    T data_;
  };

  struct RefreshNodeData {
    RefreshNodeData(const SharedRefPtr<Host>& host,
                    bool is_new_node)
      : host(host)
      , is_new_node(is_new_node) {}
    SharedRefPtr<Host> host;
    bool is_new_node;
  };

  struct RefreshFunctionData {
    typedef std::vector<std::string> StringVec;

    RefreshFunctionData(StringRef keyspace,
                        StringRef function,
                        const StringRefVec& arg_types,
                        bool is_aggregate)
      : keyspace(keyspace.to_string())
      , function(function.to_string())
      , arg_types(to_strings(arg_types))
      , is_aggregate(is_aggregate) { }

    std::string keyspace;
    std::string function;
    StringVec arg_types;
    bool is_aggregate;
  };

  void clear();
  int init();

  void schedule_reconnect(uint64_t ms = 0);
  void reconnect(bool retry_current_host);

  // Connection listener methods
  virtual void on_connection_ready(Connection* connection);
  virtual void on_connection_close(Connection* connection);
  virtual void on_connection_availability_change(Connection* connection) {}
  virtual void on_connection_event(EventResponse* response);

  static void on_reconnect(Timer* timer);

  bool handle_query_invalid_response(Response* response);
  void handle_query_failure(CassError code, const std::string& message);
  void handle_query_timeout();

  void query_meta_hosts();
  static void on_query_hosts(Cluster* cluster,
                             const UnusedData& data,
                             const MultipleRequestHandler::ResponseVec& responses);

  void query_meta_schema();
  static void on_query_meta_schema(Cluster* cluster,
                                const UnusedData& data,
                                const MultipleRequestHandler::ResponseVec& responses);

  void refresh_node_info(SharedRefPtr<Host> host,
                         bool is_new_node,
                         bool query_tokens = false);
  static void on_refresh_node_info(Cluster* cluster,
                                   const RefreshNodeData& data,
                                   Response* response);
  static void on_refresh_node_info_all(Cluster* cluster,
                                       const RefreshNodeData& data,
                                       Response* response);

  void update_node_info(SharedRefPtr<Host> host, const Row* row);

  void refresh_keyspace(const StringRef& keyspace_name);
  static void on_refresh_keyspace(Cluster* cluster, const std::string& keyspace_name, Response* response);

  void refresh_table(const StringRef& keyspace_name,
                     const StringRef& table_name);
  static void on_refresh_table(Cluster* cluster,
                               const RefreshTableData& data,
                               const MultipleRequestHandler::ResponseVec& responses);

  void refresh_type(const StringRef& keyspace_name,
                    const StringRef& type_name);
  static void on_refresh_type(Cluster* cluster,
                              const std::pair<std::string, std::string>& keyspace_and_type_names,
                              Response* response);

  void refresh_function(const StringRef& keyspace_name,
                        const StringRef& function_name,
                        const StringRefVec& arg_types,
                        bool is_aggregate);
  static void on_refresh_function(Cluster* cluster,
                                  const RefreshFunctionData& data,
                                  Response* response);

public:
  SharedRefPtr<Host> add_host(const Address& address);

private:
  SharedRefPtr<Host> get_host(const Address& address) const;
  void purge_hosts(bool is_initial_connection);
  void reset_query_plan();
  void close_handles();

private:
  void on_add(const SharedRefPtr<Host>& host);
  void on_remove(const SharedRefPtr<Host>& host);
  void on_up(const Address& address);
  void on_down(const Address& address);

  void on_connection_ready();
  void on_error(CassError code, const std::string& message);

  static void on_resolve(Resolver* resolver);

private:
  virtual void on_run();
  virtual void on_after_run();
  virtual void on_event(const ClusterEvent& event);

private:
  void connect();
  void resolve_and_connect();

private:
  Atomic<State> state_;
  ControlState control_state_;
  Config config_;
  ScopedPtr<Metrics> metrics_;
  ScopedRefPtr<LoadBalancingPolicy> load_balancing_policy_;
  Metadata metadata_;
  Connection* control_connection_;
  Timer reconnect_timer_;
  ScopedPtr<QueryPlan> query_plan_;
  Address current_host_address_;
  int protocol_version_;
  std::string last_connection_error_;
  bool should_query_tokens_;
  int pending_resolve_count_;

  static Address bind_any_ipv4_;
  static Address bind_any_ipv6_;

private:
  mutable uv_mutex_t state_mutex_;
  HostMap hosts_;
  bool current_host_mark_;
  std::vector<Session*> sessions_;
  Session* closing_session_;

private:
  DISALLOW_COPY_AND_ASSIGN(Cluster);
};

} // namespace cass

#endif
