#include "storage/cass_driver.h"

#include <cstdlib>
#include <string>

#include "thirdparty/gflags/gflags.h"

DEFINE_string(contact_ip_and_port, "127.0.0.1:9042",
              "One of the Cassandra server IPs," 
              "seperated by colon from the connect port");
DEFINE_string(log_level, "ERROR",
              "Cassandra log level:TRACE, DEBUG, INFO, WARN, ERROR, CRITICAL");
DEFINE_int32(num_threads_io, 0,
             "Number of threads that will handle query requests");
DEFINE_int32(queue_size_io, 4096,
             "The size of queue that stores pending requests");
DEFINE_int32(pending_req_low, 256,
             "The pending requests num below which writes will resume");
DEFINE_int32(pending_req_high, 512,
             "The pending requests num beyond which writes will pause");
DEFINE_int32(core_connections, 2,
             "Number of connections made to each server in each io thread");
DEFINE_int32(max_connections, 4,
             "Max num of connections made to each server in each io thread");
DEFINE_int32(page_size, 100,
             "Max count of CQL rows returned each select query");

namespace pushing {

CassDriver::CassDriver() {
  InitLogLevelMap();
  CreateCluster();
  ConnectSession();
  PrepareQuery();
}

CassDriver::~CassDriver() {
  cass_prepared_free(insert_prepared_);
  cass_prepared_free(select_prepared_);
  cass_prepared_free(delete_prepared_);
  CassFuture* close_future = cass_session_close(session_);
  cass_future_wait(close_future);
  cass_future_free(close_future);
  cass_cluster_free(cluster_);
}

void CassDriver::InitLogLevelMap() {
#define XX(log_level, str) \
  log_level_map_.insert(std::pair<std::string, CassLogLevel>(str, log_level));
  CASS_LOG_LEVEL_MAP(XX);
#undef XX
}

void CassDriver::CreateCluster() {
  int pos = FLAGS_contact_ip_and_port.find(':');
  if (pos == std::string::npos) {
    printf("Wrong format in contact_ip_and_port, use ip:port !\n");
    return;
  }
  std::string contact_point = FLAGS_contact_ip_and_port.substr(0, pos);
  std::string str_port = FLAGS_contact_ip_and_port.substr(pos + 1);
  int port = std::atoi(str_port.c_str());
  cluster_ = cass_cluster_new();
  cass_cluster_set_contact_points(cluster_, contact_point.c_str());
  cass_cluster_set_port(cluster_, port);
  cass_cluster_set_log_level(cluster_, ParseLogLevel(FLAGS_log_level));
  cass_cluster_set_num_threads_io(cluster_, FLAGS_num_threads_io);
  cass_cluster_set_queue_size_io(cluster_, FLAGS_queue_size_io);
  cass_cluster_set_pending_requests_low_water_mark(cluster_,
                                                   FLAGS_pending_req_low);
  cass_cluster_set_pending_requests_high_water_mark(cluster_,
                                                   FLAGS_pending_req_high);
  cass_cluster_set_core_connections_per_host(cluster_, FLAGS_core_connections);
  cass_cluster_set_max_connections_per_host(cluster_, FLAGS_max_connections);
}

CassLogLevel CassDriver::ParseLogLevel(std::string& log_level) {
  auto it = log_level_map_.find(log_level);
  return it->second;
}

void CassDriver::ConnectSession() {
  CassError rc = CASS_OK;
  CassFuture* future = cass_cluster_connect_keyspace(cluster_,
                                                     "offline_keyspace");
  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    PrintError(future);
  } else {
    session_ = cass_future_get_session(future);
  }
  cass_future_free(future);
}

void CassDriver::PrintError(CassFuture* future) {
  CassString message = cass_future_error_message(future);
  fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
}

void CassDriver::PrepareQuery() {
  CassError rc = CASS_OK;
  CassString insert_query = cass_string_init("INSERT INTO receiver_table "
      "(receiver_id, ts, msg_id, group_id, msg, sender_id) "
      "VALUES (?, ?, ?, ?, ?, ?);");
  CassFuture* insert_future = cass_session_prepare(session_, insert_query);
  cass_future_wait(insert_future);
  rc = cass_future_error_code(insert_future);
  if (rc != CASS_OK) {
    PrintError(insert_future);
  } else {
    insert_prepared_ = cass_future_get_prepared(insert_future);
  }
  cass_future_free(insert_future);

  rc = CASS_OK;
  CassString select_query = cass_string_init("SELECT * FROM receiver_table "
      "WHERE receiver_id = ?");
  CassFuture* select_future = cass_session_prepare(session_, select_query);
  cass_future_wait(select_future);
  rc = cass_future_error_code(select_future);
  if (rc != CASS_OK) {
    PrintError(select_future);
  } else {
    select_prepared_ = cass_future_get_prepared(select_future);
  }
  cass_future_free(select_future);

  rc = CASS_OK;
  CassString delete_query = cass_string_init("DELETE FROM receiver_table "
      "WHERE receiver_id = ?");
  CassFuture* delete_future = cass_session_prepare(session_, delete_query);
  cass_future_wait(delete_future);
  rc = cass_future_error_code(delete_future);
  if (rc != CASS_OK) {
    PrintError(delete_future);
  } else {
    delete_prepared_ = cass_future_get_prepared(delete_future);
  }
  cass_future_free(delete_future);
}

void CassDriver::Store(void (*cob)(CassFuture* future, void* data),
                       const Message& message, void* cb_data) {
  CassStatement* statement = cass_prepared_bind(insert_prepared_);
  cass_statement_bind_string(statement, 0,
                             cass_string_init(message.receiver_id.c_str()));
  cass_statement_bind_string(statement, 1,
                             cass_string_init(message.timestamp.c_str()));
  cass_statement_bind_string(statement, 2,
                             cass_string_init(message.msg_id.c_str()));
  cass_statement_bind_string(statement, 3,
                             cass_string_init(message.group_id.c_str()));
  cass_statement_bind_string(statement, 4,
                             cass_string_init(message.msg.c_str()));
  cass_statement_bind_string(statement, 5,
                             cass_string_init(message.sender_id.c_str()));

  CassFuture* future = cass_session_execute(session_, statement);
  cass_future_set_callback(future, cob, cb_data);
  cass_future_free(future);
  cass_statement_free(statement);
}

void CassDriver::Retrieve(
    std::tr1::function<void(bool success, Wrapper* data)> cob,
    const std::string& receiver_id, Wrapper* data_wrapper) {
  CassStatement* statement = cass_prepared_bind(select_prepared_);
  cass_statement_bind_string(statement, 0,
                             cass_string_init(receiver_id.c_str()));
  cass_statement_set_paging_size(statement, FLAGS_page_size);

  CassFuture* future = cass_session_execute(session_, statement);
  auto retrieve_cb = [](CassFuture* future, void* data) {
    CassError rc = cass_future_error_code(future);
    Wrapper* wrapper = (Wrapper*)data;
    if (rc == CASS_OK) {
      const CassResult* result = cass_future_get_result(future);
      if (cass_result_row_count(result)) {
        CassIterator* iterator = cass_iterator_from_result(result);
        CassString cass_receiver, cass_time, cass_msg_id,
                   cass_group_id, cass_msg, cass_sender;
        while (cass_iterator_next(iterator)) {
          const CassRow* row = cass_iterator_get_row(iterator);
          cass_value_get_string(cass_row_get_column(row, 0), &cass_receiver);
          cass_value_get_string(cass_row_get_column(row, 1), &cass_time);
          cass_value_get_string(cass_row_get_column(row, 2), &cass_msg_id);
          cass_value_get_string(cass_row_get_column(row, 3), &cass_group_id);
          cass_value_get_string(cass_row_get_column(row, 4), &cass_msg);
          cass_value_get_string(cass_row_get_column(row, 5), &cass_sender);
            
          std::string receiver(cass_receiver.data, cass_receiver.length);
          std::string time(cass_time.data, cass_time.length);
          std::string msg_id(cass_msg_id.data, cass_msg_id.length);
          std::string group_id(cass_group_id.data, cass_group_id.length);
          std::string msg(cass_msg.data, cass_msg.length);
          std::string sender(cass_sender.data, cass_sender.length);

          boost::shared_ptr<Message> message(new Message());
          message->__set_receiver_id(receiver);
          message->__set_timestamp(time);
          message->__set_msg_id(msg_id);
          message->__set_group_id(group_id);
          message->__set_msg(msg);
          message->__set_sender_id(sender);
          wrapper->pmsgs->push_back(message);
        }
        cass_bool_t has_more_pages = cass_result_has_more_pages(result);
        if (has_more_pages) {
          cass_statement_set_paging_state(wrapper->statement, result);
          (wrapper->func)();
        } else {
          cass_statement_free(wrapper->statement);
          CassStatement* statement =
              cass_prepared_bind(wrapper->this_obj->delete_prepared_);
          cass_statement_bind_string(statement, 0, cass_receiver);
          CassFuture* delete_future =
              cass_session_execute(wrapper->this_obj->session_, statement);
          cass_future_free(delete_future);
          cass_statement_free(statement);

          (wrapper->cob)(true, wrapper);
        }
        cass_iterator_free(iterator);
      } else {
        cass_statement_free(wrapper->statement);
        (wrapper->cob)(true, wrapper);
      }
      cass_result_free(result);
    } else {
      cass_statement_free(wrapper->statement);
      wrapper->this_obj->PrintError(future);
      (wrapper->cob)(false, wrapper);
    }
  };

  data_wrapper->this_obj = this;
  data_wrapper->cob = cob;
  data_wrapper->statement = statement;
  data_wrapper->func = [=]() {
    CassFuture* future = cass_session_execute(session_, statement);
    cass_future_set_callback(future, retrieve_cb, data_wrapper);
    cass_future_free(future);
  };

  cass_future_set_callback(future, retrieve_cb, data_wrapper);
  cass_future_free(future);
}

} // namespace pushing
