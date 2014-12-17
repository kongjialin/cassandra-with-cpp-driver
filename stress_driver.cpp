#include "storage/driver/cassandra.h"

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/base/functor.h"
#include "common/base/join_functor.h"
#include "common/base/timestamp.h"
#include "storage/random_message.h"
#include "thirdparty/boost/thread/condition_variable.hpp"
#include "thirdparty/boost/thread/mutex.hpp"
#include "thirdparty/boost/thread/thread.hpp"
#include "thirdparty/gflags/gflags.h"

DEFINE_string(contact_points, "127.0.0.1",
              "Comma delimited ip addreses of contact points");
DEFINE_string(log_level, "INFO",
              "Cassandra log level:TRACE, DEBUG, INFO, WARN, ERROR, CRITICAL");
DEFINE_string(operation_type, "STORE", "Type of operation: STORE, RETRIEVE");
DEFINE_int32(operation_count, 10000, "Count of Operations");
DEFINE_int32(num_threads, 1, "Number of threads making requests");
DEFINE_int32(num_threads_io, 0,
             "Number of threads that will handle query requests");
DEFINE_int32(ops_per_batch, 1000,
             "Num of queries executed concurrently per thread");
DEFINE_int32(queue_size_io, 4096,
             "The size of queue that stores pending requests");
DEFINE_int32(pending_req_low, 1000,
             "The pending requests num below which writes will resume");
DEFINE_int32(pending_req_high, 10000,
             "The pending requests num beyond which writes will pause");
DEFINE_int32(core_connections, 2,
             "Number of connections made to each server in each io thread");
DEFINE_int32(max_connections, 4,
             "Max num of connections made to each server in each io thread");

namespace pushing {

class StressDriver {
 public:
  typedef struct CBMetadata_ {
    int index;
    int thread_index;
    StressDriver* obj_this;
  } CBMetadata;
  typedef struct ThreadStatus_ {
    int count;
    JoinFunctor* joiner;
  } ThreadStatus;
  StressDriver();
  ~StressDriver();
  CassError ConnectSession();
  void Operate();

 private:
  void CreateCluster();
  CassLogLevel ParseLogLevel(std::string& log_level);
  void InitLogLevelMap();
  void PrintError(CassFuture* future);
  CassError PrepareQuery(CassString query);
  void Store(int thread_index, Functor<void>* finish);
  void Retrieve(int thread_index, Functor<void>* finish);
  void PrintResults();
  double RankLatency(double rank, double* arr, int size);
  CassCluster* cluster_;
  CassSession* session_;
  const CassPrepared* prepared_;
  CassFuture* close_future_;
  boost::mutex mutex_;
  boost::condition_variable cond_;
  std::unordered_map<std::string, CassLogLevel> log_level_map_;
  ThreadStatus* thread_status_arr_;
  std::vector<boost::thread*> threads_;
  double* latencies_;
  int64_t stress_start_time_;
  int64_t stress_end_time_;
}; 

StressDriver::StressDriver() {
  InitLogLevelMap();
  CreateCluster();
  thread_status_arr_ = new ThreadStatus[FLAGS_num_threads];
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    thread_status_arr_[i].count = 0;
  }
  latencies_ = new double[FLAGS_operation_count];
  close_future_ = NULL;
}

StressDriver::~StressDriver() {
  cass_prepared_free(prepared_);
  cass_future_wait(close_future_);  
  cass_future_free(close_future_);
  cass_cluster_free(cluster_);
  delete [] thread_status_arr_;
  delete [] latencies_;
}

void StressDriver::InitLogLevelMap() {
#define XX(log_level, str) \
  log_level_map_.insert(std::pair<std::string, CassLogLevel>(str, log_level));
  CASS_LOG_LEVEL_MAP(XX);
#undef XX
}

void StressDriver::PrintError(CassFuture* future) {
  CassString message = cass_future_error_message(future);
  fprintf(stdout, "Error: %.*s\n", (int)message.length, message.data);
  printf("print error\n");
}

void StressDriver::CreateCluster() {
  cluster_ = cass_cluster_new();
  cass_cluster_set_contact_points(cluster_, FLAGS_contact_points.c_str());
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

CassLogLevel StressDriver::ParseLogLevel(std::string& log_level) {
  auto it = log_level_map_.find(log_level);
  return it->second;
}

CassError StressDriver::ConnectSession() {
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
  return rc;
}

CassError StressDriver::PrepareQuery(CassString query) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_prepare(session_, query);
  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    PrintError(future);
  } else {
    prepared_ = cass_future_get_prepared(future);
  }
  cass_future_free(future);
  return rc;
}

void StressDriver::Operate() {
  void (StressDriver::*pfunc) (int index, Functor<void>* finish);
  if (FLAGS_operation_type == "STORE") {
    CassString insert_query = cass_string_init("INSERT INTO receiver_table "
        "(receiver_id, ts, msg_id, group_id, msg, sender_id) "
        "VALUES (?, ?, ?, ?, ?, ?);");
    PrepareQuery(insert_query);
    pfunc = &StressDriver::Store;
  } else {
    CassString select_query = cass_string_init("SELECT * FROM receiver_table "
        "WHERE receiver_id = ?");
    PrepareQuery(select_query);
    pfunc = &StressDriver::Retrieve;
  }

  auto joiner = NewFunctor([=]() {
    stress_end_time_ = GetTimeStampInMs();
    PrintResults();
    boost::mutex::scoped_lock lock(mutex_);
    close_future_ = cass_session_close(session_);
    cond_.notify_one();
  });
  Functor<void>* finish = new JoinFunctor(FLAGS_num_threads, joiner);
  stress_start_time_ = GetTimeStampInMs();
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    boost::thread* thread = new boost::thread(pfunc, this, i, finish);
    threads_.push_back(thread);
  }
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    threads_[i]->join();
    delete threads_[i];
  }

  boost::mutex::scoped_lock locker(mutex_);
  while (close_future_ == NULL) {
    cond_.wait(locker);
  }
}

void StressDriver::Store(int thread_index, Functor<void>* finish) {
  JoinFunctor* joiner =
      new JoinFunctor(FLAGS_operation_count/FLAGS_num_threads, finish);
  thread_status_arr_[thread_index].joiner = joiner;
  int iteration_num =
      FLAGS_operation_count / (FLAGS_num_threads * FLAGS_ops_per_batch);
  int index;
  Message message;
  for (int i = 0; i < iteration_num; ++i) {
    RandomMessage::GenerateMessage(&message);
    CassFuture** futures = new CassFuture*[FLAGS_ops_per_batch];
    for (int j = 0; j < FLAGS_ops_per_batch; ++j) {
      CassStatement* statement = cass_prepared_bind(prepared_);
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
      
      index = thread_index * FLAGS_operation_count / FLAGS_num_threads
              + i * FLAGS_ops_per_batch + j;
      CBMetadata* metadata = new CBMetadata();
      metadata->index = index;
      metadata->obj_this = this;
      metadata->thread_index = thread_index;
      
      auto cb = [](CassFuture* future, void* data) {
        int64_t end_time = GetTimeStampInUs();
        CBMetadata* meta = (CBMetadata*)data;
        double latency = (end_time - meta->obj_this->latencies_[meta->index])
                          / 1000.0;
        meta->obj_this->latencies_[meta->index] = latency;
        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
          meta->obj_this->PrintError(future);
        else
          meta->obj_this->thread_status_arr_[meta->thread_index].count += 1;
        meta->obj_this->thread_status_arr_[meta->thread_index].joiner->Run();
        delete meta;
      };

      latencies_[index] = GetTimeStampInUs();
      futures[j] = cass_session_execute(session_, statement);
      cass_future_set_callback(futures[j], cb, metadata);
      cass_statement_free(statement);
    }
    for (int j = 0; j < FLAGS_ops_per_batch; ++j) {
      CassFuture* future = futures[j];
      cass_future_wait(future);
      cass_future_free(future);
    }
    delete [] futures;
  }
}

void StressDriver::Retrieve(int thread_index, Functor<void>* finish) {
  JoinFunctor* joiner =
      new JoinFunctor(FLAGS_operation_count/FLAGS_num_threads, finish);
  thread_status_arr_[thread_index].joiner = joiner;
  int iteration_num =
      FLAGS_operation_count / (FLAGS_num_threads * FLAGS_ops_per_batch);
  int index;
  for (int i = 0; i < iteration_num; ++i) {
    std::string receiver;
    RandomMessage::GenerateString(&receiver);
    CassFuture** futures = new CassFuture*[FLAGS_ops_per_batch];
    for (int j = 0; j < FLAGS_ops_per_batch; ++j) {
      CassStatement* statement = cass_prepared_bind(prepared_);
      cass_statement_bind_string(statement, 0,
                                 cass_string_init(receiver.c_str()));

      index = thread_index * FLAGS_operation_count / FLAGS_num_threads
              + i * FLAGS_ops_per_batch + j;
      CBMetadata* metadata = new CBMetadata();
      metadata->thread_index = thread_index;
      metadata->index = index;
      metadata->obj_this = this;

      auto retrieve_cb = [](CassFuture* future, void* data) {
        int64_t end_time = GetTimeStampInUs();
        CBMetadata* meta = (CBMetadata*)data;
        double latency = (end_time - meta->obj_this->latencies_[meta->index])
                          / 1000.0;
        meta->obj_this->latencies_[meta->index] = latency;
        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
          meta->obj_this->PrintError(future);
        else
          meta->obj_this->thread_status_arr_[meta->thread_index].count += 1;
        meta->obj_this->thread_status_arr_[meta->thread_index].joiner->Run();
        delete meta;
      };

      latencies_[index] = GetTimeStampInUs();
      futures[j] = cass_session_execute(session_, statement);
      cass_future_set_callback(futures[j], retrieve_cb, metadata);
      cass_statement_free(statement);
    }
    for (int j = 0; j < FLAGS_ops_per_batch; ++j) {
      CassFuture* future = futures[j];
      cass_future_wait(future);
      cass_future_free(future);
    }
    delete [] futures;
  }
}

double StressDriver::RankLatency(double rank, double* arr, int size) {
  int index = rank * size;
  if (index > 0)
    --index;
  return arr[index];
}

void StressDriver::PrintResults() {
  int total_success = 0;
  double total_latency = 0;
  int64_t elapsed_ms = stress_end_time_ - stress_start_time_;
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    total_success += thread_status_arr_[i].count;
  }
  for (int i = 0; i < FLAGS_operation_count; ++i) {
    total_latency += latencies_[i];
  }
  std::sort(latencies_, latencies_ + FLAGS_operation_count);
  printf("Operation type: %s\n", FLAGS_operation_type.c_str());
  printf("Operation count: %d\n", FLAGS_operation_count);
  printf("Success count: %d\n", total_success);
  printf("QPS: %ld\n", FLAGS_operation_count * 1000 / elapsed_ms);
  printf("Average latency: %f ms\n", total_latency / FLAGS_operation_count);
  printf("Min latency: %f ms\n", latencies_[0]);
  printf(".95 latency: %f ms\n",
         RankLatency(0.95, latencies_, FLAGS_operation_count));
  printf(".99 latency: %f ms\n",
         RankLatency(0.99, latencies_, FLAGS_operation_count));
  printf(".999 latency: %f ms\n",
         RankLatency(0.999, latencies_, FLAGS_operation_count));
  printf("Max latency: %f ms\n", latencies_[FLAGS_operation_count - 1]);
  for (int i = 0; i <= 100; ++i) {
    printf("%d, %f\n", i, RankLatency(i/100.0, latencies_,
          FLAGS_operation_count));
  }
}

} // namespace pushing

using namespace pushing;
int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);

  StressDriver stress_driver;
  CassError rc = CASS_OK;
  rc = stress_driver.ConnectSession();
  if (rc != CASS_OK)
    return -1;
  stress_driver.Operate();
  return 0;
}
