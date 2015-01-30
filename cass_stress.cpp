#include "storage/cass_stress.h"

#include <stdlib.h>
#include <algorithm>

#include "common/base/timestamp.h"
#include "common/idl/message_types.h"
#include "storage/cass_driver.h"
#include "storage/random_message.h"
#include "thirdparty/gflags/gflags.h"

DECLARE_string(contact_ip_and_port);
DECLARE_string(log_level);
DECLARE_string(num_threads_io);
DECLARE_string(queue_size_io);
DECLARE_string(pending_req_low);
DECLARE_string(pending_req_high);
DECLARE_string(core_connections);
DECLARE_string(max_connections);

DEFINE_int32(operation_count, 10000, "Count of Operations");
DEFINE_int32(num_threads, 1, "Number of threads making requests");
DEFINE_int32(ops_per_batch, 1000,
             "Num of queries executed asynchronously per thread");

namespace pushing {

CassStress::CassStress() {
  work_done_ = false;
  cass_driver_ = new CassDriver();
  thread_status_arr_ = new ThreadStatus[FLAGS_num_threads];
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    thread_status_arr_[i].count = 0;
  }
  latencies_ = new double[FLAGS_operation_count];
}

CassStress::~CassStress() {
  delete [] thread_status_arr_;
  delete [] latencies_;
  delete cass_driver_;
}

void CassStress::Operate() {
  auto joiner = NewFunctor([=]() {
    stress_end_time_ = GetTimeStampInMs();
    PrintResults();
    boost::mutex::scoped_lock lock(mutex_);
    work_done_ = true;
    cond_.notify_one();
  });
  Functor<void>* finish = new JoinFunctor(FLAGS_num_threads, joiner);
  stress_start_time_ = GetTimeStampInMs();
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    boost::thread* thread = new boost::thread(&CassStress::ThreadFunc,
                                              this, i, finish);
    threads_.push_back(thread);
  }
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    threads_[i]->join();
    delete threads_[i];
  }
  
  boost::mutex::scoped_lock locker(mutex_);
  while (!work_done_) {
    cond_.wait(locker);
  }
}

void CassStress::ThreadFunc(int thread_index, Functor<void>* finish) {
  thread_status_arr_[thread_index].joiner =
      new JoinFunctor(FLAGS_operation_count/FLAGS_num_threads, finish);
  int iteration_num =
      FLAGS_operation_count / (FLAGS_num_threads * FLAGS_ops_per_batch);
  for (int i = 0; i < iteration_num; ++i) {
    thread_status_arr_[thread_index].batch_count = 0;
    for (int j = 0; j < FLAGS_ops_per_batch/2; ++j)  {
      int array_index = thread_index * FLAGS_operation_count / FLAGS_num_threads
          + i * FLAGS_ops_per_batch + j * 2;
      CallStore(thread_index, array_index);
      //CallStore(thread_index, array_index + 1);
      //CallRetrieve(thread_index, array_index);
      CallRetrieve(thread_index, array_index + 1);
      //boost::this_thread::sleep_for(boost::chrono::microseconds(100));
    }
    while (thread_status_arr_[thread_index].batch_count != FLAGS_ops_per_batch)
      boost::this_thread::sleep_for(boost::chrono::microseconds(10));
  }
}

void CassStress::CallStore(int thread_index, int array_index) {
  Message message;
  RandomMessage::GenerateMessage(&message);
  CBMetadata* metadata = new CBMetadata();
  metadata->array_index = array_index;
  metadata->thread_index = thread_index;
  metadata->obj_this = this;

  auto store_cb = [](CassFuture* future, void* data) {
    int64_t end_time = GetTimeStampInUs();
    CBMetadata* meta = (CBMetadata*)data;
    double latency = (end_time - meta->obj_this->latencies_[meta->array_index])
                      / 1000.0;
    meta->obj_this->latencies_[meta->array_index] = latency;
    std::atomic_fetch_add(
        &meta->obj_this->thread_status_arr_[meta->thread_index].batch_count, 1);
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK)
      meta->obj_this->cass_driver_->PrintError(future);
    else
      meta->obj_this->thread_status_arr_[meta->thread_index].count += 1;
    meta->obj_this->thread_status_arr_[meta->thread_index].joiner->Run();
    delete meta;
  };
  latencies_[array_index] = GetTimeStampInUs();
  cass_driver_->Store(store_cb, message, metadata);
}

void CassStress::CallRetrieve(int thread_index, int array_index) {
  std::string receiver;
  RandomMessage::GenerateString(&receiver);
  CassDriver::Wrapper* wrapper = new CassDriver::Wrapper();
  wrapper->pmsgs = new std::vector<boost::shared_ptr<Message>>;
  CBMetadata* metadata = new CBMetadata();
  metadata->thread_index = thread_index;
  metadata->array_index = array_index;
  metadata->obj_this = this;
  wrapper->cb_data = metadata;

  auto retrieve_cb = [=](bool success, CassDriver::Wrapper* data) {
    int64_t end_time = GetTimeStampInUs();
    CBMetadata* meta = (CBMetadata*)data->cb_data;
    double latency = (end_time - meta->obj_this->latencies_[meta->array_index])
                      / 1000.0;
    meta->obj_this->latencies_[meta->array_index] = latency;
    std::atomic_fetch_add(
        &meta->obj_this->thread_status_arr_[meta->thread_index].batch_count, 1);
    if (success)
      meta->obj_this->thread_status_arr_[meta->thread_index].count += 1;
    meta->obj_this->thread_status_arr_[meta->thread_index].joiner->Run();
    delete data->pmsgs;
    delete meta;
    delete data;
  };
  latencies_[array_index] = GetTimeStampInUs();
  cass_driver_->Retrieve(retrieve_cb, receiver, wrapper);
}

double CassStress::RankLatency(double rank, double* arr, int size) {
  int index = rank * size;
  if (index > 0)
    --index;
  return arr[index];
}

void CassStress::PrintResults() {
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

  RandomMessage::PrepareRowKeySet();
  CassStress cass_stress;
  cass_stress.Operate();
  return 0;
}


