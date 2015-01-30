#ifndef STORAGE_CASS_STRESS_H_
#define STORAGE_CASS_STRESS_H_

#include <atomic>
#include <string>
#include <vector>

#include "common/base/functor.h"
#include "common/base/join_functor.h"
#include "thirdparty/boost/thread/condition_variable.hpp"
#include "thirdparty/boost/thread/mutex.hpp"
#include "thirdparty/boost/thread/thread.hpp"

namespace pushing {

class CassDriver;

class CassStress {
 public:
  typedef struct CBMetadata_ {
    int array_index;
    int thread_index;
    CassStress* obj_this;
  } CBMetadata;
  typedef struct ThreadStatus_ {
    int count;
    JoinFunctor* joiner;
    std::atomic<int> batch_count;
  } ThreadStatus;

  CassStress();
  ~CassStress();
  void Operate();

 private:
  void ThreadFunc(int thread_index, Functor<void>* finish);
  void CallStore(int thread_index, int array_index);
  void CallRetrieve(int thread_index, int array_index);
  void PrintResults();
  double RankLatency(double rank, double* arr, int size);
  
  CassDriver* cass_driver_;
  boost::mutex mutex_;
  boost::condition_variable cond_;
  ThreadStatus* thread_status_arr_;
  std::vector<boost::thread*> threads_;
  double* latencies_;
  int64_t stress_start_time_;
  int64_t stress_end_time_;
  bool work_done_;
};

} // namespace pushing

#endif // STORAGE_CASS_STRESS_H_
