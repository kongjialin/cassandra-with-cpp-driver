#ifndef STORAGE_CASS_DRIVER_H_
#define STORAGE_CASS_DRIVER_H_

#include <string>
#include <tr1/functional>
#include <unordered_map>
#include <vector>

#include "common/idl/message_types.h"
#include "storage/driver/cassandra.h"

namespace pushing {

class CassDriver {
 public:
  typedef struct Wrapper_ {
    CassDriver* this_obj;
    void* cb_data;
    std::tr1::function<void(bool success, Wrapper_* data)> cob;
    std::vector<boost::shared_ptr<Message>>* pmsgs;
    CassStatement* statement;
    std::tr1::function<void()> func;
  } Wrapper;

  CassDriver();
  ~CassDriver();
  void Store(void (*cob)(CassFuture* future, void* data),
             const Message& message, void* cb_data);
  void Retrieve(std::tr1::function<void(bool success, Wrapper* data)> cob,
                const std::string& receiver_id, Wrapper* data_wrapper);
  void PrintError(CassFuture* future);
 
 private:
  void InitLogLevelMap();
  void CreateCluster();
  CassLogLevel ParseLogLevel(std::string& log_level);
  void ConnectSession();
  void PrepareQuery();
  CassCluster* cluster_;
  CassSession* session_;
  const CassPrepared* insert_prepared_;
  const CassPrepared* select_prepared_;
  const CassPrepared* delete_prepared_;
  std::unordered_map<std::string, CassLogLevel> log_level_map_;
};

} // namespace pushing

#endif // STORAGE_CASS_DRIVER_H_
