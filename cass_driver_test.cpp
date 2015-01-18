#include "storage/cass_driver.h"

#include "thirdparty/boost/thread/thread.hpp"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

DECLARE_string(contact_ip_and_port);
DECLARE_string(log_level);
DECLARE_string(num_threads_io);
DECLARE_string(queue_size_io);
DECLARE_string(pending_req_low);
DECLARE_string(pending_req_high);
DECLARE_string(core_connections);
DECLARE_string(max_connections);

namespace pushing {

class CassDriverTest : public testing::Test {
 public:
  void StoreAndRetrieve() {
    int count = 1000;
    for (int i = 0; i < count; ++i) {
      // Store
      Message message;
      message.__set_receiver_id("unit_test_id");
      message.__set_timestamp("10000000000000");
      message.__set_msg_id("222");
      message.__set_group_id("");
      message.__set_msg("message # 222");
      message.__set_sender_id("44444444");

      auto store_cb = [](CassFuture* future, void* data) {
        CassError rc = cass_future_error_code(future);
        char* receiver = (char*)data;
        if (rc != CASS_OK)
          LOG(ERROR) << "store failed";
        else
          EXPECT_STREQ("unit_test_id", receiver); 
      };
      char* cb_data1 = "unit_test_id";
      cass_driver_->Store(store_cb, message, cb_data1);
 
      // Retrieve
      std::string receiver = "unit_test_id";
      CassDriver::Wrapper* wrapper = new CassDriver::Wrapper();
      wrapper->pmsgs = new std::vector<boost::shared_ptr<Message>>;
      char* cb_data = "callback data";
      wrapper->cb_data = cb_data;
      auto retrieve_cb = [=](bool success, CassDriver::Wrapper* data) {
        EXPECT_TRUE(success);
        EXPECT_STREQ((char*)data->cb_data, "callback data");
        if (data->pmsgs->size()) {
          EXPECT_EQ(data->pmsgs->size(), 1);
          EXPECT_STREQ(data->pmsgs->at(0)->receiver_id.c_str(), "unit_test_id");
          EXPECT_STREQ(data->pmsgs->at(0)->timestamp.c_str(), "10000000000000");
          EXPECT_STREQ(data->pmsgs->at(0)->msg_id.c_str(), "222");
          EXPECT_STREQ(data->pmsgs->at(0)->group_id.c_str(), "");
          EXPECT_STREQ(data->pmsgs->at(0)->msg.c_str(), "message # 222");
          EXPECT_STREQ(data->pmsgs->at(0)->sender_id.c_str(), "44444444");
        }
        delete data->pmsgs;
        delete data;
      };
      cass_driver_->Retrieve(retrieve_cb, receiver, wrapper);
    }
  }

 protected:
  virtual void SetUp() {
    FLAGS_contact_ip_and_port = "172.16.123.187:9042";
    cass_driver_ = boost::shared_ptr<CassDriver>(new CassDriver());
  }

  boost::shared_ptr<CassDriver> cass_driver_;
};

TEST_F(CassDriverTest, StoreRetrieveTest) {
  StoreAndRetrieve();
  boost::this_thread::sleep_for(boost::chrono::seconds(5));
}

} // namespace pushing
