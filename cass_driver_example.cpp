#include "cass_driver.h"

#include <iostream>

#include "thirdparty/boost/thread/thread.hpp"
#include "thirdparty/gflags/gflags.h"

DECLARE_string(contact_ip_and_port);
DECLARE_string(log_level);
DECLARE_string(num_threads_io);
DECLARE_string(queue_size_io);
DECLARE_string(pending_req_low);
DECLARE_string(pending_req_high);
DECLARE_string(core_connections);
DECLARE_string(max_connections);

using namespace pushing;

void run_example(CassDriver& cass_driver) {
  // Store
  Message message;
  message.__set_receiver_id("aaaa");
  message.__set_timestamp("100000000000000");
  message.__set_msg_id("222");
  message.__set_group_id("");
  message.__set_msg("message # 222");
  message.__set_sender_id("44444444");

  auto store_cb = [](CassFuture* future, void* data) {
    CassError rc = cass_future_error_code(future);
    char* receiver = (char*)data;// data is a customized struct
    if (rc != CASS_OK)
      printf("store message for %s fail\n", receiver);
    else
      printf("store message for %s succeed\n", receiver);
  };
  char* cb_struct = "aaaa";//
  cass_driver.Store(store_cb, message, cb_struct);

  // Retrieve
  std::string receiver = "aaaa";
  CassDriver::Wrapper* wrapper = new CassDriver::Wrapper();
  wrapper->pmsgs = new std::vector<boost::shared_ptr<Message>>;
  char* cb = "I am a customized struct";//
  wrapper->cb_data = cb;//
  auto retrieve_cb = [=](bool success, CassDriver::Wrapper* data) {
    if (success) {
      std::cout << "success | msgs: " << data->pmsgs->size() << std::endl;
      for (int i = 0; i < data->pmsgs->size(); ++i) {
        printf("receiver: %s, timestamp: %s, msg_id: %s, group_id: %s,"
               " message: %s, sender: %s\n",
               data->pmsgs->at(i)->receiver_id.c_str(),
               data->pmsgs->at(i)->timestamp.c_str(),
               data->pmsgs->at(i)->msg_id.c_str(),
               data->pmsgs->at(i)->group_id.c_str(),
               data->pmsgs->at(i)->msg.c_str(),
               data->pmsgs->at(i)->sender_id.c_str());
      }
      std::cout << (char*)data->cb_data << std::endl;//
    } else {
      printf("fail\n");
    }
    delete data->pmsgs;
    delete data;
  };
  cass_driver.Retrieve(retrieve_cb, receiver, wrapper);
}

int main(int argc, char** argv) {
  FLAGS_contact_ip_and_port = "172.16.123.187:9042";
  google::ParseCommandLineFlags(&argc, &argv, false);
  CassDriver cass_driver;
  for (int i = 0; i < 1; ++i) {
    for (int j = 0; j < 100; ++j) {
      run_example(cass_driver);
    }
    boost::this_thread::sleep_for(boost::chrono::seconds(1));
  }

  boost::this_thread::sleep_for(boost::chrono::seconds(600));
  return 0;
}

