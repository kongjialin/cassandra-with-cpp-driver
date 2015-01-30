#include "storage/random_message.h"

#include <stdlib.h>
#include <string>

#include "storage/murmurhash3.h"
#include "thirdparty/cass/Cassandra.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/thrift/protocol/TBinaryProtocol.h"
#include "thirdparty/thrift/transport/TSocket.h"
#include "thirdparty/thrift/transport/TTransportUtils.h"

DEFINE_string(ring_info_ip, "172.16.123.187",
              "cassandra server ip from which to fetch the ring info");

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::org::apache::cassandra;

namespace pushing {

std::string RandomMessage::charset_ = "0123456789"
                                      "abcdefghijklmnopqrstuvwxyz";

int RandomMessage::size_ = charset_.length();
std::vector<std::string> RandomMessage::row_key_set_;

void RandomMessage::PrepareRowKeySet() {
  boost::shared_ptr<TTransport> socket(new TSocket(FLAGS_ring_info_ip, 9160));
  boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  boost::shared_ptr<CassandraClient> client(new CassandraClient(protocol));
  transport->open();
  std::string keyspace = "offline_keyspace";
  std::vector<TokenRange> ring;
  client->describe_ring(ring, keyspace);
  transport->close();
  std::multimap<std::string, boost::shared_ptr<Range>> server_to_range;
  for (auto& range : ring)  {
    int64_t left = strtoll(range.start_token.c_str(), NULL, 10);
    int64_t right = strtoll(range.end_token.c_str(), NULL, 10);
    boost::shared_ptr<Range> r(new Range(left, right));
    for (auto& host : range.endpoints) {
      server_to_range.insert(
          std::pair<std::string, boost::shared_ptr<Range>>(host, r));
    }
  }

  srand(time(NULL));
  for (auto it = server_to_range.begin(); it != server_to_range.end();
       it = server_to_range.upper_bound(it->first)) {
    while (true) {
      std::string row_key;
      for (int i = 0; i < 10; ++i)
        row_key += charset_[rand() % size_];
      int64_t hash[2];
      MurmurHash3_x64_128(row_key.c_str(), row_key.size(), 0, hash);
      int64_t token = hash[0];
      auto inner_it = it;
      for (; inner_it != server_to_range.upper_bound(it->first); ++inner_it) {
        if (inner_it->second->Contain(token)) {
          row_key_set_.push_back(row_key);
          break;
        } else {
          ++inner_it;
        }
      }
      if (inner_it != server_to_range.upper_bound(it->first))
        break;
    }
  }
}

void RandomMessage::GenerateMessage(Message* message) {
  message->__set_receiver_id(row_key_set_[rand() % row_key_set_.size()]);
  message->__set_timestamp("100000000000000");
  message->__set_msg_id("11111");
  message->__set_group_id("12345");
  message->__set_msg("This is a message generated in Cassandra stress testing");
  message->__set_sender_id("kongjialin92@gmail.com");
}

void RandomMessage::GenerateString(std::string* str) {
  *str = row_key_set_[rand() % row_key_set_.size()];
}

RandomMessage::Range::Range(int64_t left, int64_t right) : left_(left),
                                                           right_(right) {
}

bool RandomMessage::Range::Contain(int64_t token) {
  if (left_ >= right_) {
    if (token > left_)
      return true;
    else
      return right_ >= token;
  } else {
    return token > left_ && right_ >= token;
  }
}

}// namespace pushing

