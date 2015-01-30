#ifndef STORAGE_RANDOM_MESSAGE_H_
#define STORAGE_RANDOM_MESSAGE_H_

#include <stdint.h>
#include <map>
#include <vector>

#include "common/idl/message_types.h"

namespace pushing {

class RandomMessage {
 public:
  static void PrepareRowKeySet();
  static void GenerateMessage(Message* message);
  static void GenerateString(std::string* str);

 private:
  class Range {
   public:
    Range(int64_t left, int64_t right);
    bool Contain(int64_t token);

   private:
    int64_t left_;
    int64_t right_;
  };

  static std::string charset_;
  static int size_;
  static std::vector<std::string> row_key_set_;
};

} // namespace pushing

#endif // STORAGE_RANDOM_MESSAGE_H_
