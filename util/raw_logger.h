#ifndef STORAGE_LEVELDB_UTIL_RAW_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_RAW_LOGGER_H_

#include <sys/time.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sstream>
#include <thread>

#include "leveldb/env.h"

namespace leveldb {

class RawLogger final : public Logger {
 public:
  explicit RawLogger(std::FILE* fp) : fp_(fp) {}

  ~RawLogger() override {}

  void Logv(const char* format, va_list arguments) override {
    std::va_list arguments_copy;
    va_copy(arguments_copy, arguments);
    fprintf(stderr, "LevelDB LOG: ");
    vfprintf(stderr, format, arguments);
    if (format[strlen(format) - 1] != '\n')
      fprintf(stderr, "\n");
    va_end(arguments_copy);
  }

 private:
  std::FILE* const fp_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_RAW_LOGGER_H_

