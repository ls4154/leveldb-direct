#include <cstdio>

#include "leveldb/compactsst.h"

namespace leveldb {
namespace {
}  // namespace

Status CompactSST(int level, uint64_t sequence, std::vector<FileMeta>& input_files,
                  std::vector<FileMeta>& input2_files, std::vector<FileMeta>& output_files) {
  return Status::OK();
}

}  // namespace leveldb
