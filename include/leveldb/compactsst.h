
#ifndef COMPACTSST_H_
#define COMPACTSST_H_

#include <cstdint>
#include <vector>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

using FileMeta = std::pair<void*, uint32_t>; // buffer, file size in bytes

LEVELDB_EXPORT Status CompactSST(int level, uint64_t sequence,
                                 std::vector<FileMeta>& input_files,
                                 std::vector<FileMeta>& input2_files,
                                 std::vector<FileMeta>& output_files,
                                 void* result_buf);

}

#endif
