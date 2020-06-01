
#ifndef STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_
#define STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_

#include <string>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

LEVELDB_EXPORT Status CompactSST(Env* env, int level,
                                           std::vector<std::string>& in_files,
                                           std::vector<std::string>& in_files2,
                                           std::vector<std::string>& out_files,
                                           uint64_t seqnum);

}

#endif // STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_