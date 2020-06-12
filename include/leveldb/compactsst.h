
#ifndef STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_
#define STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_

#include <string>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

LEVELDB_EXPORT Status CompactSST(Env* env, std::string& dbname, int level,
                                           std::vector<std::string>& in_files,
                                           std::vector<std::string>& in_files2,
                                           std::vector<std::string>& out_files,
                                           uint64_t seqnum, uint64_t max_file_size);

}

#endif // STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_
