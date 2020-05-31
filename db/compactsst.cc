
#include "leveldb/compactsst.h"
#include "db/dbformat.h"

namespace leveldb {

LEVELDB_EXPORT Status CompactSST(Env* env, std::vector<std::string>& in_files,
                                           std::vector<std::string>& in_files2,
                                           std::vector<std::string>& out_files,
                                           uint64_t seqnum) {
  return Status::OK();
}

}
