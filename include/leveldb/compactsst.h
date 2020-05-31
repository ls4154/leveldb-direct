
#ifndef STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_
#define STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_

#include <string>

#include "leveldb/env.h"
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

LEVELDB_EXPORT Status CompactSST(Env* env);

}

#endif // STORAGE_LEVELDB_INCLUDE_COMPACTSST_H_
