#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "leveldb/env.h"

// TODO: use single header file for config constants
#define LDBFS_MAGIC (0xe51ab1541542020full)

#define META_SIZE (128)
#define MAX_NAMELEN (META_SIZE - 8)
#define BLK_CNT (512)
#define FS_SIZE (BLK_SIZE * BLK_CNT)

struct FileMeta {
  union {
    struct {
      uint32_t f_size;
      uint16_t f_next_blk;
      uint8_t  f_name_len;
    };
    uint64_t   sb_magic;
  };
  char         f_name[MAX_NAMELEN];
};

struct SuperBlock {
  FileMeta sb_meta[BLK_CNT];
};

int main(int argc, char** argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: dumpsb <dbname>\n");
    return 1;
  }

  leveldb::Env* env = leveldb::Env::Default();

  std::string db_name(argv[1]);
  env->CreateDir(db_name);

  std::vector<std::string> filenames;
  env->GetChildren(db_name, &filenames);

  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t fsize;
    env->GetFileSize(filenames[i], &fsize);
    printf("%s %lu bytes\n", filenames[i].c_str(), fsize);
  }

  return 0;
}

