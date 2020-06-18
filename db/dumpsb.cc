#include <cstdio>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

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

  std::string sb_file(argv[1]);
  sb_file += "/sb";
  int fd = open(sb_file.c_str(), O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "error open %s\n", sb_file.c_str());
    return 1;
  }

  void* sb_mmap_base = mmap(nullptr, sizeof(SuperBlock), PROT_READ, MAP_SHARED, fd, 0);
  if (sb_mmap_base == MAP_FAILED) {
    perror("mmap failed");
    exit(1);
  }
  close(fd);

  SuperBlock* sb_ptr = reinterpret_cast<SuperBlock*>(sb_mmap_base);
  FileMeta* sb_meta = &sb_ptr->sb_meta[0];
  if (sb_meta->sb_magic != LDBFS_MAGIC) {
    fprintf(stderr, "wrong magic number\n");
    return 1;
  }

  for (int i = 1; i < BLK_CNT; i++) {
    FileMeta* meta_ent = &sb_ptr->sb_meta[i];
    if (meta_ent->f_name_len > 0) {
      printf("%3d: %s %ubytes\n", i, meta_ent->f_name, (unsigned)meta_ent->f_size);
    }
  }

  munmap(sb_mmap_base, sizeof(SuperBlock));

  return 0;
}
