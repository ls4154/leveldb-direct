#include <bits/stdint-uintn.h>
#include <cstdio>
#include <cstdint>
#include <string>

#include "leveldb/compactsst.h"

namespace leveldb {
namespace {

#define MAX_FILE_CNT (12)
#define MAX_OBJ_SIZE (4 * 1024 * 1024)

struct CompactionShared {
  char input_file[MAX_FILE_CNT][MAX_OBJ_SIZE];
  char input2_file[MAX_FILE_CNT][MAX_OBJ_SIZE];
  char output_file[MAX_FILE_CNT * 2][MAX_OBJ_SIZE];
  int state;
  void* host_buf;
};

struct InputData {
  uint32_t level;
  uint64_t sequence;
  uint32_t input_cnt;
  uint32_t input2_cnt;
  uint32_t output_cnt;
  char data[];
};

bool StartCompactionDaemon() {
  void* mmap_base; // TODO mmap
  CompactionShared* cshared = reinterpret_cast<CompactionShared*>(mmap_base);
  int* state = &cshared->state;
  while (1) {
    while (*state == 0);
    *state = 1;

    InputData* id = reinterpret_cast<InputData*>(cshared->host_buf);

    fprintf(stderr, "start compaction\n");
    fprintf(stderr, "level %u, sequence %llu\n", id->level, (long long)id->sequence);

    if (id->input_cnt > MAX_FILE_CNT) {
      fprintf(stderr, "too many input files\n");
      exit(1);
    }
    std::vector<FileMeta> input_files;
    unsigned offset = 0;
    uint32_t* size_arr = reinterpret_cast<uint32_t*>(&id->data[offset]);
    fprintf(stderr, "input files %u\n", id->input_cnt);
    for (int i = 0; i < id->input_cnt; i++) {
      fprintf(stderr, "    %p %u\n", &cshared->input_file[i], size_arr[i]);
      input_files.push_back({&cshared->input_file[i], size_arr[i]});
    }

    if (id->input2_cnt > MAX_FILE_CNT) {
      fprintf(stderr, "too many input2 files\n");
      exit(1);
    }
    std::vector<FileMeta> input2_files;
    offset = id->input_cnt * sizeof(uint32_t);
    size_arr = reinterpret_cast<uint32_t*>(&id->data[offset]);
    fprintf(stderr, "input2 files %u\n", id->input2_cnt);
    for (int i = 0; i < id->input2_cnt; i++) {
      fprintf(stderr, "    %p %u\n", &cshared->input2_file[i], size_arr[i]);
      input2_files.push_back({&cshared->input2_file[i], size_arr[i]});
    }

    if (id->output_cnt > MAX_FILE_CNT) {
      fprintf(stderr, "too many output files\n");
      exit(1);
    }
    std::vector<FileMeta> output_files;
    fprintf(stderr, "output files %u\n", id->output_cnt);
    for (int i = 0; i < id->output_cnt; i++) {
      fprintf(stderr, "    %p\n", &cshared->output_file[i]);
      input2_files.push_back({&cshared->output_file[i], 0});
    }
    CompactSST(id->level, id->sequence, input_files, input2_files, output_files);
  }
  return true;
}

}  // namespace
}  // namespace leveldb

int main(int argc, char** argv) {
  bool ok = leveldb::StartCompactionDaemon();
  return (ok ? 0 : 1);
}
