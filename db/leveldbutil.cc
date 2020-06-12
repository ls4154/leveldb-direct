// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdlib>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "leveldb/dumpfile.h"
#include "leveldb/compactsst.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {
namespace {

class StdoutPrinter : public WritableFile {
 public:
  virtual Status Append(const Slice& data) {
    fwrite(data.data(), 1, data.size(), stdout);
    return Status::OK();
  }
  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }
};

bool HandleDumpCommand(Env* env, char** files, int num) {
  StdoutPrinter printer;
  bool ok = true;
  for (int i = 0; i < num; i++) {
    Status s = DumpFile(env, files[i], &printer);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      ok = false;
    }
  }
  return ok;
}

// Parse comma seperated list
bool ParseFileList(char* s, std::vector<std::string>& v) {
  char* token = strtok(s, ", ");
  while (token) {
    v.push_back(token);
    token = strtok(nullptr, ", ");
  }
  return true;
}

bool HandleCompactCommand(Env* env, char** argv, int argc) {
  if (argc < 7) {
    fprintf(stderr, "compact: missing arguments\n");
    return false;
  }

  std::string dbname;
  int level;
  std::vector<std::string> in_files;
  std::vector<std::string> in_files2;
  std::vector<std::string> out_files;
  uint64_t seqnum;
  uint64_t max_file_size;

  dbname = argv[0];
  level = atoi(argv[1]);
  ParseFileList(argv[2], in_files);
  ParseFileList(argv[3], in_files2);
  ParseFileList(argv[4], out_files);
  seqnum = strtoull(argv[5], nullptr, 10);
  max_file_size = strtoull(argv[6], nullptr, 10);

  Status s = CompactSST(env, dbname, level, in_files, in_files2, out_files, seqnum, max_file_size);
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return false;
  }
  return true;
}

}  // namespace
}  // namespace leveldb

static void Usage() {
  fprintf(stderr,
          "Usage: leveldbutil command...\n"
          "   dump files...                                    -- dump contents of specified files\n"
          "   compact dbname level infiles infiles2 outfiles sequence filesize -- compact sstables\n");
}

int main(int argc, char** argv) {
  leveldb::Env* env = leveldb::Env::Default();
  bool ok = true;
  if (argc < 2) {
    Usage();
    ok = false;
  } else {
    std::string command = argv[1];
    if (command == "dump") {
      ok = leveldb::HandleDumpCommand(env, argv + 2, argc - 2);
    } else if (command == "compact") {
      ok = leveldb::HandleCompactCommand(env, argv + 2, argc - 2);
    } else {
      Usage();
      ok = false;
    }
  }
  return (ok ? 0 : 1);
}
