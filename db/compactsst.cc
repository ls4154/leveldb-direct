#include <cstdio>
#include <cstdint>
#include <unistd.h>
#include <fcntl.h>

#include "db/dbformat.h"
#include "db/version_set.h"

#include "leveldb/compactsst.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "table/iterator_wrapper.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"

namespace leveldb {
namespace {

Env* g_env;

struct CompactionInfo {
  Options* opts;
  int level;
  std::vector<RandomAccessFile*> infiles[2];
  std::vector<int> infile_sizes[2];
  std::vector<WritableFile*> outfiles;
  std::vector<int> outfile_sizes;
  std::vector<InternalKey> outfile_smallest;
  std::vector<InternalKey> outfile_largest;
  uint32_t out_cnt;
  SequenceNumber smallest_snapshot;
  uint64_t max_output_file_size;
  InternalKeyComparator* icmp;
  void* result_buf;
};

CompactionInfo* MakeCompactionInfo(int level, uint64_t sequence,
                                  std::vector<FileMeta>& input_files,
                                  std::vector<FileMeta>& input2_files,
                                  std::vector<FileMeta>& output_files,
                                  void* result_buf) {
  CompactionInfo* ci = new CompactionInfo;

  ci->opts = new Options;
  ci->level = level;
  ci->out_cnt = 0;
  ci->smallest_snapshot = sequence;
  ci->max_output_file_size = 4 * 1000 * 1000;
  ci->outfile_sizes.resize(output_files.size());
  ci->outfile_smallest.resize(output_files.size());
  ci->outfile_largest.resize(output_files.size());
  ci->icmp = new InternalKeyComparator(ci->opts->comparator);
  ci->result_buf = result_buf;

  ci->opts->comparator = ci->icmp;

  fprintf(stderr, "level %d\n", level);
  for (FileMeta& fm : input_files) {
    std::string fname((char*)&fm.first, 4);
    fname.append((char*)&fm.second, 4);

    RandomAccessFile* infile;
    g_env->NewRandomAccessFile(fname, &infile);
    ci->infiles[0].push_back(infile);

    ci->infile_sizes[0].push_back(fm.second);
  }
  fprintf(stderr, "level %d\n", level+1);
  for (FileMeta& fm : input2_files) {
    std::string fname((char*)&fm.first, 4);
    fname.append((char*)&fm.second, 4);

    RandomAccessFile* infile;
    g_env->NewRandomAccessFile(fname, &infile);
    ci->infiles[1].push_back(infile);

    ci->infile_sizes[1].push_back(fm.second);
  }
  fprintf(stderr, "output\n");
  for (FileMeta& fm : output_files) {
    std::string fname((char*)&fm.first, 4);
    fname.append((char*)&fm.second, 4);

    WritableFile* outfile;
    g_env->NewWritableFile(fname, &outfile);
    ci->outfiles.push_back(outfile);
  }

  return ci;
}

class LevelFileIterator : public Iterator {
 public:
  LevelFileIterator(const std::vector<RandomAccessFile*>& files, 
                    const std::vector<int>& sizes,
                    const Options& options, const ReadOptions& roptions)
    : files_(files), sizes_(sizes), options_(options), roptions_(roptions),
      iter_(nullptr), idx_(0) {
  }
  virtual void Seek(const Slice& target) {
    assert(0);
  }
  virtual void SeekToFirst() {
    idx_ = 0;
    Table* tbl;
    Status s = Table::Open(options_, files_[0], sizes_[0], &tbl);
    if (!s.ok()) {
      fprintf(stderr, "Table open error\n");
      exit(1);
    }
    iter_.Set(tbl->NewIterator(roptions_));
    iter_.SeekToFirst();
  }
  virtual void SeekToLast() {
    assert(0);
  }
  virtual void Next() {
    iter_.Next();
    if (!iter_.Valid()) {
      idx_++;
      if (idx_ < files_.size()) {
        Table* tbl;
        Status s = Table::Open(options_, files_[idx_], sizes_[idx_], &tbl);
        if (!s.ok()) {
          fprintf(stderr, "Table open error\n");
          exit(1);
        }
        iter_.Set(tbl->NewIterator(roptions_));
        iter_.SeekToFirst();
      }
    }
  }
  virtual void Prev() {
    assert(0);
  }
  virtual bool Valid() const {
    return iter_.Valid();
  }
  virtual Slice key() const {
    return iter_.key();
  }
  virtual Slice value() const {
    return iter_.value();
  }
  virtual Status status() const { return Status::OK(); }

 private:
  const std::vector<RandomAccessFile*>& files_;
  const std::vector<int>& sizes_;
  const Options& options_;
  const ReadOptions& roptions_;
  IteratorWrapper iter_;
  int idx_;
};

Iterator* MakeInputIterator(CompactionInfo* ci) {
  ReadOptions ropts;

  fprintf(stderr, "Make input iterator\n");

  const int space = ci->level == 0 ? ci->infiles[0].size() + 1 : 2;
  Iterator** iter_list = new Iterator*[space];
  int num = 0;

  for (int which = 0; which < 2; which++) {
    fprintf(stderr, "which %d\n", which);
    if (!ci->infiles[which].empty()) {
      if (ci->level + which == 0) {
        const std::vector<RandomAccessFile*>& files = ci->infiles[which];
        const std::vector<int>& sizes = ci->infile_sizes[which];
        for (size_t i = 0; i < files.size(); i++) {
          fprintf(stderr, "  file %lu\n", (long unsigned)i);
          Table* tbl;
          Status s = leveldb::Table::Open(*ci->opts, files[i], sizes[i], &tbl);
          if (!s.ok()) {
            fprintf(stderr, "  Table open error\n");
            exit(1);
          }
          //fprintf(stderr, "   open done\n");
          iter_list[num++] = tbl->NewIterator(ropts);
        }
      } else {
        const std::vector<RandomAccessFile*>& files = ci->infiles[which];
        const std::vector<int>& sizes = ci->infile_sizes[which];
        iter_list[num++] = new LevelFileIterator(files, sizes, *ci->opts, ropts);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(ci->icmp, iter_list, num);
  delete[] iter_list;
  return result;
}

bool DoCompaction(CompactionInfo* ci) {
  fprintf(stderr, "DoCompaction start\n");
  Env* env = g_env;

  Iterator* input = MakeInputIterator(ci);
  TableBuilder* builder = nullptr;
  WritableFile* outfile = nullptr;
  int out_idx = -1;

  fprintf(stderr, "seek to first\n");

  input->SeekToFirst();

  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  const Comparator* ucmp = ci->icmp->user_comparator();

  int drop_cnt = 0;

  fprintf(stderr, "start compaction loop\n");
  for (; input->Valid();) {
    Slice key = input->key();

    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          ucmp->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= ci->smallest_snapshot) {
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
    //fprintf(stderr, "key: %s\n", ikey.user_key.ToString().c_str());

    if (!drop) {
      if (builder == nullptr) {
        out_idx++;
        if (out_idx == ci->outfiles.size()) {
          fprintf(stderr, "Out of output files\n");
          exit(1);
        }
        builder = new TableBuilder(*ci->opts, ci->outfiles[out_idx]);
      }
      if (builder->NumEntries() == 0) {
        ci->outfile_smallest[out_idx].DecodeFrom(key);
      }
      ci->outfile_largest[out_idx].DecodeFrom(key);
      builder->Add(key, input->value());

      if (builder->FileSize() >= ci->max_output_file_size) {
        status = input->status();
        if (!status.ok()) {
          fprintf(stderr, "iterator error\n");
          exit(1);
        }
        uint64_t num_entries = builder->NumEntries();
        status = builder->Finish();
        uint64_t file_size = builder->FileSize();
        ci->outfile_sizes[out_idx] = file_size;
        delete builder;
        builder = nullptr;

        if (!status.ok()) {
          fprintf(stderr, "finish error\n");
          exit(1);
        }
        // no need to sync and close
        delete outfile;
        outfile = nullptr;

        fprintf(stderr, "Generated table %llu keys %llu bytes\n",
                        static_cast<unsigned long long>(num_entries),
                        static_cast<unsigned long long>(file_size));
      }
    } else {
      drop_cnt++;
    }
    input->Next();
  }
  fprintf(stderr, "loop out\n");
  fprintf(stderr, " dropped %d\n", drop_cnt);

  if (builder != nullptr) {
    status = input->status();
    if (!status.ok()) {
      fprintf(stderr, "iterator error\n");
      exit(1);
    }
    uint64_t num_entries = builder->NumEntries();
    status = builder->Finish();
    uint64_t file_size = builder->FileSize();
    ci->outfile_sizes[out_idx] = file_size;
    delete builder;
    builder = nullptr;

    if (!status.ok()) {
      fprintf(stderr, "finish error\n");
      exit(1);
    }
    // no need to sync and close
    delete outfile;
    outfile = nullptr;

    fprintf(stderr, "Generated table %llu keys %llu bytes\n",
        static_cast<unsigned long long>(num_entries),
        static_cast<unsigned long long>(file_size));
  }

  ci->out_cnt = out_idx + 1;

  delete input;
  input = nullptr;

  return true;
}

bool MakeResultInfo(CompactionInfo* ci) {
  Env* env = g_env;

  WritableFile* outfile;
  std::string fname((char*)&ci->result_buf, 4);
  int zero = 0;
  fname.append((char*)&zero, 4);
  Status status = env->NewWritableFile(fname, &outfile);
  if (!status.ok()) {
    fprintf(stderr, "Result file create failed\n");
    exit(1);
  }

  int total_size = 0;

  outfile->Append(Slice(reinterpret_cast<char*>(&ci->out_cnt), sizeof(uint32_t)));
  total_size += sizeof(uint32_t);
  for (int i = 0; i < ci->out_cnt; i++) {
    outfile->Append(Slice((char*)&ci->outfile_sizes[i], sizeof(uint32_t)));
    total_size += sizeof(uint32_t);
  }
  for (int i = 0; i < ci->out_cnt; i++) {
    uint32_t klen = ci->outfile_smallest[i].Encode().size();
    outfile->Append(Slice((char*)(&klen), sizeof(uint32_t)));
    total_size += sizeof(uint32_t);
    outfile->Append(ci->outfile_smallest[i].Encode());
    total_size += klen;

    klen = ci->outfile_largest[i].Encode().size();
    outfile->Append(Slice((char*)(&klen), sizeof(uint32_t)));
    total_size += sizeof(uint32_t);
    outfile->Append(ci->outfile_largest[i].Encode());
    total_size += klen;
  }

  if (total_size > 4 * 1024 * 1024) {
    fprintf(stderr, "out buf size\n");
    exit(1);
  }

  fprintf(stderr, "out buf size %d\n", total_size);

  delete outfile;

  return true;
}

}  // namespace

Status CompactSST(int level, uint64_t sequence, std::vector<FileMeta>& input_files,
                  std::vector<FileMeta>& input2_files, std::vector<FileMeta>& output_files,
                  void* result_buf) {
  fprintf(stderr, "CompactSST start\n");

  g_env = leveldb::Env::Default();

  CompactionInfo* ci = MakeCompactionInfo(level, sequence, input_files, input2_files, output_files, result_buf);
  bool ok = DoCompaction(ci);
  fprintf(stderr, "CompactSST done\n");
  MakeResultInfo(ci);

  /*
  static int ccnt = 0;
  fprintf(stderr, "creating debug output files %d\n", ++ccnt);
  for (int i = 0; i < ci->out_cnt; i++) {
    char fname[32];
    int size = ci->outfile_sizes[i];
    void* buf = output_files[i].first;
    sprintf(fname, "%d-%d.ldb", ccnt, i);
    int fd = open(fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
    int wcnt = write(fd, buf, size);
    fprintf(stderr, "  %s %dbytes\n", fname, size);
    if (wcnt < size) {
      fprintf(stderr, "  write only %d bytes\n", wcnt);
    }
  }
  */

  return Status::OK();
}

}  // namespace leveldb
