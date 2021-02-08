
#include <stdio.h>
#include <assert.h>
#include <stdint.h>

#include "leveldb/compactsst.h"

#include "db/dbformat.h"
#include "db/db_iter.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

namespace {

struct TableMeta {
  uint64_t number;
  uint64_t file_size;
  InternalKey smallest, largest;
};

class MyCompaction {
public:
  MyCompaction(Env* env, std::string&db_name, int level,
               std::vector<std::string>& in_files,
               std::vector<std::string>& in_files2,
               std::vector<std::string>& out_files,
               uint64_t seqnum, uint64_t max_file_size);
  ~MyCompaction();
  bool DoCompaction();
  bool MakeResultInfo(std::string output_name);
  void PrintOutputInfo();
private:
  Iterator* MakeInputIterator();

  Env* env_;
  Options* opts_;
  std::string dbname_;
  int level_;
  std::vector<TableMeta> inputs_[2];
  std::vector<TableMeta> outputs_;
  uint32_t out_cnt_;
  SequenceNumber smallest_snapshot_;
  uint64_t max_output_file_size_;
  InternalKeyComparator* icmp_;
  TableCache* table_cache_;
};



class FileNumIterator : public Iterator {
 public:
  FileNumIterator(const InternalKeyComparator& icmp,
                  const std::vector<TableMeta>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {
  }
  virtual bool Valid() const { return index_ < flist_->size(); }
  virtual void Seek(const Slice& target) {
    assert(0);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_].largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_].number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_].file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<TableMeta>* const flist_;
  uint32_t index_;
  mutable char value_buf_[16];
};

Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

MyCompaction::MyCompaction(Env* env, std::string&db_name, int level,
                           std::vector<std::string>& in_files,
                           std::vector<std::string>& in_files2,
                           std::vector<std::string>& out_files,
                           uint64_t seqnum, uint64_t max_file_size) {
  ReadOptions ropts;

  env_ = env;
  opts_ = new Options;
  dbname_ = db_name;
  icmp_ = new InternalKeyComparator(opts_->comparator);
  table_cache_ = new TableCache(dbname_, *opts_, 100);
  smallest_snapshot_ = seqnum;
  max_output_file_size_ = max_file_size;

  opts_->comparator = icmp_;

  fprintf(stderr, "Inputs (Level %d)\n", level);
  for (std::string& s : in_files) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    fprintf(stderr, "%s ", s.c_str());

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    std::string fname = TableFileName(dbname_, fnum);
    Status status = env_->GetFileSize(fname, &fsize);
    if (!status.ok()) {
      fprintf(stderr, "\n%s not exists\n", fname.c_str());
      exit(1);
    }

    Iterator* titer = table_cache_->NewIterator(ropts, fnum, fsize);

    titer->SeekToFirst();
    Slice key = titer->key();
    InternalKey ik_smallest;
    ik_smallest.DecodeFrom(key);
    fprintf(stderr, " %s .. ", ik_smallest.user_key().ToString().c_str());

    titer->SeekToLast();
    key = titer->key();
    InternalKey ik_largest;
    ik_largest.DecodeFrom(key);
    fprintf(stderr, "%s\n", ik_largest.user_key().ToString().c_str());

    inputs_[0].push_back({fnum, fsize, ik_smallest, ik_largest});
  }
  fprintf(stderr, "Inputs (Level %d)\n", level + 1);
  for (std::string& s : in_files2) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    fprintf(stderr, " %s ", s.c_str());

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    std::string fname = TableFileName(dbname_, fnum);
    Status status = env_->GetFileSize(fname, &fsize);
    if (!status.ok()) {
      fprintf(stderr, "\n%s not exists\n", fname.c_str());
      exit(1);
    }

    Iterator* titer = table_cache_->NewIterator(ropts, fnum, fsize);

    titer->SeekToFirst();
    Slice key = titer->key();
    InternalKey ik_smallest;
    ik_smallest.DecodeFrom(key);
    fprintf(stderr, " %s .. ", ik_smallest.user_key().ToString().c_str());

    titer->SeekToLast();
    key = titer->key();
    InternalKey ik_largest;
    ik_largest.DecodeFrom(key);
    fprintf(stderr, "%s\n", ik_largest.user_key().ToString().c_str());

    inputs_[1].push_back({fnum, fsize, ik_smallest, ik_largest});
  }
  fprintf(stderr, "Outputs\n");
  for (std::string& s : out_files) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    fprintf(stderr, " %s\n", s.c_str());

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    fsize = 0;

    outputs_.push_back({fnum, fsize, InternalKey(), InternalKey()});
  }
}

MyCompaction::~MyCompaction() {
  delete opts_;
  delete icmp_;
  delete table_cache_;
}

Iterator* MyCompaction::MakeInputIterator() {
  ReadOptions ropts;

  const int space = level_ == 0 ? inputs_[0].size() + 1: 2;
  Iterator** iter_list = new Iterator*[space];
  int num = 0;

  for (int which = 0; which < 2; which++) {
    if (!inputs_[which].empty()) {
      if (level_ + which == 0) {
        const std::vector<TableMeta>& files = inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          iter_list[num++] = table_cache_->NewIterator(ropts, files[i].number,
                                                       files[i].file_size);
        }
      } else {
        iter_list[num++] = NewTwoLevelIterator(
            new FileNumIterator(*icmp_, &inputs_[which]),
            &GetFileIterator, table_cache_, ropts);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(icmp_, iter_list, num);
  delete[] iter_list;
  return result;
}

bool MyCompaction::DoCompaction() {
  Iterator* input = MakeInputIterator();
  TableBuilder* builder = nullptr;
  WritableFile* outfile = nullptr;
  TableMeta* cur_output = nullptr;
  int out_cnt = 0;

  input->SeekToFirst();

  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  const Comparator* ucmp = icmp_->user_comparator();

  for (; input->Valid();) {
    Slice key = input->key();

    // TODO: check stop?

    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          ucmp->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= smallest_snapshot_) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      }
      // TODO: check base level

      last_sequence_for_key = ikey.sequence;
    }

    if (!drop) {
      if (builder == nullptr) {
        if (out_cnt == outputs_.size()) {
          fprintf(stderr, "Out of output files\n");
          exit(1);
        }
        cur_output = &outputs_[out_cnt++];
        uint64_t fnum = cur_output->number;
        std::string fname = TableFileName(dbname_, fnum);
        status = env_->NewWritableFile(fname, &outfile);
        if (!status.ok()) {
          fprintf(stderr, "NewWritableFile error %s\n", fname.c_str());
          exit(1);
        }
        builder = new TableBuilder(*opts_, outfile);
      }
      if (builder->NumEntries() == 0) {
        cur_output->smallest.DecodeFrom(key);
      }
      cur_output->largest.DecodeFrom(key);
      builder->Add(key, input->value());

      if (builder->FileSize() >= max_output_file_size_) {
        status = input->status();
        if (!status.ok()) {
          fprintf(stderr, "iterator error\n");
          exit(1);
        }
        uint64_t num_entries = builder->NumEntries();
        status = builder->Finish();
        uint64_t file_size = builder->FileSize();
        cur_output->file_size = file_size;
        delete builder;
        builder = nullptr;

        if (!status.ok()) {
          fprintf(stderr, "finish error\n");
          exit(1);
        }
        status = outfile->Sync();
        if (!status.ok()) {
          fprintf(stderr, "sync error\n");
          exit(1);
        }
        status = outfile->Close();
        if (!status.ok()) {
          fprintf(stderr, "close error\n");
          exit(1);
        }
        delete outfile;
        outfile = nullptr;

        // TODO verify table
        fprintf(stderr, "Generated table %llu, %llu keys, %llu bytes\n",
                        static_cast<unsigned long long>(cur_output->number),
                        static_cast<unsigned long long>(num_entries),
                        static_cast<unsigned long long>(file_size));
      }
    }

    input->Next();
  }

  if (builder != nullptr) {
    status = input->status();
    if (!status.ok()) {
      fprintf(stderr, "iterator error\n");
      exit(1);
    }
    uint64_t num_entries = builder->NumEntries();
    status = builder->Finish();
    uint64_t file_size = builder->FileSize();
    cur_output->file_size = file_size;
    delete builder;
    builder = nullptr;

    if (!status.ok()) {
      fprintf(stderr, "finish error\n");
      exit(1);
    }
    status = outfile->Sync();
    if (!status.ok()) {
      fprintf(stderr, "sync error\n");
      exit(1);
    }
    status = outfile->Close();
    if (!status.ok()) {
      fprintf(stderr, "close error\n");
      exit(1);
    }
    delete outfile;
    outfile = nullptr;

    // TODO verify table
    fprintf(stderr, "Generated table %llu, %llu keys, %llu bytes\n",
                    static_cast<unsigned long long>(cur_output->number),
                    static_cast<unsigned long long>(num_entries),
                    static_cast<unsigned long long>(file_size));
  }

  out_cnt_ = out_cnt;

  delete input;
  input = nullptr;

  return true;
}

bool MyCompaction::MakeResultInfo(std::string output_name) {
  WritableFile* outfile = nullptr;
  Status status = env_->NewWritableFile(dbname_ + "/" + output_name, &outfile);
  if (!status.ok()) {
    fprintf(stderr, "NewWritableFile error %s\n", output_name.c_str());
    exit(1);
  }

  outfile->Append(Slice(reinterpret_cast<char*>(&out_cnt_), sizeof(uint32_t)));
  for (TableMeta& tm : outputs_) {
    if (tm.file_size == 0) {
      continue;
    }
    uint32_t fnum = tm.number;
    uint32_t fsize = tm.file_size;
    outfile->Append(Slice(reinterpret_cast<char*>(&fnum), sizeof(uint32_t)));
    outfile->Append(Slice(reinterpret_cast<char*>(&fsize), sizeof(uint32_t)));

    uint32_t klen = tm.smallest.user_key().size();
    outfile->Append(Slice(reinterpret_cast<char*>(&klen), sizeof(uint32_t)));
    outfile->Append(tm.smallest.Encode());

    klen = tm.largest.user_key().size();
    outfile->Append(Slice(reinterpret_cast<char*>(&klen), sizeof(uint32_t)));
    outfile->Append(tm.largest.Encode());
  }
  outfile->Sync();
  if (!status.ok()) {
    fprintf(stderr, "sync error\n");
    exit(1);
  }
  outfile->Close();
  if (!status.ok()) {
    fprintf(stderr, "close error\n");
    exit(1);
  }
  delete outfile;

  return true;
}

void MyCompaction::PrintOutputInfo() {
  for (TableMeta& tm : outputs_) {
    if (tm.file_size == 0) {
      continue;
    }
    fprintf(stderr, "File %llu %s .. %s\n", static_cast<unsigned long long>(tm.number),
                                            tm.smallest.user_key().ToString().c_str(),
                                            tm.largest.user_key().ToString().c_str());
  }
}

} // namespace

Status CompactSST(Env* env, std::string&db_name, int level,
                  std::vector<std::string>& in_files,
                  std::vector<std::string>& in_files2,
                  std::vector<std::string>& out_files,
                  uint64_t seqnum, uint64_t max_file_size) {
  MyCompaction comp(env, db_name, level, in_files, in_files2,
                    out_files, seqnum, max_file_size);

  uint64_t start_time = env->NowMicros();
  bool ok = comp.DoCompaction();
  uint64_t finish_time = env->NowMicros();
  if (!ok) {
    return Status::InvalidArgument("Compaction error");
  }
  ok = comp.MakeResultInfo("compact.out");
  if (!ok) {
    return Status::InvalidArgument("Make result error");
  }
  comp.PrintOutputInfo();
  fprintf(stderr, "Compaction time %lld us\n",
          static_cast<long long>(finish_time - start_time));

  return Status::OK();
}

} // namespace leveldb
