
#include <bits/stdint-uintn.h>
#include <stdint.h>
#include <cassert>
#include <stdio.h>

#include <sys/time.h>

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

struct CompactionInfo {
  Env* env;
  Options* opts;
  std::string dbname;
  int level;
  std::vector<TableMeta> inputs[2];
  std::vector<TableMeta> outputs;
  uint32_t out_cnt;
  SequenceNumber smallest_snapshot;
  uint64_t max_output_file_size;
  InternalKeyComparator* icmp;
  TableCache* table_cache;
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

Iterator* MakeInputIterator(CompactionInfo* c) {
  ReadOptions ropts;

  const int space = c->level == 0 ? c->inputs[0].size() + 1: 2;
  Iterator** iter_list = new Iterator*[space];
  int num = 0;

  for (int which = 0; which < 2; which++) {
    if (!c->inputs[which].empty()) {
      if (c->level + which == 0) {
        const std::vector<TableMeta>& files = c->inputs[which];
        for (size_t i = 0; i < files.size(); i++) {
          iter_list[num++] = c->table_cache->NewIterator(ropts, files[i].number,
                                                         files[i].file_size);
        }
      } else {
        iter_list[num++] = NewTwoLevelIterator(
            new FileNumIterator(*c->icmp, &c->inputs[which]),
            &GetFileIterator, c->table_cache, ropts);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(c->icmp, iter_list, num);
  delete[] iter_list;
  return result;
}

CompactionInfo* MakeCompctionInfo(Env* env, std::string&dbname, int level,
                                            std::vector<std::string>& in_files,
                                            std::vector<std::string>& in_files2,
                                            std::vector<std::string>& out_files,
                                            uint64_t seqnum, uint64_t max_file_size) {
  CompactionInfo* ci = new CompactionInfo;

  ReadOptions ropts;

  ci->env = env;
  ci->opts = new Options;
  ci->dbname = dbname;
  ci->icmp = new InternalKeyComparator(ci->opts->comparator);
  ci->table_cache = new TableCache(ci->dbname, *ci->opts, 100);
  ci->smallest_snapshot = seqnum;
  ci->max_output_file_size = max_file_size;

  fprintf(stderr, "Level %d: ", level);
  for (std::string& s : in_files) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    fprintf(stderr, "%s", s.c_str());

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    std::string fname = TableFileName(ci->dbname, fnum);
    Status status = ci->env->GetFileSize(fname, &fsize);
    if (!status.ok()) {
      fprintf(stderr, "\n%s not exists\n", fname.c_str());
      exit(1);
    }

    Iterator* titer = ci->table_cache->NewIterator(ropts, fnum, fsize);

    titer->SeekToFirst();
    Slice key = titer->key();
    // ParsedInternalKey ikey;
    // if (!ParseInternalKey(key, &ikey)) {
    //   assert(0);
    // }
    InternalKey ik_smallest;
    ik_smallest.DecodeFrom(key);
    fprintf(stderr, "[%s,", ik_smallest.user_key().ToString().c_str());

    titer->SeekToLast();
    key = titer->key();
    InternalKey ik_largest;
    ik_largest.DecodeFrom(key);
    fprintf(stderr, "%s] ", ik_largest.user_key().ToString().c_str());

    ci->inputs[0].push_back({fnum, fsize, ik_smallest, ik_largest});
  }
  fprintf(stderr, "\n"
                  "Level %d: ", level + 1);
  for (std::string& s : in_files2) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    fprintf(stderr, "%s", s.c_str());

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    std::string fname = TableFileName(ci->dbname, fnum);
    Status status = ci->env->GetFileSize(fname, &fsize);
    if (!status.ok()) {
      fprintf(stderr, "\n%s not exists\n", fname.c_str());
      exit(1);
    }

    Iterator* titer = ci->table_cache->NewIterator(ropts, fnum, fsize);

    titer->SeekToFirst();
    Slice key = titer->key();
    InternalKey ik_smallest;
    ik_smallest.DecodeFrom(key);
    fprintf(stderr, "[%s,", ik_smallest.user_key().ToString().c_str());

    titer->SeekToLast();
    key = titer->key();
    InternalKey ik_largest;
    ik_largest.DecodeFrom(key);
    fprintf(stderr, "%s] ", ik_largest.user_key().ToString().c_str());

    ci->inputs[1].push_back({fnum, fsize, ik_smallest, ik_largest});
  }
  fprintf(stderr, "\n");
  fprintf(stderr, "Outputs: ");
  for (std::string& s : out_files) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    fprintf(stderr, "%s ", s.c_str());

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    fsize = 0;

    ci->outputs.push_back({fnum, fsize, InternalKey(), InternalKey()});
  }
  fprintf(stderr, "\n");

  return ci;
}

bool DoCompaction(CompactionInfo* ci) {
  Env* env = ci->env;

  Iterator* input = MakeInputIterator(ci);
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

  const Comparator* ucmp = ci->icmp->user_comparator();

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

      if (last_sequence_for_key <= ci->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      }
      // TODO: check base level

      last_sequence_for_key = ikey.sequence;
    }

    if (!drop) {
      if (builder == nullptr) {
        if (out_cnt == ci->outputs.size()) {
          fprintf(stderr, "Out of output files\n");
          exit(1);
        }
        cur_output = &ci->outputs[out_cnt++];
        uint64_t fnum = cur_output->number;
        std::string fname = TableFileName(ci->dbname, fnum);
        status = env->NewWritableFile(fname, &outfile);
        if (!status.ok()) {
          fprintf(stderr, "NewWritableFile error %s\n", fname.c_str());
          exit(1);
        }
        builder = new TableBuilder(*ci->opts, outfile);
      }
      if (builder->NumEntries() == 0) {
        cur_output->smallest.DecodeFrom(key);
      }
      cur_output->largest.DecodeFrom(key);
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

  ci->out_cnt = out_cnt;

  delete input;
  input = nullptr;

  return true;
}

bool MakeResultInfo(CompactionInfo* ci, std::string output_name) {
  Env* env = ci->env;
  WritableFile* outfile = nullptr;
  Status status = env->NewWritableFile(ci->dbname + "/" + output_name, &outfile);
  if (!status.ok()) {
    fprintf(stderr, "NewWritableFile error %s\n", output_name.c_str());
    exit(1);
  }

  outfile->Append(Slice(reinterpret_cast<char*>(&ci->out_cnt), sizeof(uint32_t)));
  for (TableMeta& tm : ci->outputs) {
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

} // namespace

Status CompactSST(Env* env, std::string&dbname, int level,
                            std::vector<std::string>& in_files,
                            std::vector<std::string>& in_files2,
                            std::vector<std::string>& out_files,
                            uint64_t seqnum, uint64_t max_file_size) {
  CompactionInfo* ci = MakeCompctionInfo(env, dbname, level, in_files, in_files2,
                                               out_files, seqnum, max_file_size);

  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);
  bool ok = DoCompaction(ci);
  gettimeofday(&tv2, NULL);
  if (!ok) {
    return Status::InvalidArgument("Compaction error");
  }

  ok = MakeResultInfo(ci, "compact.out");
  if (!ok) {
    return Status::InvalidArgument("Make result error");
  }

  for (TableMeta& tm : ci->outputs) {
    if (tm.file_size == 0) {
      continue;
    }
    fprintf(stderr, "File %llu %s .. %s\n", static_cast<unsigned long long>(tm.number),
                                            tm.smallest.user_key().ToString().c_str(),
                                            tm.largest.user_key().ToString().c_str());
  }

  fprintf(stderr, "Compaction time %lld us\n",
          static_cast<long long>((tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_usec - tv1.tv_usec));

  return Status::OK();
}

} // namespace leveldb
