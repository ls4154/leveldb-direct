
#include <stdint.h>
#include <cassert>
#include <stdio.h>

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
  Options* opts;
  int level;
  std::vector<TableMeta> inputs[2];
  std::vector<TableMeta> outputs;
  SequenceNumber smallest_snapshot;
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

CompactionInfo* MakeCompctionInfo(int level, std::vector<std::string>& in_files,
                                             std::vector<std::string>& in_files2,
                                             std::vector<std::string>& out_files,
                                             uint64_t seqnum) {

  Env* env = Env::Default();

  CompactionInfo* ci = new CompactionInfo;

  std::string dbname = "testdb";

  ReadOptions ropts;

  ci->opts = new Options;
  ci->icmp = new InternalKeyComparator(ci->opts->comparator);
  ci->table_cache = new TableCache(dbname, *ci->opts, 100);
  ci->smallest_snapshot = seqnum;

  fprintf(stderr, "Level %d: ", level);
  for (std::string& s : in_files) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    fprintf(stderr, "%s", s.c_str());

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    std::string fname = TableFileName(dbname, fnum);
    Status status = env->GetFileSize(fname, &fsize);
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

    std::string fname = TableFileName(dbname, fnum);
    Status status = env->GetFileSize(fname, &fsize);
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

  for (std::string& s : out_files) {
    uint64_t fnum;
    uint64_t fsize;
    FileType ftype;

    ParseFileName(s, &fnum, &ftype);
    assert(ftype == kTableFile);

    fsize = 0;

    ci->outputs.push_back({fnum, fsize, InternalKey(), InternalKey()});
  }

  return ci;
}

bool DoCompaction(CompactionInfo* ci) {
  Iterator* input = MakeInputIterator(ci);

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
    }

    input->Next();
  }

  return true;
}

} // namespace

Status CompactSST(Env* env, int level, std::vector<std::string>& in_files,
                                       std::vector<std::string>& in_files2,
                                       std::vector<std::string>& out_files,
                                       uint64_t seqnum) {
  fprintf(stderr, "CompactSST\n");

  CompactionInfo* ci = MakeCompctionInfo(level, in_files, in_files2, out_files, seqnum);

  bool ok = DoCompaction(ci);
  if (!ok) {
    return Status::InvalidArgument("CompacSST error");
  }

  return Status::OK();
}

} // namespace leveldb
