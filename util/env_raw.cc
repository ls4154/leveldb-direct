// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <algorithm>
#include <queue>
#include <set>
#include <map>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/raw_logger.h"

extern "C" {
#include "spdk/stdinc.h"
#include "spdk/ioat.h"
#include "spdk/nvme.h"
#include "spdk/string.h"
#include "spdk/env.h"
}

namespace leveldb {

struct ctrlr_entry {
  struct spdk_nvme_ctrlr* ctrlr;
  struct ctrlr_entry* next;
  char name[1024];
};

struct ns_entry {
  struct spdk_nvme_ctrlr* ctrlr;
  struct spdk_nvme_ns* ns;
  struct ns_entry* next;
  struct spdk_nvme_qpair* qpair;
};

struct ctrlr_entry* g_controllers = NULL;
struct ns_entry* g_namespaces = NULL;
struct port::Mutex g_ns_mtx;
int g_sectsize;
int g_nsect;
int g_sect_per_blk;
char* g_spdkbuf;

bool g_vmd = false;

bool probe_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
              struct spdk_nvme_ctrlr_opts* opts)
{
  printf("Attaching to %s\n", trid->traddr);
  return true;
}

void register_ns(struct spdk_nvme_ctrlr* ctrlr, struct spdk_nvme_ns* ns)
{
  struct ns_entry *entry;

  if (!spdk_nvme_ns_is_active(ns))
    return;

  entry = static_cast<ns_entry*>(malloc(sizeof(struct ns_entry)));
  if (entry == NULL) {
    perror("ns_entry malloc");
    exit(1);
  }

  entry->ctrlr = ctrlr;
  entry->ns = ns;
  entry->next = g_namespaces;
  g_namespaces = entry;
}


void attach_cb(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
               struct spdk_nvme_ctrlr* ctrlr, const struct spdk_nvme_ctrlr_opts* opts)
{
  int nsid, num_ns;
  struct ctrlr_entry* entry;
  struct spdk_nvme_ns* ns;
  const struct spdk_nvme_ctrlr_data* cdata;

  entry = static_cast<ctrlr_entry*>(malloc(sizeof(struct ctrlr_entry)));
  if (entry == NULL) {
    perror("ctrlr_entry malloc");
    exit(1);
  }

  printf("Attachedto %s\n", trid->traddr);
  cdata = spdk_nvme_ctrlr_get_data(ctrlr);

  snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);
  entry->ctrlr = ctrlr;
  entry->next = g_controllers;
  g_controllers = entry;

  num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
  printf("Using controller %s with %d namespaces.\n", entry->name, num_ns);
  for (nsid = 1; nsid <= num_ns; nsid++) {
    ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL)
      continue;
    register_ns(ctrlr, ns);
  }
}

void cleanup(void)
{
  struct ns_entry *ns_entry = g_namespaces;
  struct ctrlr_entry *ctrlr_entry = g_controllers;

  while (ns_entry) {
    struct ns_entry *next = ns_entry->next;
    free(ns_entry);
    ns_entry = next;
  }

  while (ctrlr_entry) {
    struct ctrlr_entry *next = ctrlr_entry->next;
    spdk_nvme_detach(ctrlr_entry->ctrlr);
    free(ctrlr_entry);
    ctrlr_entry = next;
  }
}

void write_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
  int* st = static_cast<int*>(arg);
  *st = 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "spdk write cpl error\n");
    *st = 2;
  }
}

void read_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
  int* st = static_cast<int*>(arg);
  *st = 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "spdk read cpl error\n");
    *st = 2;
  }
}

namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

constexpr const size_t kWritableFileBufferSize = 65536;

Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}
#define LDBFS_MAGIC 0x1234567890abcdefull
#define BLK_SIZE (8ULL * 1024 * 1024)
#define BLK_CNT (1024)
#define FS_SIZE (BLK_SIZE * BLK_CNT)

#define BLK_META_SIZE (8 * 4)
#define MAX_NAMELEN (1024 - BLK_META_SIZE)
#define MAX_PAYLOAD (BLK_SIZE - 1024)

struct SuperBlock {
    uint64_t sb_magic;
    uint64_t sb_fcnt;
    uint64_t sb_reserved0;
    uint64_t sb_reserved1;
};

enum {
    FTYPE_FREE = 0,
    FTYPE_REG = 1,
    FTYPE_DIR = 2,
    FTYPE_LOCK = 3,
    FTYPE_DATA = 4
};

struct RawFile {
    uint64_t f_type;
    uint64_t f_size;
    uint64_t f_name_len;
    uint64_t f_next_blk;
    char f_name[MAX_NAMELEN];
    char f_payload[];
};

static Slice Basename(const std::string& filename) {
  std::string::size_type separator_pos = filename.rfind('/');
  if (separator_pos == std::string::npos) {
    return Slice(filename);
  }
  // The filename component should not contain a path separator. If it does,
  // the splitting was done incorrectly.
  assert(filename.find('/', separator_pos + 1) == std::string::npos);

  return Slice(filename.data() + separator_pos + 1,
      filename.length() - separator_pos - 1);
}

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, char* file_buf, uint32_t idx)
      : filename_(filename), buf_(file_buf), offset_(0), idx_(idx) {
    int rc;
    int compl_status = 0;
    g_ns_mtx.Lock();
    spdk_nvme_ns_cmd_read(g_namespaces->ns, g_namespaces->qpair, buf_,
                           g_sect_per_blk * idx_, g_sect_per_blk,
                           read_complete, &compl_status, 0);
    if (rc != 0) {
      fprintf(stderr, "spdk read failed\n");
      exit(1);
    }
    while (!compl_status)
      spdk_nvme_qpair_process_completions(g_namespaces->qpair, 0);
    file_ptr_ = reinterpret_cast<RawFile*>(buf_);
    g_ns_mtx.Unlock();
  }
  ~PosixSequentialFile() override {
    spdk_free(buf_);
  }

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    n = std::min(n, file_ptr_->f_size - offset_);
    memcpy(scratch, file_ptr_->f_payload + offset_, n);
    *result = Slice(scratch, n);
    //*result = Slice(file_ptr_->f_payload + offset_, n);
    offset_ += n;
    return status;
  }

  Status Skip(uint64_t n) override {
    offset_ += n;
    if (offset_ > MAX_PAYLOAD)
      return PosixError(filename_, errno);
    return Status::OK();
  }

 private:
  const std::string filename_;
  char* buf_;
  RawFile *file_ptr_;
  off_t offset_;
  uint32_t idx_;
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
  // instance, and will be used to determine if .
  PosixRandomAccessFile(std::string filename, char* file_buf, uint32_t idx)
      : filename_(std::move(filename)), buf_(file_buf), idx_(idx) {
    int rc;
    int compl_status = 0;
    g_ns_mtx.Lock();
    spdk_nvme_ns_cmd_read(g_namespaces->ns, g_namespaces->qpair, buf_,
                           g_sect_per_blk * idx_, g_sect_per_blk,
                           read_complete, &compl_status, 0);
    if (rc != 0) {
      fprintf(stderr, "spdk read failed\n");
      exit(1);
    }
    while (!compl_status)
      spdk_nvme_qpair_process_completions(g_namespaces->qpair, 0);
    file_ptr_ = reinterpret_cast<RawFile*>(buf_);
    g_ns_mtx.Unlock();
  }

  ~PosixRandomAccessFile() override {
    spdk_free(buf_);
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    Status status;

    n = std::min(n, file_ptr_->f_size - offset);

    memcpy(scratch, file_ptr_->f_payload + offset, n);

    *result = Slice(scratch, n);

    return status;
  }

 private:
  const std::string filename_;

  char* buf_;
  RawFile *file_ptr_;
  uint32_t idx_;
};

class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, RawFile *file_ptr)
      : pos_(0),
        is_manifest_(IsManifest(filename)),
        filename_(filename),
        file_ptr_(file_ptr),
        dirname_(Dirname(filename_)) {
    offset_ = file_ptr->f_size;
  }

  ~PosixWritableFile() override {
  }

  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    assert(offset_ + write_size <= MAX_PAYLOAD);
    memcpy(file_ptr_->f_payload + offset_, write_data, write_size);

    offset_ += write_size;
    file_ptr_->f_size += write_size;

    return Status::OK();
  }

  Status Close() override {
    return Status::OK();
  }

  Status Flush() override {
    return Status::OK();
  }

  Status Sync() override {
    msync(file_ptr_->f_payload, file_ptr_->f_size, MS_SYNC);
    return Status::OK();
  }

 private:
  // Ensures that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power
  // failures.
  //
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.

  // Returns the directory name in a path pointing to a file.
  //
  // Returns "." if the path does not contain any directory separator.
  static std::string Dirname(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return std::string(".");
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return filename.substr(0, separator_pos);
  }

  // Extracts the file name from a path pointing to a file.
  //
  // The returned Slice points to |filename|'s data buffer, so it is only valid
  // while |filename| is alive and unchanged.
  static Slice Basename(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return Slice(filename);
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return Slice(filename.data() + separator_pos + 1,
                 filename.length() - separator_pos - 1);
  }

  // True if the given file is a manifest file.
  static bool IsManifest(const std::string& filename) {
    return Basename(filename).starts_with("MANIFEST");
  }

  // buf_[0, pos_ - 1] contains data to be written to fd_.
  char buf_[kWritableFileBufferSize];
  size_t pos_;

  const bool is_manifest_;  // True if the file's name starts with MANIFEST.
  const std::string filename_;
  const std::string dirname_;  // The directory of filename_.

  RawFile *file_ptr_;
  off_t offset_;
};

int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info;
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
  file_lock_info.l_whence = SEEK_SET;
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;  // Lock/unlock entire file.
  return ::fcntl(fd, F_SETLK, &file_lock_info);
}

// Instances are thread-safe because they are immutable.
class PosixFileLock : public FileLock {
 public:
  PosixFileLock(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}

  int fd() const { return fd_; }
  const std::string& filename() const { return filename_; }

 private:
  const int fd_;
  const std::string filename_;
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntrl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
class PosixLockTable {
 public:
  bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    bool succeeded = locked_files_.insert(fname).second;
    mu_.Unlock();
    return succeeded;
  }
  void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    locked_files_.erase(fname);
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_ GUARDED_BY(mu_);
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    static char msg[] = "PosixEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr);
    std::abort();
  }

  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    char* fbuf;
    uint32_t idx;
    if (file_table_.count(filename)) {
      fbuf = static_cast<char*>(
                    spdk_zmalloc(BLK_SIZE, 0x1000, static_cast<uint64_t*>(NULL),
                                 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA));
      if (fbuf == NULL) {
        fprintf(stderr, "SeqFile zmalloc failed\n");
        exit(1);
      }
      idx = file_table_[filename];
    } else {
      return PosixError(filename, ENOENT);
    }
    *result = new PosixSequentialFile(filename, fbuf, idx);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    char* fbuf;
    uint32_t idx;
    if (file_table_.count(filename)) {
      fbuf = static_cast<char*>(
                    spdk_zmalloc(BLK_SIZE, 0x1000, static_cast<uint64_t*>(NULL),
                                 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA));
      if (fbuf == NULL) {
        fprintf(stderr, "SeqFile zmalloc failed\n");
        exit(1);
      }
      idx = file_table_[filename];
    } else {
      return PosixError(filename, ENOENT);
    }
    *result = new PosixRandomAccessFile(filename, fbuf, idx);
    return Status::OK();
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    struct RawFile *fptr;
    if (file_table_.count(filename)) {
      fptr = reinterpret_cast<struct RawFile*>(
          static_cast<char*>(NULL)
          + BLK_SIZE * file_table_[filename]);
      fptr->f_size = 0; // delete if exists
    } else {
      file_table_.insert({filename, free_idx_++});
      fptr = reinterpret_cast<struct RawFile*>(
          static_cast<char*>(NULL)
          + BLK_SIZE * file_table_[filename]);
      strcpy(fptr->f_name, filename.c_str());
      fptr->f_name_len = filename.size();
      fptr->f_size = 0;
      fptr->f_type = FTYPE_REG;
    }
    *result = new PosixWritableFile(filename, fptr);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    struct RawFile *fptr;
    if (file_table_.count(filename)) {
      fptr = reinterpret_cast<struct RawFile*>(
          static_cast<char*>(NULL)
          + BLK_SIZE * file_table_[filename]);
    } else {
      file_table_.insert({filename, free_idx_++});
      fptr = reinterpret_cast<struct RawFile*>(
          static_cast<char*>(NULL)
          + BLK_SIZE * file_table_[filename]);
      strcpy(fptr->f_name, filename.c_str());
      fptr->f_name_len = filename.size();
      fptr->f_size = 0;
      fptr->f_type = FTYPE_REG;
    }
    *result = new PosixWritableFile(filename, fptr);
    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    return file_table_.count(filename);
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    result->clear();

    for (auto &it : file_table_) {
      result->emplace_back(Basename(it.first).ToString());
    }

    return Status::OK();
  }

  Status DeleteFile(const std::string& filename) override {
    if (!file_table_.count(filename)) {
      return PosixError(filename, ENOENT);
    }
    struct RawFile *fptr;
    fptr = reinterpret_cast<struct RawFile*>(
        static_cast<char*>(NULL) + BLK_SIZE * file_table_[filename]);
    fptr->f_type = FTYPE_FREE;
    file_table_.erase(filename);

    return Status::OK();
  }

  Status CreateDir(const std::string& dirname) override {
    return Status::OK();
  }

  Status DeleteDir(const std::string& dirname) override {
    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    if (!file_table_.count(filename)) {
      return PosixError(filename, ENOENT);
    }
    struct RawFile *fptr;
    fptr = reinterpret_cast<struct RawFile*>(
        static_cast<char*>(NULL) + BLK_SIZE * file_table_[filename]);
    *size = fptr->f_size;
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    if (!file_table_.count(from)) {
      return PosixError(from, ENOENT);
    }
    struct RawFile *fptr;
    fptr = reinterpret_cast<struct RawFile*>(
        static_cast<char*>(NULL) + BLK_SIZE * file_table_[from]);

    if (file_table_.count(to)) {
      struct RawFile *fptr2;
      fptr2 = reinterpret_cast<struct RawFile*>(
          static_cast<char*>(NULL) + BLK_SIZE * file_table_[to]);
      fptr2->f_type = FTYPE_FREE;
      file_table_.erase(to);
    }

    file_table_[to] = file_table_[from];
    file_table_.erase(from);

    strcpy(fptr->f_name, to.c_str());
    fptr->f_name_len = to.size();

    return Status::OK();
  }

  Status LockFile(const std::string& filename, FileLock** lock) override {
    int fd = 0;
    *lock = new PosixFileLock(fd, filename);
    return Status::OK();
  }

  Status UnlockFile(FileLock* lock) override {
    PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
    delete posix_file_lock;
    return Status::OK();
  }

  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;

  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override;

  Status GetTestDirectory(std::string* result) override {

    return Status::OK();
  }

  Status NewLogger(const std::string& filename, Logger** result) override {
    std::FILE* fp = nullptr;
    *result = new RawLogger(fp);
    return Status::OK();
  }

  uint64_t NowMicros() override {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
  }

  void SleepForMicroseconds(int micros) override { ::usleep(micros); }

 private:
  void BackgroundThreadMain();

  static void BackgroundThreadEntryPoint(PosixEnv* env) {
    env->BackgroundThreadMain();
  }

  // Stores the work item data in a Schedule() call.
  //
  // Instances are constructed on the thread calling Schedule() and used on the
  // background thread.
  //
  // This structure is thread-safe beacuse it is immutable.
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (*const function)(void*);
    void* const arg;
  };

  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);

  PosixLockTable locks_;  // Thread-safe.

  uint64_t dev_size_;
  std::map<std::string, uint64_t> file_table_;
  int free_idx_;
};

}  // namespace

PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false) {

  int rc;
  struct spdk_env_opts opts;

  g_ns_mtx.Lock();
  spdk_env_opts_init(&opts);
  opts.name = "leveldb";
  opts.shm_id = 0;
  if (spdk_env_init(&opts) < 0) {
    fprintf(stderr, "spdk_env_init failed\n");
    exit(1);
  }

  rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
  if (rc != 0) {
    fprintf(stderr, "spdk_nvme_probe failed\n");
    cleanup();
    exit(1);
  }

  if (g_controllers == NULL) {
    fprintf(stderr, "no NVMe contollers found\n");
    cleanup();
    exit(1);
  }

  struct ns_entry *ns_ent;
  ns_ent = g_namespaces;
  if (ns_ent != NULL) {
    ns_ent->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_ent->ctrlr, NULL, 0);
    if (ns_ent->qpair == NULL) {
      fprintf(stderr, "spdk_nvme_ctrlr_alloc_io_qpair failed\n");
      exit(1);
    }
  }

  g_sectsize = spdk_nvme_ns_get_sector_size(ns_ent->ns);
  g_nsect = spdk_nvme_ns_get_num_sectors(ns_ent->ns);
  fprintf(stderr, "nvme sector size %d\n", g_sectsize);
  fprintf(stderr, "nvme ns sector count %d\n", g_nsect);
  assert(BLK_SIZE % g_sectsize == 0);
  g_sect_per_blk = BLK_SIZE / g_sectsize;
  fprintf(stderr, "sectors per block %d\n", g_sect_per_blk);

  g_spdkbuf = static_cast<char*>(
                  spdk_zmalloc(BLK_SIZE, 0x1000, static_cast<uint64_t*>(NULL),
                  SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA));
  if (g_spdkbuf == NULL) {
    fprintf(stderr, "spdk_zmalloc failed\n");
    exit(1);
  }
  int compl_status = 0;
  rc = spdk_nvme_ns_cmd_read(ns_ent->ns, ns_ent->qpair, g_spdkbuf,
                             0, g_sect_per_blk,
                             read_complete, &compl_status, 0);
  if (rc != 0) {
    fprintf(stderr, "spdk read failed\n");
    exit(1);
  }

  while (!compl_status)
    spdk_nvme_qpair_process_completions(ns_ent->qpair, 0);

  dev_size_ = spdk_nvme_ns_get_size(ns_ent->ns);
  free_idx_ = 0;
  struct SuperBlock* sb_ptr = reinterpret_cast<struct SuperBlock*>(g_spdkbuf);
  if (sb_ptr->sb_magic == LDBFS_MAGIC) {
    fprintf(stderr, "found ldbfs\n");
    for (int i = 1; i < BLK_CNT; i++) {
      compl_status = 0;
      rc = spdk_nvme_ns_cmd_read(ns_ent->ns, ns_ent->qpair, g_spdkbuf,
                                 0, g_sect_per_blk,
                                 read_complete, &compl_status, 0);
      if (rc != 0) {
        fprintf(stderr, "spdk read failed\n");
        exit(1);
      }
      while (!compl_status)
        spdk_nvme_qpair_process_completions(ns_ent->qpair, 0);

      struct RawFile *fptr = reinterpret_cast<struct RawFile*>(g_spdkbuf);
      switch (fptr->f_type) {
        case FTYPE_FREE:
          if (free_idx_ == 0) {
            free_idx_ = i;
          }
          break;
        case FTYPE_REG:
          file_table_.insert({fptr->f_name, i});
          free_idx_ = 0;
          break;
        default:
          printf("unknown file\n");
          break;
      }
    }
  } else {
    sb_ptr->sb_magic = LDBFS_MAGIC;
    compl_status = 0;
    rc = spdk_nvme_ns_cmd_write(ns_ent->ns, ns_ent->qpair, g_spdkbuf,
                           0, g_sect_per_blk,
                           write_complete, &compl_status, 0);
    if (rc != 0) {
      fprintf(stderr, "spdk write failed\n");
      exit(1);
    }
    while (!compl_status)
      spdk_nvme_qpair_process_completions(ns_ent->qpair, 0);

    fprintf(stderr, "spdk sb write done\n");

    struct RawFile* fptr = reinterpret_cast<struct RawFile*>(g_spdkbuf);
    fptr->f_type = FTYPE_FREE;
    fptr->f_size = 0;
    fptr->f_name_len = 0;
    fptr->f_name[0] = '\0';
    for (int i = 1; i < BLK_CNT; i++) {
      fprintf(stderr, "write blk %d\n", i);
      rc = spdk_nvme_ns_cmd_write(ns_ent->ns, ns_ent->qpair, g_spdkbuf,
                                 g_sect_per_blk * i, g_sect_per_blk,
                                 write_complete, &compl_status, 0);
      if (rc != 0) {
        fprintf(stderr, "spdk write failed\n");
        exit(1);
      }
      while (!compl_status)
        spdk_nvme_qpair_process_completions(ns_ent->qpair, 0);
    }
    free_idx_ = 1;
  }
  g_ns_mtx.Unlock();
}

void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }

  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}

void PosixEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();

    // Wait until there is work to be done.
    while (background_work_queue_.empty()) {
      background_work_cv_.Wait();
    }

    assert(!background_work_queue_.empty());
    auto background_work_function = background_work_queue_.front().function;
    void* background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    background_work_function(background_work_arg);
  }
}

namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

}  // namespace

void PosixEnv::StartThread(void (*thread_main)(void* thread_main_arg),
                           void* thread_main_arg) {
  std::thread new_thread(thread_main, thread_main_arg);
  new_thread.detach();
}

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
}

Env* Env::Default() {
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb
