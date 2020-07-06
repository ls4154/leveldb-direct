// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
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
#include "util/posix_logger.h"

#include "db/filename.h"

extern "C" {
#include "spdk/stdinc.h"
#include "spdk/ioat.h"
#include "spdk/nvme.h"
#include "spdk/string.h"
#include "spdk/env.h"
}

#ifdef NDEBUG
#define dprint(...) do { } while (0)
#else
#define dprint(...) do { fprintf(stderr, __VA_ARGS__); } while (0)
#endif

namespace leveldb {

#define ROUND_UP(N, S) (((N) + (S) - 1) / (S) * (S))
#define ROUND_DOWN(N, S) ((N) / (S) * (S))
#define DIV_ROUND_UP(N, S) (((N) + (S) - 1) / (S))

#define LDBFS_MAGIC (0xe51ab1541542020full)
#define OBJ_SIZE (4ULL * 1024 * 1024)       // 4 MiB per object
#define META_SIZE (128)
#define OBJ_CNT (OBJ_SIZE / META_SIZE)      // maximum objs in LDBFS
#define FS_SIZE (OBJ_SIZE * OBJ_CNT)
#define MAX_NAMELEN (META_SIZE - 8)

#define READ_UNIT (64ULL * 1024)            // Read granularity

static_assert(OBJ_SIZE % READ_UNIT == 0, "");

#define BUF_ALIGN (0x1000)

struct FileMeta {
  union {
    struct {
      uint32_t f_size;
      uint16_t f_reserved;
      uint8_t  f_name_len;
    };
    uint64_t   sb_magic;
  };
  char         f_name[MAX_NAMELEN];
};

struct SuperBlock {
  FileMeta sb_meta[OBJ_CNT];
};

struct ctrlr_entry {
  struct spdk_nvme_ctrlr* ctrlr;
  struct ctrlr_entry* next;
  char name[1024];
};

struct ns_entry {
  struct spdk_nvme_ctrlr* ctrlr;
  struct spdk_nvme_ns* ns;
  struct ns_entry* next;
  port::Mutex qpair_mtx;
  struct spdk_nvme_qpair* qpair;          // guarded by qpair_mtx
  struct spdk_nvme_qpair* qpair_comp;     // for compaction thread
};

struct ctrlr_entry* g_controllers = NULL; // guarded by g_ns_mtx
struct ns_entry* g_namespaces = NULL;     // guarded by g_ns_mtx
port::Mutex g_ns_mtx;

int g_sectsize;
int g_nsect;
int g_sect_per_obj;
uint64_t g_dev_size;

std::string g_dbname;

void* g_sbbuf;                            // guarded by g_fs_mtx
SuperBlock* g_sb_ptr;                     // guarded by g_fs_mtx
std::map<std::string, int> g_file_table;  // guarded by g_fs_mtx
std::queue<int> g_free_idx;               // guarded by g_fs_mtx

char* g_last_write_buf = nullptr;
int g_last_write_idx = -1;

port::Mutex g_fs_mtx;

thread_local bool compaction_thd = false;

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

  entry = new ns_entry;
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
  int* compl_status = static_cast<int*>(arg);
  *compl_status = 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "spdk write cpl error\n");
    *compl_status = 2;
  }
}

void read_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
  int* compl_status = static_cast<int*>(arg);
  *compl_status = 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "spdk read cpl error\n");
    *compl_status = 2;
  }
}

void write_from_buf(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                    void *buf, uint64_t lba, uint32_t cnt, int* chk_compl)
{
  int rc;

  if (chk_compl != nullptr) {
    rc = spdk_nvme_ns_cmd_write(ns, qpair, buf, lba, cnt, write_complete, chk_compl, 0);
    if (rc != 0) {
      fprintf(stderr, "spdk cmd wirte failed\n");
      exit(1);
    }
    return;
  }

  int l_chk_cpl = 0;
  rc = spdk_nvme_ns_cmd_write(ns, qpair, buf, lba, cnt, write_complete, &l_chk_cpl, 0);
  if (rc != 0) {
    fprintf(stderr, "spdk write failed\n");
    exit(1);
  }
  while (!l_chk_cpl)
    spdk_nvme_qpair_process_completions(qpair, 0);
}

void read_to_buf(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                 void *buf, uint64_t lba, uint32_t cnt, int* chk_compl)
{
  int rc;

  if (chk_compl != nullptr) {
    rc = spdk_nvme_ns_cmd_read(ns, qpair, buf, lba, cnt, read_complete, chk_compl, 0);
    if (rc != 0) {
      fprintf(stderr, "spdk cmd read failed\n");
      exit(1);
    }
    return;
  }

  int l_chk_cpl = 0;
  rc = spdk_nvme_ns_cmd_read(ns, qpair, buf, lba, cnt, read_complete, &l_chk_cpl, 0);
  if (rc != 0) {
    fprintf(stderr, "spdk read failed\n");
    exit(1);
  }
  while (!l_chk_cpl)
    spdk_nvme_qpair_process_completions(qpair, 0);
}

void check_completion(struct spdk_nvme_qpair* qpair)
{
  spdk_nvme_qpair_process_completions(qpair, 0);
}

void init_spdk(void)
{
  int rc;
  struct spdk_env_opts opts;

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

  if (g_namespaces == NULL) {
    fprintf(stderr, "no namespaces found\n");
    cleanup();
    exit(1);
  }

  struct ns_entry *ns_ent = g_namespaces;

  ns_ent->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_ent->ctrlr, NULL, 0);
  if (ns_ent->qpair == NULL) {
    fprintf(stderr, "spdk_nvme_ctrlr_alloc_io_qpair failed\n");
    exit(1);
  }

  ns_ent->qpair_comp = spdk_nvme_ctrlr_alloc_io_qpair(ns_ent->ctrlr, NULL, 0);
  if (ns_ent->qpair_comp == NULL) {
    fprintf(stderr, "spdk_nvme_ctrlr_alloc_io_qpair failed (compaction)\n");
    exit(1);
  }

  g_sectsize = spdk_nvme_ns_get_sector_size(ns_ent->ns);
  g_nsect = spdk_nvme_ns_get_num_sectors(ns_ent->ns);
  assert(OBJ_SIZE % g_sectsize == 0);
  g_sect_per_obj = OBJ_SIZE / g_sectsize;
  g_dev_size = spdk_nvme_ns_get_size(ns_ent->ns);

  dprint("nvme sector size %d\n", g_sectsize);
  dprint("nvme ns sector count %d\n", g_nsect);
  dprint("sectors per block %d\n", g_sect_per_obj);
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

Slice Basename(const std::string& filename) {
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
  PosixSequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(filename) {}
  ~PosixSequentialFile() override { close(fd_); }

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    while (true) {
      ::ssize_t read_size = ::read(fd_, scratch, n);
      if (read_size < 0) {  // Read error.
        if (errno == EINTR) {
          continue;  // Retry
        }
        status = PosixError(filename_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }

  Status Skip(uint64_t n) override {
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const int fd_;
  const std::string filename_;
};

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixMmapReadableFile final : public RandomAccessFile {
 public:
  // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
  // must be the result of a successful call to mmap(). This instances takes
  // over the ownership of the region.
  PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length)
      : mmap_base_(mmap_base),
        length_(length),
        filename_(std::move(filename)) {}

  ~PosixMmapReadableFile() override {
    ::munmap(static_cast<void*>(mmap_base_), length_);
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) override {
    if (offset + n > length_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  char* const mmap_base_;
  const size_t length_;
  const std::string filename_;
};

class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd)
      : pos_(0),
        fd_(fd),
        is_manifest_(IsManifest(filename)),
        filename_(std::move(filename)),
        dirname_(Dirname(filename_)) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      // Ignoring any potential errors
      Close();
    }
  }

  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    if (write_size < kWritableFileBufferSize) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);
  }

  Status Close() override {
    Status status = FlushBuffer();
    const int close_result = ::close(fd_);
    if (close_result < 0 && status.ok()) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

  Status Flush() override { return FlushBuffer(); }

  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifest refers to files that are not
    // yet on disk.
    Status status = SyncDirIfManifest();
    if (!status.ok()) {
      return status;
    }

    status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    return SyncFd(fd_, filename_);
  }

 private:
  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  Status WriteUnbuffered(const char* data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;  // Retry
        }
        return PosixError(filename_, errno);
      }
      data += write_result;
      size -= write_result;
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    Status status;
    if (!is_manifest_) {
      return status;
    }

    int fd = ::open(dirname_.c_str(), O_RDONLY);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  // Ensures that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power
  // failures.
  //
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.
  static Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
    // On macOS and iOS, fsync() doesn't guarantee durability past power
    // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
    // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
    // fsync().
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
      return Status::OK();
    }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
    bool sync_success = ::fdatasync(fd) == 0;
#else
    bool sync_success = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

    if (sync_success) {
      return Status::OK();
    }
    return PosixError(fd_path, errno);
  }

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
  int fd_;

  const bool is_manifest_;  // True if the file's name starts with MANIFEST.
  const std::string filename_;
  const std::string dirname_;  // The directory of filename_.
};

class RawSequentialFile final : public SequentialFile {
 public:
  RawSequentialFile(std::string filename, char* file_buf, int idx)
      : filename_(filename), buf_(file_buf), offset_(0), idx_(idx),
        size_(g_sb_ptr->sb_meta[idx].f_size) {
    struct ns_entry* ns_ent = g_namespaces;
    struct spdk_nvme_ns* ns = ns_ent->ns;
    struct spdk_nvme_qpair* qpair = ns_ent->qpair_comp;
    if (!compaction_thd) {
      ns_ent->qpair_mtx.Lock();
      qpair = ns_ent->qpair;
    }
    read_to_buf(ns, qpair, buf_, g_sect_per_obj * idx,
                ROUND_UP(size_, g_sectsize) / g_sectsize, nullptr);
    if (!compaction_thd) {
      ns_ent->qpair_mtx.Unlock();
    }
  }
  ~RawSequentialFile() override {
    spdk_free(buf_);
  }

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    n = std::min(n, size_ - offset_);
    // memcpy(scratch, buf_ + offset_, n);
    // *result = Slice(scratch, n);
    *result = Slice(buf_ + offset_, n);
    offset_ += n;
    return status;
  }

  Status Skip(uint64_t n) override {
    offset_ += n;
    if (offset_ > OBJ_SIZE)
      return PosixError(filename_, errno);
    return Status::OK();
  }

 private:
  const std::string filename_;
  char* buf_;
  uint32_t size_;
  uint64_t offset_;
  int idx_;
};

class RawRandomAccessFile final : public RandomAccessFile {
 public:
  RawRandomAccessFile(std::string filename, char* file_buf, int idx)
      : filename_(std::move(filename)), buf_(file_buf), idx_(idx),
        size_(g_sb_ptr->sb_meta[idx].f_size) {
  }

  ~RawRandomAccessFile() override {
    spdk_free(buf_);
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) override {
    Status status;
    if (offset + n > size_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }
    *result = Slice(buf_ + offset, n);
    return status;
  }

 private:
  const std::string filename_;
  char* buf_;
  uint32_t size_;
  int idx_;
};

class RawPartialRandomAccessFile final : public RandomAccessFile {
 public:
  RawPartialRandomAccessFile(std::string filename, char* file_buf, int idx)
      : filename_(std::move(filename)), buf_(file_buf), idx_(idx),
        size_(g_sb_ptr->sb_meta[idx].f_size), bmap_(), last_bend_(-2), seq_cnt_(0),
        prefetch_idx_(-1) {
    v_compl_.reserve(OBJ_SIZE / READ_UNIT + 10); // to prevent memory address change
  }

  ~RawPartialRandomAccessFile() override {
    spdk_free(buf_);
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) override {
    Status status;
    if (offset + n > size_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    struct ns_entry* ns_ent = g_namespaces;
    struct spdk_nvme_ns* ns = ns_ent->ns;
    struct spdk_nvme_qpair* qpair = ns_ent->qpair_comp;
    if (!compaction_thd) {
      ns_ent->qpair_mtx.Lock();
      qpair = ns_ent->qpair;
    }

    int bstart = offset / READ_UNIT;
    int bend = (offset + n - 1) / READ_UNIT;
    for (int i = bstart; i <= bend; i++) {
      assert(i < OBJ_SIZE / READ_UNIT);
      int bmap_idx = i / 8;
      int bmap_shift = i % 8;
      assert(bmap_idx < OBJ_SIZE / READ_UNIT);
      if ((bmap_[bmap_idx] & (1 << (bmap_shift))) == 0) {
        char* target_buf = buf_ + i * READ_UNIT;
        uint64_t lba = g_sect_per_obj * idx_ + i * (READ_UNIT / g_sectsize);
        uint32_t cnt = READ_UNIT / g_sectsize;
        v_compl_.push_back(0);
        read_to_buf(ns, qpair, target_buf, lba, cnt, &v_compl_.back());
        bmap_[bmap_idx] |= (1 << (bmap_shift));
      }
    }

    bool need_chk = false;
    if (prefetch_idx_ >= 0) {
      if (prefetch_idx_ >= bstart && prefetch_idx_ <= bend) {
        assert(v_compl_.size() > 0);
        need_chk = true;
      } else if (v_compl_.size() > 1) {
        need_chk = true;
      }
    } else {
      if (v_compl_.size() > 0) {
        need_chk = true;
      }
    }
    if (need_chk) {
      for (int i = 0; i < v_compl_.size(); i++) {
        int *c = &v_compl_[i];
        while (*c == 0)
          check_completion(qpair);
      }
      v_compl_.clear();
      prefetch_idx_ = -1;
    }

    if (bend == last_bend_) {
      // do nothing
    } else if (bend == last_bend_ + 1) {
      last_bend_ = bend;
      seq_cnt_++;
    } else {
      last_bend_ = bend;
      seq_cnt_ = 0;
    }

    if (seq_cnt_ >= 4) { // prefetch threshold
      int idx = bend + 1;
      int bmap_idx = idx / 8;
      int bmap_shift = idx % 8;
      if (prefetch_idx_ < 0 && idx <= (size_ - 1) / READ_UNIT &&
          (bmap_[bmap_idx] & (1 << (bmap_shift))) == 0) {
        char* target_buf = buf_ + idx * READ_UNIT;
        uint64_t lba = g_sect_per_obj * idx_ + idx * (READ_UNIT / g_sectsize);
        uint32_t cnt = READ_UNIT / g_sectsize;
        v_compl_.push_back(0);
        read_to_buf(ns, qpair, target_buf, lba, cnt, &v_compl_.back());
        bmap_[bmap_idx] |= (1 << (bmap_shift));
        prefetch_idx_ = idx;
      }
    }

    if (!compaction_thd) {
      ns_ent->qpair_mtx.Unlock();
    }

    *result = Slice(buf_ + offset, n);

    return status;
  }

 private:
  const std::string filename_;
  char* buf_;
  uint32_t size_;
  int idx_;

  int last_bend_;
  int seq_cnt_;
  int prefetch_idx_; // pending prefetch if non negative
  std::vector<int> v_compl_;

  uint8_t bmap_[DIV_ROUND_UP(OBJ_SIZE / READ_UNIT, 8)];
};

class RawWritableFile final : public WritableFile {
 public:
  RawWritableFile(std::string filename, char* file_buf, int idx, bool truncate)
      : filename_(filename), buf_(file_buf), idx_(idx), closed_(false),
        size_(g_sb_ptr->sb_meta[idx].f_size), synced_(size_) {
    if (truncate) {
      size_ = 0;
      synced_ = 0;
      return;
    }
    struct ns_entry* ns_ent = g_namespaces;
    struct spdk_nvme_ns* ns = ns_ent->ns;
    struct spdk_nvme_qpair* qpair = ns_ent->qpair_comp;
    if (!compaction_thd) {
      ns_ent->qpair_mtx.Lock();
      qpair = ns_ent->qpair;
    }
    read_to_buf(ns, qpair, buf_, g_sect_per_obj * idx,
                ROUND_UP(size_, g_sectsize) / g_sectsize, nullptr);
    if (!compaction_thd) {
      ns_ent->qpair_mtx.Unlock();
    }
  }

  ~RawWritableFile() override {
    if (!closed_)
      Close();
    if (filename_.rfind("ldb") != std::string::npos) {
      g_fs_mtx.Lock();
      if (g_last_write_buf != nullptr) {
        spdk_free(g_last_write_buf);
      }
      g_last_write_buf = buf_;
      g_last_write_idx = idx_;
      g_fs_mtx.Unlock();
    } else {
      spdk_free(buf_);
    }
  }

  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    assert(size_ + write_size <= OBJ_SIZE);
    memcpy(buf_ + size_, write_data, write_size);

    size_ += write_size;

    return Status::OK();
  }

  Status Close() override {
    FileMeta* meta = &g_sb_ptr->sb_meta[idx_];
    meta->f_size = size_;
    msync(meta, META_SIZE, MS_SYNC);

    Sync();

    closed_ = true;
    return Status::OK();
  }

  Status Flush() override {
    return Status::OK();
  }

  Status Sync() override {
    if (synced_ == size_)
      return Status::OK();
    struct ns_entry* ns_ent = g_namespaces;
    struct spdk_nvme_ns* ns = ns_ent->ns;
    struct spdk_nvme_qpair* qpair = ns_ent->qpair_comp;
    if (!compaction_thd) {
      ns_ent->qpair_mtx.Lock();
      qpair = ns_ent->qpair;
    }
    char* target_buf = buf_ + ROUND_DOWN(synced_, g_sectsize);
    uint64_t lba = g_sect_per_obj * idx_ +
                   ROUND_DOWN(synced_, g_sectsize) / g_sectsize;
    uint32_t cnt = ROUND_UP(size_ - synced_, g_sectsize) / g_sectsize;

    write_from_buf(ns, qpair, target_buf, lba, cnt, nullptr);
    if (!compaction_thd) {
      ns_ent->qpair_mtx.Unlock();
    }
    synced_ = size_;
    return Status::OK();
  }

 private:
  const std::string filename_;
  char* buf_;
  uint32_t size_;
  uint32_t synced_;
  int idx_;
  bool closed_;
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
    dprint("NewSequentialFile %s\n", filename.c_str());

    std::string basename = Basename(filename).ToString();

    g_fs_mtx.Lock();
    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(filename, ENOENT);
    }
    int idx = g_file_table[basename];
    g_fs_mtx.Unlock();

    char* fbuf = static_cast<char*>(
                 spdk_malloc(OBJ_SIZE, BUF_ALIGN, static_cast<uint64_t*>(NULL),
                             SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA));
    if (fbuf == NULL) {
      fprintf(stderr, "NewSequentialFile malloc failed\n");
      exit(1);
    }
    *result = new RawSequentialFile(basename, fbuf, idx);

    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    dprint("NewRandomAccessFile %s\n", filename.c_str());

    std::string basename = Basename(filename).ToString();

    char* fbuf = nullptr;

    g_fs_mtx.Lock();
    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(filename, ENOENT);
    }
    int idx = g_file_table[basename];

    if (g_last_write_idx == idx) {
      fbuf = g_last_write_buf;
      g_last_write_buf = nullptr;
      g_last_write_idx = -1;
    }

    g_fs_mtx.Unlock();

    if (fbuf != nullptr) {
      *result = new RawRandomAccessFile(basename, fbuf, idx);
    } else {
      char* fbuf = static_cast<char*>(spdk_malloc(OBJ_SIZE, BUF_ALIGN,
                                      static_cast<uint64_t*>(NULL),
                                      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA));
      if (fbuf == NULL) {
        fprintf(stderr, "NewRandomAccessFile malloc failed\n");
        exit(1);
      }
      *result = new RawPartialRandomAccessFile(basename, fbuf, idx);
    }

    return Status::OK();
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    dprint("NewWritableFile %s\n", filename.c_str());

    std::string basename = Basename(filename).ToString();

    g_fs_mtx.Lock();
    int idx;
    if (!g_file_table.count(basename)) {
      if (g_free_idx.empty()) {
        fprintf(stderr, "out of blocks\n");
        exit(1);
      }
      idx = g_free_idx.front();
      g_free_idx.pop();
      g_file_table.insert({basename, idx});

      FileMeta* meta = &g_sb_ptr->sb_meta[idx];
      strcpy(meta->f_name, basename.c_str());
      meta->f_name_len = basename.size();
      meta->f_size = 0;
      meta->f_reserved = 0;
    } else {
      idx = g_file_table[basename];
    }
    g_fs_mtx.Unlock();

    char* fbuf = static_cast<char*>(
                 spdk_malloc(OBJ_SIZE, BUF_ALIGN, static_cast<uint64_t*>(NULL),
                             SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA));
    if (fbuf == NULL) {
      fprintf(stderr, "NewWritableFile malloc failed\n");
      exit(1);
    }
    *result = new RawWritableFile(basename, fbuf, idx, true);

    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    dprint("NewAppendableFile %s\n", filename.c_str());

    std::string basename = Basename(filename).ToString();

    g_fs_mtx.Lock();
    int idx;
    if (!g_file_table.count(basename)) {
      if (g_free_idx.empty()) {
        fprintf(stderr, "out of blocks\n");
        exit(1);
      }
      idx = g_free_idx.front();
      g_free_idx.pop();
      g_file_table.insert({basename, idx});

      FileMeta* meta = &g_sb_ptr->sb_meta[idx];
      strcpy(meta->f_name, basename.c_str());
      meta->f_name_len = basename.size();
      meta->f_size = 0;
      meta->f_reserved = 0;
    } else {
      idx = g_file_table[basename];
    }
    g_fs_mtx.Unlock();

    char* fbuf = static_cast<char*>(
                 spdk_malloc(OBJ_SIZE, BUF_ALIGN, static_cast<uint64_t*>(NULL),
                             SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA));
    if (fbuf == NULL) {
      fprintf(stderr, "NewAppendableFile malloc failed\n");
      exit(1);
    }
    *result = new RawWritableFile(basename, fbuf, idx, false);

    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    std::string basename = Basename(filename).ToString();

    bool ret;
    g_fs_mtx.Lock();
    ret = g_file_table.count(basename);
    g_fs_mtx.Unlock();

    return ret;
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    result->clear();

    g_fs_mtx.Lock();
    for (auto &it : g_file_table)
      result->emplace_back(it.first);
    g_fs_mtx.Unlock();

    return Status::OK();
  }

  Status DeleteFile(const std::string& filename) override {
    dprint("DeleteFile %s\n", filename.c_str());

    std::string basename = Basename(filename).ToString();

    g_fs_mtx.Lock();

    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(filename, ENOENT);
    }

    int idx = g_file_table[basename];
    FileMeta* meta = &g_sb_ptr->sb_meta[idx];

    meta->f_size = 0;
    meta->f_name_len = 0;
    meta->f_reserved = 0;
    meta->f_name[0] = '\0';

    msync(meta, META_SIZE, MS_SYNC);

    if (g_last_write_idx == idx) {
      spdk_free(g_last_write_buf);
      g_last_write_idx = -1;
      g_last_write_buf = nullptr;
    }

    g_free_idx.push(idx);
    g_file_table.erase(basename);

    g_fs_mtx.Unlock();

    return Status::OK();
  }

  // initialize internal filesystem here
  Status CreateDir(const std::string& dirname) override {
    if (g_dbname == "") {
      g_dbname = dirname;
      int sb_fd = open((g_dbname + ".sb").c_str(), O_RDWR | O_CREAT, 0644);
      if (sb_fd == -1) {
        perror("open sb");
        exit(1);
      }
      if (ftruncate(sb_fd, sizeof(SuperBlock)) == -1) {
        perror("ftruncate");
        exit(1);
      }
      g_sbbuf = mmap(nullptr, sizeof(SuperBlock), PROT_READ | PROT_WRITE, MAP_SHARED, sb_fd, 0);
      if (g_sbbuf == MAP_FAILED) {
        perror("mmap");
        exit(1);
      }
      close(sb_fd);

      g_fs_mtx.Lock();
      g_sb_ptr = reinterpret_cast<SuperBlock*>(g_sbbuf);
      FileMeta* sb_meta = &g_sb_ptr->sb_meta[0];
      if (sb_meta->sb_magic == LDBFS_MAGIC) {
        dprint("ldbfs found\n");
        for (int i = 1; i < OBJ_CNT; i++) {
          FileMeta* meta_ent = &g_sb_ptr->sb_meta[i];
          if (meta_ent->f_name_len == 0) {
            g_free_idx.push(i);
          } else {
            g_file_table.insert({meta_ent->f_name, i});
          }
        }
      } else {
        memset(g_sbbuf, 0, sizeof(SuperBlock));
        sb_meta->sb_magic = LDBFS_MAGIC;

        for (int i = 1; i < OBJ_CNT; i++) {
          g_free_idx.push(i);
        }
      }
      g_fs_mtx.Unlock();
    }

    return Status::OK();
  }

  Status DeleteDir(const std::string& dirname) override {
    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    dprint("GetFileSize %s\n", filename.c_str());

    std::string basename = Basename(filename).ToString();

    g_fs_mtx.Lock();
    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(filename, ENOENT);
    }

    int idx = g_file_table[basename];

    FileMeta* meta = &g_sb_ptr->sb_meta[idx];

    *size = meta->f_size;

    g_fs_mtx.Unlock();

    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    dprint("RenameFile %s %s\n", from.c_str(), to.c_str());

    std::string basename_from = Basename(from).ToString();
    std::string basename_to = Basename(to).ToString();

    g_fs_mtx.Lock();

    if (!g_file_table.count(basename_from)) {
      g_fs_mtx.Unlock();
      return PosixError(from, ENOENT);
    }

    g_fs_mtx.Unlock();
    DeleteFile(to); // ignore error
    g_fs_mtx.Lock();

    int idx = g_file_table[basename_from];
    FileMeta* meta = &g_sb_ptr->sb_meta[idx];

    meta->f_name_len = basename_to.size();
    strcpy(meta->f_name, basename_to.c_str());

    msync(meta, META_SIZE, MS_SYNC);

    g_file_table[basename_to] = g_file_table[basename_from];
    g_file_table.erase(basename_from);

    g_fs_mtx.Unlock();

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
    std::FILE* fp = std::fopen(filename.c_str(), "w");
    if (fp == nullptr) {
      *result = nullptr;
      return PosixError(filename, errno);
    } else {
      *result = new PosixLogger(fp);
      return Status::OK();
    }
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
};

}  // namespace

PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false) {
  g_ns_mtx.Lock();
  init_spdk();
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
  compaction_thd = true;
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

