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
#include "util/posix_logger.h"

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
#define OBJ_SIZE (4ULL * 1024 * 1024)
#define META_SIZE (128)
#define OBJ_CNT (256)
#define FS_SIZE (OBJ_SIZE * OBJ_CNT)
#define MAX_NAMELEN (META_SIZE - 8)

#define PAGESIZE (4096)

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
static_assert(sizeof(FileMeta) == META_SIZE, "FileMeta size");

struct SuperBlock {
  FileMeta sb_meta[OBJ_CNT];
};

int g_dev_fd;
uint64_t g_dev_size;
void* g_mmap_base = nullptr;
std::string g_dbname = "";

SuperBlock* g_sb_ptr;                    // guarded by g_fs_mtx
std::map<std::string, int> g_file_table; // guarded by g_fs_mtx
std::queue<int> g_free_idx;              // guarded by g_fs_mtx

port::Mutex g_fs_mtx;

namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit.
int g_mmap_limit = kDefaultMmapLimit;

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

class RawSequentialFile final : public SequentialFile {
 public:
  RawSequentialFile(std::string filename, char* file_buf, int size)
      : filename_(filename), buf_(file_buf), offset_(0), size_(size) {
  }
  ~RawSequentialFile() override {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    n = std::min(n, (size_t)(size_ - offset_));
    *result = Slice(buf_ + offset_, n);
    offset_ += n;
    return Status::OK();
  }

  Status Skip(uint64_t n) override {
    offset_ += n;
    if (offset_ > size_)
      return PosixError(filename_, errno);
    return Status::OK();
  }

 private:
  const std::string filename_;
  char* buf_;
  uint32_t size_;
  off_t offset_;
};

class RawRandomAccessFile final : public RandomAccessFile {
 public:
  RawRandomAccessFile(std::string filename, char* file_buf, int size)
      : filename_(std::move(filename)), buf_(file_buf), size_(size) {
  }

  ~RawRandomAccessFile() override {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset + n > size_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }
    *result = Slice(buf_ + offset, n);
    return Status::OK();
  }

 private:
  const std::string filename_;
  char* buf_;
  uint32_t size_;
};

class RawWritableFile final : public WritableFile {
 public:
  RawWritableFile(std::string filename, char* file_buf, FileMeta* meta)
      : filename_(std::move(filename)), buf_(file_buf), meta_(meta) {
    size_ = meta->f_size;
  }

  ~RawWritableFile() override {
    meta_->f_size = size_;
  }

  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    if(size_ + write_size > OBJ_SIZE) {
      fprintf(stderr, "write exceed 4M\n");
      exit(1);
    }
    memcpy(buf_ + size_, write_data, write_size);

    size_ += write_size;

    return Status::OK();
  }

  Status Close() override {
    return Status::OK();
  }

  Status Flush() override {
    return Status::OK();
  }

  Status Sync() override {
    // TODO: sync dirty pages only
    int rc = msync((void*)ROUND_DOWN((unsigned long)buf_, PAGESIZE),
                   ROUND_UP(size_, PAGESIZE), MS_SYNC);
    if (rc == -1) {
      perror("msync data");
      return PosixError(filename_, errno);
    }
    meta_->f_size = size_;
    rc = msync((void*)ROUND_DOWN((unsigned long)meta_, PAGESIZE), PAGESIZE, MS_SYNC);
    if (rc == -1) {
      perror("msync meta");
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const std::string filename_;
  char* buf_;
  FileMeta* meta_;
  uint32_t size_;
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
    std::string basename = Basename(filename).ToString();
    g_fs_mtx.Lock();
    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(basename, ENOENT);
    }
    int idx = g_file_table[basename];
    g_fs_mtx.Unlock();

    char* fbuf = GetFileBuf(idx);
    FileMeta* meta = GetFileMeta(idx);
    *result = new RawSequentialFile(basename, fbuf, meta->f_size);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    std::string basename = Basename(filename).ToString();
    g_fs_mtx.Lock();
    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(basename, ENOENT);
    }
    int idx = g_file_table[basename];
    g_fs_mtx.Unlock();

    char* fbuf = GetFileBuf(idx);
    FileMeta* meta = GetFileMeta(idx);
    *result = new RawRandomAccessFile(basename, fbuf, meta->f_size);
    return Status::OK();
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    std::string basename = Basename(filename).ToString();
    int idx = -1;
    char* fbuf = nullptr;
    FileMeta* meta = nullptr;

    g_fs_mtx.Lock();
    if (g_file_table.count(basename)) {
      idx = g_file_table[basename];
      fbuf = GetFileBuf(idx);
      meta = GetFileMeta(idx);
      meta->f_size = 0; // truncate
    } else {
      if (g_free_idx.empty()) {
        fprintf(stderr, "out of space\n");
        g_fs_mtx.Unlock();
        return PosixError(basename, ENOSPC);
      }
      idx = g_free_idx.front();
      g_free_idx.pop();
      g_file_table.insert({basename, idx});
      fbuf = GetFileBuf(idx);
      meta = GetFileMeta(idx);
      strcpy(meta->f_name, basename.c_str());
      meta->f_name_len = basename.size();
      meta->f_size = 0;
    }
    g_fs_mtx.Unlock();
    *result = new RawWritableFile(basename, fbuf, meta);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    std::string basename = Basename(filename).ToString();
    int idx = -1;
    char* fbuf = nullptr;
    FileMeta* meta = nullptr;
    g_fs_mtx.Lock();
    if (g_file_table.count(basename)) {
      idx = g_file_table[basename];
      fbuf = GetFileBuf(idx);
      meta = GetFileMeta(idx);
    } else {
      if (g_free_idx.empty()) {
        fprintf(stderr, "out of space\n");
        g_fs_mtx.Unlock();
        return PosixError(basename, ENOSPC);
      }
      idx = g_free_idx.front();
      g_free_idx.pop();
      g_file_table.insert({basename, idx});
      fbuf = GetFileBuf(idx);
      meta = GetFileMeta(idx);
      strcpy(meta->f_name, basename.c_str());
      meta->f_name_len = basename.size();
      meta->f_size = 0;
    }
    g_fs_mtx.Unlock();
    *result = new RawWritableFile(basename, fbuf, meta);
    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    std::string basename = Basename(filename).ToString();
    g_fs_mtx.Lock();
    bool ret = g_file_table.count(basename);
    g_fs_mtx.Unlock();
    return ret;
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    if (g_dbname == "") {
      OpenLDBRaw(directory_path);
    }
    result->clear();

    g_fs_mtx.Lock();
    for (auto &it : g_file_table)
      result->emplace_back(it.first);
    g_fs_mtx.Unlock();

    return Status::OK();
  }

  Status DeleteFile(const std::string& filename) override {
    std::string basename = Basename(filename).ToString();
    g_fs_mtx.Lock();
    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(basename, ENOENT);
    }
    int idx = g_file_table[basename];
    FileMeta* meta = GetFileMeta(idx);
    meta->f_name_len = 0;
    meta->f_name[0] = '\0';
    meta->f_size = 0;
    g_file_table.erase(basename);
    g_free_idx.push(idx);
    g_fs_mtx.Unlock();

    return Status::OK();
  }

  Status CreateDir(const std::string& dirname) override {
    if (g_dbname != "") {
      return Status::IOError("db already opened\n");
    }
    OpenLDBRaw(dirname);
    return Status::OK();
  }

  Status DeleteDir(const std::string& dirname) override {
    FileMeta* super_meta = GetFileMeta(0);
    super_meta->sb_magic = 0;
    for (int i = 1; i < OBJ_CNT; i++) {
      FileMeta* meta = GetFileMeta(i);
      meta->f_name_len = 0;
      meta->f_name[0] = '\0';
      meta->f_size = 0;
    }
    g_file_table.clear();
    while (!g_free_idx.empty()) {
      g_free_idx.pop();
    }
    for (int i = 1; i < OBJ_CNT; i++) {
      g_free_idx.push(i);
    }
    msync(super_meta, OBJ_SIZE, MS_SYNC);
    g_dbname = "";
    munmap(g_mmap_base, g_dev_size);
    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    std::string basename = Basename(filename).ToString();
    g_fs_mtx.Lock();
    if (!g_file_table.count(basename)) {
      g_fs_mtx.Unlock();
      return PosixError(basename, ENOENT);
    }
    int idx = g_file_table[basename];
    FileMeta* meta = GetFileMeta(idx);
    *size = meta->f_size;
    g_fs_mtx.Unlock();
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    std::string basename_from = Basename(from).ToString();
    std::string basename_to = Basename(to).ToString();
    g_fs_mtx.Lock();
    if (!g_file_table.count(basename_from)) {
      g_fs_mtx.Unlock();
      return PosixError(basename_from, ENOENT);
    }
    int idx = g_file_table[basename_from];
    FileMeta* meta = GetFileMeta(idx);

    g_fs_mtx.Unlock();
    DeleteFile(to); // may not exists, ignore error
    g_fs_mtx.Lock();

    g_file_table.erase(basename_from);
    g_file_table[basename_to] = idx;

    strcpy(meta->f_name, basename_to.c_str());
    meta->f_name_len = basename_to.size();
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
    return Status::NotSupported(__FUNCTION__);
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

  FileMeta* GetFileMeta(int idx) {
    return &g_sb_ptr->sb_meta[idx];
  }

  char* GetFileBuf(int idx) {
    return static_cast<char*>(g_mmap_base) + OBJ_SIZE * idx;
  }

  void OpenLDBRaw(const std::string& dbname) {
    bool real_dev = false;
    g_dbname = dbname;
    g_dev_size = FS_SIZE;
    g_dev_fd = open(dbname.c_str(), O_RDWR | O_CREAT, 0644);
    if (g_dev_fd == -1) {
      perror("open ldb");
      exit(1);
    }
    struct stat sts;
    if (fstat(g_dev_fd, &sts) == -1) {
      perror("fstat");
      exit(1);
    }
    if ((sts.st_mode & S_IFMT) == S_IFREG) {
      if (ftruncate(g_dev_fd, g_dev_size) == -1) {
        perror("ftruncate");
        exit(1);
      }
    } else if ((sts.st_mode & S_IFMT) == S_IFBLK) {
      int nblk;
      int sectsize;
      ioctl(g_dev_fd, BLKGETSIZE, &nblk);
      ioctl(g_dev_fd, BLKSSZGET, &sectsize);
      real_dev = true;
      if (1LL * sectsize * nblk < FS_SIZE) {
        fprintf(stderr, "device too small\n");
        exit(1);
      }
    } else {
      fprintf(stderr, "unsupported file type\n");
      exit(1);
    }
    g_mmap_base = mmap(nullptr, g_dev_size, PROT_READ | PROT_WRITE, MAP_SHARED, g_dev_fd, 0);
    if (g_mmap_base == MAP_FAILED) {
      perror("mmap");
      exit(1);
    }
    g_fs_mtx.Lock();
    g_sb_ptr = static_cast<SuperBlock*>(g_mmap_base);
    FileMeta* super_meta = GetFileMeta(0);
    if (super_meta->sb_magic == LDBFS_MAGIC) {
      for (int i = 1; i < OBJ_CNT; i++) {
        FileMeta* meta = GetFileMeta(i);
        if (meta->f_name_len > 0) {
          g_file_table.insert({meta->f_name, i});
        } else {
          g_free_idx.push(i);
        }
      }
    } else {
      super_meta->sb_magic = LDBFS_MAGIC;
      for (int i = 1; i < OBJ_CNT; i++) {
        FileMeta* meta = GetFileMeta(i);
        meta->f_name_len = 0;
        meta->f_size = 0;
      }
    }
    g_fs_mtx.Unlock();
  }

  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);
};

}  // namespace

PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false) {
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
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit = limit;
}

Env* Env::Default() {
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb

