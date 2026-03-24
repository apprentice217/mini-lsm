#include "env.h"
#include "table.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>
#include <string>
#include <algorithm>
#include <cstdlib>

namespace minidb {

// ---------------------------------------------------------------------------
// PosixWritableFile：顺序写文件，用于 WAL 和 SSTable 的落盘。
//
// 写缓冲策略：维护一个 64KB 的用户态缓冲区（buf_），Append 时优先填充缓冲，
// 缓冲满才调用 write()，将系统调用次数从 O(每条 KV) 降低到 O(每 64KB)。
// Flush/Sync/Close 前必须先将缓冲 FlushBuffer() 落入内核。
// ---------------------------------------------------------------------------
class PosixWritableFile : public WritableFile {
public:
    static constexpr size_t kWriteBufferSize = 65536; // 64 KB 用户态写缓冲

    PosixWritableFile(const std::string& filename, int fd)
        : filename_(filename), fd_(fd), buf_pos_(0) {}

    ~PosixWritableFile() override {
        if (fd_ >= 0) {
            (void)Close();
        }
    }

    Status Append(const Slice& data) override {
        const char* src  = data.data();
        size_t      left = data.size();

        // 优先填满缓冲区，超出时才 flush 并直接写剩余数据。
        while (left > 0) {
            size_t avail = kWriteBufferSize - buf_pos_;
            if (avail > 0) {
                size_t copy = std::min(left, avail);
                memcpy(buf_ + buf_pos_, src, copy);
                buf_pos_ += copy;
                src      += copy;
                left     -= copy;
            }
            if (buf_pos_ == kWriteBufferSize) {
                Status s = FlushBuffer();
                if (!s.ok()) return s;
            }
        }
        return Status::OK();
    }

    // Flush 将用户态缓冲写入内核 Page Cache（不 fsync）。
    Status Flush() override {
        return FlushBuffer();
    }

    // Sync 先 flush 用户态缓冲，再 fsync 落盘。
    Status Sync() override {
        Status s = FlushBuffer();
        if (!s.ok()) return s;
        if (fsync(fd_) < 0) {
            return Status::IOError(filename_, strerror(errno));
        }
        return Status::OK();
    }

    Status Close() override {
        Status s = FlushBuffer(); // 关闭前必须 flush
        if (close(fd_) < 0 && s.ok()) {
            s = Status::IOError(filename_, strerror(errno));
        }
        fd_ = -1;
        return s;
    }

private:
    // FlushBuffer 将 buf_[0..buf_pos_) 写入内核，清空缓冲。
    Status FlushBuffer() {
        if (buf_pos_ == 0) return Status::OK();
        const char* src  = buf_;
        size_t      left = buf_pos_;
        while (left > 0) {
            ssize_t done = write(fd_, src, left);
            if (done < 0) {
                if (errno == EINTR) continue;
                return Status::IOError(filename_, strerror(errno));
            }
            src  += done;
            left -= static_cast<size_t>(done);
        }
        buf_pos_ = 0;
        return Status::OK();
    }

    std::string filename_;
    int         fd_;
    char        buf_[kWriteBufferSize];
    size_t      buf_pos_;
};

// ---------------------------------------------------------------------------
// PosixRandomAccessFile：随机读文件，用于 SSTable 的点查。
//
// 实现策略：将整个文件 mmap 到进程地址空间（PROT_READ, MAP_SHARED）。
// Read() 直接返回指向 mmap 内存的 Slice，无需将数据从内核拷贝到用户提供的 scratch。
// 优势：
//   - 零拷贝：Data Block 的字节直接由 OS Page Cache 提供，不经过 pread 的额外 memcpy。
//   - 内核负责换入/换出（LRU Page Cache），多次读同一 Block 命中 Page Cache 无系统调用。
// 注意：scratch 参数不再使用（保留以兼容接口），调用方不应在 Read() 返回后释放 scratch
//       并期望 result 仍有效——result 指向 mmap 区域，生命周期与文件对象相同。
// ---------------------------------------------------------------------------
class PosixRandomAccessFile : public RandomAccessFile {
public:
    PosixRandomAccessFile(const std::string& filename, int fd, uint64_t file_size)
        : filename_(filename), fd_(fd), file_size_(file_size), base_(nullptr) {
        if (file_size_ > 0) {
            void* ptr = mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, fd_, 0);
            if (ptr != MAP_FAILED) {
                base_ = static_cast<const char*>(ptr);
                // 预读提示：告知内核此文件将被随机访问，不需要顺序预读。
                madvise(const_cast<char*>(base_), file_size_, MADV_RANDOM);
            }
        }
    }

    ~PosixRandomAccessFile() override {
        if (base_ != nullptr) {
            munmap(const_cast<char*>(base_), file_size_);
        }
        close(fd_);
    }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
        if (base_ != nullptr) {
            // mmap 路径：零拷贝，直接返回指向映射内存的 Slice。
            if (offset + n > file_size_) {
                return Status::Corruption("PosixRandomAccessFile: read out of bounds");
            }
            *result = Slice(base_ + offset, n);
            return Status::OK();
        }
        // fallback：mmap 失败时退化到 pread。
        ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
        *result   = Slice(scratch, r < 0 ? 0 : static_cast<size_t>(r));
        if (r < 0) {
            return Status::IOError(filename_, strerror(errno));
        }
        if (static_cast<size_t>(r) != n) {
            return Status::Corruption("PosixRandomAccessFile: short read");
        }
        return Status::OK();
    }

private:
    std::string    filename_;
    int            fd_;
    uint64_t       file_size_;
    const char*    base_; // mmap 起始地址，nullptr 表示 mmap 失败
};

// ---------------------------------------------------------------------------
// PosixSequentialFile：顺序读文件，用于 WAL 和 MANIFEST 的回放。
// ---------------------------------------------------------------------------
class PosixSequentialFile : public SequentialFile {
public:
    PosixSequentialFile(const std::string& fname, FILE* f)
        : filename_(fname), file_(f) {}

    ~PosixSequentialFile() override { std::fclose(file_); }

    Status Read(size_t n, Slice* result, char* scratch) override {
        size_t r = std::fread(scratch, 1, n, file_);
        *result  = Slice(scratch, r);
        if (r < n && !std::feof(file_)) {
            return Status::IOError(filename_, std::strerror(errno));
        }
        return Status::OK();
    }

    Status Skip(uint64_t n) override {
        if (std::fseek(file_, static_cast<long>(n), SEEK_CUR)) {
            return Status::IOError(filename_, std::strerror(errno));
        }
        return Status::OK();
    }

private:
    std::string filename_;
    FILE*       file_;
};

// ---------------------------------------------------------------------------
// 工厂函数
// ---------------------------------------------------------------------------

Status NewWritableFile(const std::string& filename, WritableFile** result) {
    *result  = nullptr;
    int fd   = open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        return Status::IOError(filename, strerror(errno));
    }
    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
}

Status NewRandomAccessFile(const std::string& filename, RandomAccessFile** result) {
    *result = nullptr;
    int fd  = open(filename.c_str(), O_RDONLY, 0);
    if (fd < 0) {
        return Status::IOError(filename, strerror(errno));
    }
    struct stat st;
    uint64_t file_size = 0;
    if (fstat(fd, &st) == 0) {
        file_size = static_cast<uint64_t>(st.st_size);
    }
    *result = new PosixRandomAccessFile(filename, fd, file_size);
    return Status::OK();
}

Status NewSequentialFile(const std::string& fname, SequentialFile** result) {
    FILE* f = std::fopen(fname.c_str(), "rb");
    if (f == nullptr) {
        *result = nullptr;
        return Status::IOError(fname, std::strerror(errno));
    }
    *result = new PosixSequentialFile(fname, f);
    return Status::OK();
}

} // namespace minidb
