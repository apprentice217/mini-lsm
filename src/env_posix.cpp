#include "env.h"
#include "table.h"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>

namespace minidb {

// ---------------------------------------------------------------------------
// PosixWritableFile：顺序写文件，用于 WAL 和 SSTable 的落盘。
// ---------------------------------------------------------------------------
class PosixWritableFile : public WritableFile {
public:
    PosixWritableFile(const std::string& filename, int fd)
        : filename_(filename), fd_(fd) {}

    ~PosixWritableFile() override {
        if (fd_ >= 0) {
            // 析构时关闭文件，忽略返回值（析构函数不应抛出异常）。
            (void)Close();
        }
    }

    Status Append(const Slice& data) override {
        const char* src  = data.data();
        size_t      left = data.size();
        while (left > 0) {
            ssize_t done = write(fd_, src, left);
            if (done < 0) {
                if (errno == EINTR) continue;
                return Status::IOError(filename_, strerror(errno));
            }
            src  += done;
            left -= static_cast<size_t>(done);
        }
        return Status::OK();
    }

    // Flush 当前实现直接写入内核 Page Cache，无用户态缓冲，故为空操作。
    Status Flush() override { return Status::OK(); }

    // Sync 将内核脏页强制刷入磁盘，用于 WAL sync 模式和 SSTable 落盘后的持久化确认。
    Status Sync() override {
        if (fsync(fd_) < 0) {
            return Status::IOError(filename_, strerror(errno));
        }
        return Status::OK();
    }

    Status Close() override {
        Status s;
        if (close(fd_) < 0) {
            s = Status::IOError(filename_, strerror(errno));
        }
        fd_ = -1;
        return s;
    }

private:
    std::string filename_;
    int         fd_;
};

// ---------------------------------------------------------------------------
// PosixRandomAccessFile：随机读文件，用于 SSTable 的点查。
// 使用 pread(2) 实现线程安全的偏移读，不修改文件描述符的游标。
// ---------------------------------------------------------------------------
class PosixRandomAccessFile : public RandomAccessFile {
public:
    PosixRandomAccessFile(const std::string& filename, int fd)
        : filename_(filename), fd_(fd) {}

    ~PosixRandomAccessFile() override { close(fd_); }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
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
    std::string filename_;
    int         fd_;
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
    *result = new PosixRandomAccessFile(filename, fd);
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
