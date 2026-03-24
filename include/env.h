#pragma once

#include <string>
#include <cstdint>
#include "slice.h"
#include "status.h"

namespace minidb {

class SequentialFile {
public:
    virtual ~SequentialFile() = default;
    virtual Status Read(size_t n, Slice* result, char* scratch) = 0;
    virtual Status Skip(uint64_t n) = 0;
};

// =========================================================================
// 顺序写文件接口抽象
// =========================================================================
class WritableFile {
public:
    WritableFile() = default;
    virtual ~WritableFile() = default;

    WritableFile(const WritableFile&) = delete;
    WritableFile& operator=(const WritableFile&) = delete;

    virtual Status Append(const Slice& data) = 0;
    virtual Status Flush() = 0;
    virtual Status Sync() = 0;
    virtual Status Close() = 0;
};

// =========================================================================
// 随机读文件接口抽象
// =========================================================================
class RandomAccessFile {
public:
    RandomAccessFile() = default;
    virtual ~RandomAccessFile() = default;

    RandomAccessFile(const RandomAccessFile&) = delete;
    RandomAccessFile& operator=(const RandomAccessFile&) = delete;

    // 工业级随机读：线程安全，不改变文件内部游标
    // scratch 是由调用者分配的内存，用于存放读取到的底层物理字节
    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const = 0;
};

// =========================================================================
// [新增] 暴露给外界的工厂函数声明
// 其他模块（如 main.cpp 或 DBImpl）通过调用这两个函数来获取操作系统真实的文件句柄
// =========================================================================

// 创建一个用于追加写入的文件。如果文件已存在，会被直接清空。
Status NewWritableFile(const std::string& filename, WritableFile** result);

// 打开一个只读文件，用于支持并发的点查。
Status NewRandomAccessFile(const std::string& filename, RandomAccessFile** result);

// 【新增】：向操作系统申请一个顺序读文件句柄
Status NewSequentialFile(const std::string& fname, SequentialFile** result);


// 另外，确保也有这个写文件接口（LogAndApply 写 MANIFEST 需要）
Status NewWritableFile(const std::string& fname, class WritableFile** result);

} // namespace minidb