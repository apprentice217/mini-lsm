// include/iterator.h
#pragma once

#include "slice.h"
#include "status.h"

namespace minidb {

// 统一的迭代器接口：无论是查内存还是查磁盘，都用这套抽象方法
class Iterator {
public:
    Iterator() = default;
    virtual ~Iterator() = default;

    // 禁用拷贝
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    // 核心接口
    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice& target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    virtual Status status() const = 0;
};

} // namespace minidb