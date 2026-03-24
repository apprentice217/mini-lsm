#pragma once

#include <string>
#include "slice.h"
#include "status.h"
#include "options.h"

namespace minidb {

class WriteBatch;
class Iterator;
class Snapshot;

// 数据库抽象门面，对外隐藏所有实现细节。
class DB {
public:
    static Status Open(const Options& options, const std::string& name, DB** dbptr);

    DB() = default;
    virtual ~DB() = default;

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& value) = 0;
    virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;
    virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;
    virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value) = 0;

    // NewIterator 返回一个覆盖所有 Level（MemTable + SSTable）的合并迭代器。
    // 迭代时只能看到 ReadOptions::snapshot 指定时刻（或最新）的数据。
    // 调用方负责 delete 返回的迭代器。
    virtual Iterator* NewIterator(const ReadOptions& options) = 0;

    // GetSnapshot 创建一个当前时刻的只读快照，调用方负责通过 ReleaseSnapshot 释放。
    // 持有 Snapshot 期间，相应的数据版本不会被 compaction 回收。
    virtual const Snapshot* GetSnapshot() = 0;
    virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;
};

} // namespace minidb
