#pragma once

#include <cstdint>
#include <string>
#include "slice.h"
#include "status.h"
#include "block.h" // 包含 Iterator 接口
#include "options.h"
#include "env.h"

namespace minidb {

// -------------------------------------------------------------------------
// SSTable 宏观解析器
// -------------------------------------------------------------------------
class Table {
public:
    // 静态工厂：解析 Footer，加载 Index Block，校验魔数
    static Status Open(const Options& options, RandomAccessFile* file, uint64_t file_size, Table** table);


    ~Table();

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    Status Get(const ReadOptions& options, const Slice& key, std::string* value);
    
    // 返回一个双层迭代器，能够遍历整个 SSTable 的所有数据
    Iterator* NewIterator(const ReadOptions& options) const;


private:
    struct Rep;
    Rep* rep_; // Pimpl 惯用法，隐藏内部极其复杂的 Block 缓存和索引状态

    explicit Table(Rep* rep) : rep_(rep) {}
};

} // namespace minidb