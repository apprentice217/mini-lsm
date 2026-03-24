#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <list>
#include <unordered_map>
#include "table.h"
#include "options.h"
#include "env.h"

namespace minidb {

// TableCache 缓存已打开的 SSTable 对象（含 Index Block 和 Bloom Filter）。
// 使用 LRU 策略（链表 + 哈希表）管理 capacity 个条目，避免频繁重新打开文件。
class TableCache {
public:
    TableCache(const std::string& dbname, const Options& options, int capacity);
    ~TableCache() = default;

    TableCache(const TableCache&) = delete;
    TableCache& operator=(const TableCache&) = delete;

    // Get 在指定 SSTable 文件中查找 key（InternalKey 格式），
    // 内部委托给 Table::Get 完成 Index Block 二分 + Bloom Filter + Data Block 精确匹配。
    Status Get(const ReadOptions& options,
               uint64_t file_number,
               uint64_t file_size,
               const Slice& k,
               std::string* value);

    // NewIterator 返回指定 SSTable 的全量迭代器（TwoLevelIterator）。
    // 调用方负责 delete 返回的迭代器。
    Iterator* NewIterator(const ReadOptions& options,
                          uint64_t file_number,
                          uint64_t file_size);

private:
    struct CacheEntry {
        uint64_t                   file_number;
        std::shared_ptr<Table>     table;
        std::shared_ptr<RandomAccessFile> file;
    };

    Status FindTable(uint64_t file_number, uint64_t file_size,
                     std::shared_ptr<Table>* table);

    std::string dbname_;
    Options     options_;
    int         capacity_;

    std::mutex mutex_;
    std::list<CacheEntry>                                      lru_list_;
    std::unordered_map<uint64_t, decltype(lru_list_)::iterator> hash_map_;
};

} // namespace minidb
