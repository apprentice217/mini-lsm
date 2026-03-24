#pragma once

#include <string>
#include "iterator.h"
#include "slice.h"
#include "arena.h"
#include "skiplist.h"

namespace minidb {

class Comparator;

// MemTableComparator 将 Arena 节点首地址（const char*）解码为 InternalKey Slice，
// 再委托给 InternalKeyComparator 完成比较（UserKey 升序 + 序列号降序）。
struct MemTableComparator {
    const Comparator* comparator;
    explicit MemTableComparator(const Comparator* c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
};

// MemTable 是 LSM-tree 的写缓冲区，内部用跳表维护有序的 InternalKey-Value 对。
// 所有节点内存由 Arena 统一管理，MemTable 析构时一次性释放，无单节点 free 开销。
// 写入由外部 mutex 保护（单写），读取通过跳表的无锁并发语义支持并发。
class MemTable {
public:
    explicit MemTable(const Comparator* comparator);
    ~MemTable() = default;

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    // Add 将一条带版本的 KV 操作编码后插入跳表。
    // type 为 kTypeValue（写入）或 kTypeDeletion（删除墓碑）。
    void Add(uint64_t seq, char type, const Slice& key, const Slice& value);

    // Get 在跳表中查找 UserKey 的最新版本。
    // 找到有效值时将其存入 value 并返回 true；
    // 找到删除墓碑时返回 true（通知上层无需再向磁盘查找）；
    // 未找到时返回 false。
    bool Get(const Slice& key, std::string* value);

    // NewIterator 返回按 InternalKey 顺序遍历跳表的迭代器，用于 flush 到 SSTable。
    Iterator* NewIterator();

    // ApproximateMemoryUsage 返回 Arena 当前占用的字节数，供上层判断是否触发 flush。
    size_t ApproximateMemoryUsage() const;

private:
    friend class MemTableIterator;
    typedef SkipList<const char*, MemTableComparator> Table;

    MemTableComparator comparator_;
    Arena              arena_;
    Table              table_;
};

} // namespace minidb
