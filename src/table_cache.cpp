#include "table_cache.h"
#include "coding.h"
#include <cstdio>

namespace minidb {

static std::string TableFileName(const std::string& dbname, uint64_t number) {
    char buf[100];
    std::snprintf(buf, sizeof(buf), "%s/%06llu.sst",
                  dbname.c_str(), (unsigned long long)number);
    return std::string(buf);
}

TableCache::TableCache(const std::string& dbname, const Options& options, int capacity)
    : dbname_(dbname), options_(options), capacity_(capacity) {
}

// FindTable 先在 LRU 缓存中查找，命中则将节点提升至链表头部（最近使用）；
// 未命中则打开 SSTable 文件，解析 Footer 和 Index Block，插入缓存后返回。
// 若缓存已满，则驱逐链表尾部（最久未使用）的条目。
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             std::shared_ptr<Table>* table) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = hash_map_.find(file_number);
    if (it != hash_map_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        *table = it->second->table;
        return Status::OK();
    }

    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* raw_file = nullptr;
    Status s = NewRandomAccessFile(fname, &raw_file);
    if (!s.ok()) return s;

    Table* raw_table = nullptr;
    s = Table::Open(options_, raw_file, file_size, &raw_table);
    if (!s.ok()) {
        delete raw_file;
        return s;
    }

    CacheEntry new_entry;
    new_entry.file_number = file_number;
    new_entry.table.reset(raw_table);
    new_entry.file.reset(raw_file);

    if (static_cast<int>(lru_list_.size()) >= capacity_) {
        uint64_t old_file_num = lru_list_.back().file_number;
        hash_map_.erase(old_file_num);
        lru_list_.pop_back();
    }

    lru_list_.push_front(new_entry);
    hash_map_[file_number] = lru_list_.begin();

    *table = new_entry.table;
    return Status::OK();
}

// Get 直接委托给 Table::Get，后者内部完成：
//   Index Block 二分 -> Bloom Filter 拦截 -> Data Block 加载 -> key 精确匹配。
// 这样避免在 TableCache 层重复实现查找逻辑，并正确复用 Bloom Filter 过滤路径。
Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       std::string* value) {
    std::shared_ptr<Table> table;
    Status s = FindTable(file_number, file_size, &table);
    if (!s.ok()) return s;

    return table->Get(options, k, value);
}

// NewIterator 打开 SSTable 并返回 TwoLevelIterator，供 MergingIterator 组合使用。
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size) {
    std::shared_ptr<Table> table;
    Status s = FindTable(file_number, file_size, &table);
    if (!s.ok()) {
        // 返回一个始终无效的迭代器，避免调用方 nullptr 解引用。
        class ErrorIterator : public Iterator {
        public:
            explicit ErrorIterator(Status s) : s_(std::move(s)) {}
            bool Valid() const override { return false; }
            void SeekToFirst() override {}
            void SeekToLast() override {}
            void Seek(const Slice&) override {}
            void Next() override {}
            void Prev() override {}
            Slice key() const override { return Slice(); }
            Slice value() const override { return Slice(); }
            Status status() const override { return s_; }
        private:
            Status s_;
        };
        return new ErrorIterator(s);
    }
    return table->NewIterator(options);
}

} // namespace minidb
