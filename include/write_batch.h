#pragma once

#include <string>
#include <vector>
#include "slice.h"

namespace minidb {

// 内部强类型枚举，防止与全局 ValueType 冲突
enum class BatchValueType : unsigned char {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1
};

struct BatchRecord {
    BatchValueType type;
    std::string key;
    std::string value;
};

// WriteBatch：将多个零散的 KV 操作打包为一个原子事务
class WriteBatch {
public:
    WriteBatch() = default;
    ~WriteBatch() = default;

    // 遵循 C++ Core Guidelines 的 Rule of Five：禁用拷贝，显式声明移动语义
    WriteBatch(const WriteBatch&) = delete;
    WriteBatch& operator=(const WriteBatch&) = delete;
    WriteBatch(WriteBatch&&) = default;
    WriteBatch& operator=(WriteBatch&&) = default;

    void Put(const Slice& key, const Slice& value) {
        records_.push_back({BatchValueType::kTypeValue, key.ToString(), value.ToString()});
        byte_size_ += key.size() + value.size() + 8;
    }

    void Delete(const Slice& key) {
        records_.push_back({BatchValueType::kTypeDeletion, key.ToString(), ""});
        byte_size_ += key.size() + 8;
    }

    void Clear() {
        records_.clear();
        byte_size_ = 0;
    }

    size_t ApproximateSize() const { return byte_size_; }
    const std::vector<BatchRecord>& Records() const { return records_; }

private:
    std::vector<BatchRecord> records_;
    size_t byte_size_ = 0;
};

} // namespace minidb