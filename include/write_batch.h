#pragma once

#include <string>
#include <vector>
#include "slice.h"

namespace minidb {

// 使用强类型枚举，枚举值会被限制在BatchValueType的作用域内，避免命名冲突,更加安全
enum class BatchValueType : unsigned char {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1
};

// 一条批操作会记录：操作类型、stirng类型的key、string类型的value
struct BatchRecord {
    BatchValueType type;
    std::string key;
    std::string value;
};

// WriteBatch：按添加顺序保存一组KV写入和删除操作，供数据库统一提交处理。
class WriteBatch {
public:
    WriteBatch() = default;
    ~WriteBatch() = default;

    // 禁止拷贝构造和拷贝赋值，允许移动构造和移动赋值
    // 这里移动不代表移动后的源对象会自动变成空容器且源对象的byte_size_不会自动变0
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
    size_t byte_size_ = 0; // 记录批次的估计大小
};

} // namespace minidb