#pragma once
#include "slice.h"
#include "comparator.h"
#include "coding.h"
#include <cassert>

namespace minidb {

// 每条 KV 记录在 MemTable 和 WAL 中的操作类型，占 InternalKey 末 8 字节的低位。
enum ValueType : uint8_t {
    kTypeDeletion = 0,
    kTypeValue    = 1
};

// ExtractUserKey 从 InternalKey 中剥离末尾 8 字节（序列号 + 操作类型），返回纯 UserKey 视图。
inline Slice ExtractUserKey(const Slice& internal_key) {
    assert(internal_key.size() >= 8);
    return Slice(internal_key.data(), internal_key.size() - 8);
}

// InternalKeyComparator 在 UserKey 比较器的基础上附加序列号降序语义：
//   - 相同 UserKey 下，序列号越大（越新）排越前；
//   - 保证 Seek(LookupKey) 时优先命中最新版本。
class InternalKeyComparator : public Comparator {
public:
    explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}

    const char* Name() const override { return "minidb.InternalKeyComparator"; }
    int Compare(const Slice& a, const Slice& b) const override;

    const Comparator* user_comparator() const { return user_comparator_; }

private:
    const Comparator* user_comparator_;
};

} // namespace minidb
