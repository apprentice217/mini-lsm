#include "memtable.h"
#include "db_format.h"
#include "coding.h"
#include "comparator.h"
#include <cassert>

namespace minidb {

// 从 Arena 分配的节点首地址读取长度前缀，并返回其后紧跟的数据 Slice。
// 节点内存布局：[varint32: internal_key_size][internal_key][varint32: val_size][val]
static const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
    bool result = GetVarint32(&p, limit, value);
    assert(result);
    return p;
}

static Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len;
    const char* p = GetVarint32Ptr(data, data + 5, &len);
    return Slice(p, len);
}

// MemTableComparator 委托给 InternalKeyComparator。
// InternalKeyComparator 的语义：UserKey 升序，序列号降序（新版本排在前面）。
int MemTableComparator::operator()(const char* a, const char* b) const {
    Slice a_slice = GetLengthPrefixedSlice(a);
    Slice b_slice = GetLengthPrefixedSlice(b);
    return comparator->Compare(a_slice, b_slice);
}

// ---------------------------------------------------------------------------
// MemTableIterator：将 SkipList 迭代器适配为通用 Iterator 接口。
// ---------------------------------------------------------------------------
class MemTableIterator : public Iterator {
public:
    explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

    bool Valid()       const override { return iter_.Valid(); }
    void Seek(const Slice& k) override { iter_.Seek(k.data()); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast()  override { iter_.SeekToLast(); }
    void Next()        override { iter_.Next(); }
    void Prev()        override { iter_.Prev(); }
    Status status() const override { return Status::OK(); }

    // key() 返回包含序列号的 InternalKey Slice（零拷贝，指向 Arena 内存）。
    Slice key() const override {
        return GetLengthPrefixedSlice(iter_.key());
    }

    // value() 紧跟在 InternalKey 之后，同样通过长度前缀定位（零拷贝）。
    Slice value() const override {
        Slice key_slice = GetLengthPrefixedSlice(iter_.key());
        const char* value_ptr = key_slice.data() + key_slice.size();
        return GetLengthPrefixedSlice(value_ptr);
    }

private:
    MemTable::Table::Iterator iter_;
};

// ---------------------------------------------------------------------------
// MemTable 实现
// ---------------------------------------------------------------------------

MemTable::MemTable(const Comparator* comparator)
    : comparator_(comparator),
      arena_(),
      table_(comparator_, &arena_) {
}

size_t MemTable::ApproximateMemoryUsage() const {
    return arena_.MemoryUsage();
}

// Add 将一条操作编码后插入跳表。
// 节点格式：[varint32 internal_key_size][UserKey][seq<<8|type: 8B][varint32 val_size][Value]
// InternalKey 包含 UserKey 和 8 字节的 (seq, type) 尾缀，是 SkipList 排序的基准。
void MemTable::Add(uint64_t seq, char type, const Slice& key, const Slice& value) {
    size_t key_size          = key.size();
    size_t val_size          = value.size();
    size_t internal_key_size = key_size + 8;

    const size_t encoded_len =
        VarintLength(internal_key_size) + internal_key_size +
        VarintLength(val_size) + val_size;

    char* buf = arena_.AllocateAligned(encoded_len);
    char* p   = buf;

    // 写入 InternalKey 长度前缀
    std::string tmp;
    PutVarint32(&tmp, static_cast<uint32_t>(internal_key_size));
    memcpy(p, tmp.data(), tmp.size());
    p += tmp.size();

    // 写入 UserKey
    memcpy(p, key.data(), key_size);
    p += key_size;

    // 写入 (seq << 8 | type)，高 56 位为序列号，低 8 位为操作类型
    uint64_t packed = (seq << 8) | static_cast<uint8_t>(type);
    EncodeFixed64(p, packed);
    p += 8;

    // 写入 Value 长度前缀和 Value
    tmp.clear();
    PutVarint32(&tmp, static_cast<uint32_t>(val_size));
    memcpy(p, tmp.data(), tmp.size());
    p += tmp.size();
    memcpy(p, value.data(), val_size);

    table_.Insert(buf);
}

// Get 在跳表中查找 UserKey 的最新版本。
// 构造一个序列号为最大值的 LookupKey，使 Seek 命中该 UserKey 下序列号最大（最新）的节点。
bool MemTable::Get(const Slice& key, std::string* value) {
    size_t key_size          = key.size();
    size_t internal_key_size = key_size + 8;

    // 构造查找键：序列号全 1（56 位），type = kTypeValue，保证 Seek 定位到该 key 的最新版本。
    std::string lookup_buf;
    PutVarint32(&lookup_buf, static_cast<uint32_t>(internal_key_size));
    lookup_buf.append(key.data(), key_size);
    uint64_t packed = (0xFFFFFFFFFFFFFFull << 8) | 0x01;
    PutFixed64(&lookup_buf, packed);

    Table::Iterator iter(&table_);
    iter.Seek(lookup_buf.data());

    if (!iter.Valid()) return false;

    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);

    // 校验 UserKey 是否匹配（InternalKey 末 8 字节为 seq+type，需排除）。
    if (comparator_.comparator->Compare(Slice(key_ptr, key_length - 8), key) != 0) {
        return false;
    }

    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    switch (tag & 0xff) {
        case kTypeValue: {
            Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
            value->assign(v.data(), v.size());
            return true;
        }
        case kTypeDeletion:
            // 找到墓碑：key 已被删除，无需继续向磁盘查找，直接返回 true 终止查找链。
            return true;
        default:
            return false;
    }
}

Iterator* MemTable::NewIterator() {
    return new MemTableIterator(&table_);
}

} // namespace minidb
