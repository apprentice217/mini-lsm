#pragma once

#include <cstdint>
#include <string>
#include "slice.h"
#include "status.h"

namespace minidb {

// 架构师级魔数：0xdb4775248b80fb57ull (LevelDB 经典魔数)
// 占据 Footer 的最后 8 字节，用于在文件打开时极速验证合法性
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// BlockHandle：指示一个 Block 在文件中的物理区间
class BlockHandle {
public:
    BlockHandle() : offset_(~static_cast<uint64_t>(0)), size_(0) {}

    uint64_t offset() const { return offset_; }
    void set_offset(uint64_t offset) { offset_ = offset; }

    uint64_t size() const { return size_; }
    void set_size(uint64_t size) { size_ = size; }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

    // 最大编码长度：2 个 64 位变长整数，每个最多 10 字节
    enum { kMaxEncodedLength = 10 + 10 };

private:
    uint64_t offset_;
    uint64_t size_;
};

// Footer：SSTable 文件的尾部定海神针，固定 48 字节
class Footer {
public:
    Footer() = default;

    const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
    void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

    const BlockHandle& index_handle() const { return index_handle_; }
    void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

    // 严苛的物理尺寸：2个最大 Handle (40字节) + 8字节魔数 = 48字节
    enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

private:
    BlockHandle metaindex_handle_;
    BlockHandle index_handle_;
};

} // namespace minidb