#include "block_builder.h"
#include "coding.h"
#include <cassert>
#include <algorithm>

namespace minidb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
    restarts_.push_back(0); // 第 0 个重启点始终在偏移 0 处。
}

void BlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    counter_   = 0;
    finished_  = false;
    last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
    // 裸数据 + 重启点数组（每项 4 字节）+ 数组长度（4 字节）
    return buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t);
}

// Finish 在 buffer_ 末尾追加重启点数组和数组长度，返回完整 Block 的 Slice 视图。
Slice BlockBuilder::Finish() {
    for (uint32_t offset : restarts_) {
        PutFixed32(&buffer_, offset);
    }
    PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));
    finished_ = true;
    return Slice(buffer_);
}

// Add 使用前缀压缩存储 key，每隔 kBlockRestartInterval 条记录强制插入一个重启点（完整 key）。
// 重启点确保解码时可从任意位置开始，并支持块内二分查找。
//
// 每条记录的格式：[shared_len: varint][non_shared_len: varint][val_len: varint][non_shared_key][value]
void BlockBuilder::Add(const Slice& key, const Slice& value) {
    Slice last_key_piece(last_key_);
    assert(!finished_);
    assert(counter_ <= options_->block_restart_interval);

    size_t shared = 0;
    if (counter_ < options_->block_restart_interval) {
        const size_t min_length = std::min(last_key_piece.size(), key.size());
        while (shared < min_length && last_key_piece[shared] == key[shared]) {
            shared++;
        }
    } else {
        // 到达重启点间隔，强制清零共享前缀，插入新的重启点。
        restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
        counter_ = 0;
    }

    const size_t non_shared = key.size() - shared;
    PutVarint32(&buffer_, static_cast<uint32_t>(shared));
    PutVarint32(&buffer_, static_cast<uint32_t>(non_shared));
    PutVarint32(&buffer_, static_cast<uint32_t>(value.size()));
    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());

    last_key_.resize(shared);
    last_key_.append(key.data() + shared, non_shared);
    counter_++;
}

} // namespace minidb
