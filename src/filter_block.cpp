#include "filter_block.h"
#include "filter_policy.h"
#include "coding.h"

namespace minidb {

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

// StartBlock 在每次 Data Block 切换时被调用，确保每 2^kFilterBaseLg 字节的数据
// 对应一个独立的布隆过滤器 segment。若数据增长跨越了多个 2KB 边界，则补齐空 segment。
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
    uint64_t filter_index = block_offset >> kFilterBaseLg;
    while (filter_index > filter_offsets_.size()) {
        GenerateFilter();
    }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
    start_.push_back(keys_.size());
    keys_.append(key.data(), key.size());
}

// Finish 序列化所有 segment 的偏移量数组，并在末尾记录数组起始位置和 kFilterBaseLg。
// 格式：[filter_0][filter_1]...[offset_0: 4B][offset_1: 4B]...[array_offset: 4B][base_lg: 1B]
Slice FilterBlockBuilder::Finish() {
    if (!start_.empty()) {
        GenerateFilter();
    }
    const uint32_t array_offset = static_cast<uint32_t>(result_.size());
    for (uint32_t off : filter_offsets_) {
        PutFixed32(&result_, off);
    }
    PutFixed32(&result_, array_offset);
    result_.push_back(static_cast<char>(kFilterBaseLg));
    return Slice(result_);
}

// GenerateFilter 将当前积累的 key 集合传给 FilterPolicy 生成一个布隆过滤器 bit 数组，
// 追加到 result_ 并记录其起始偏移。
void FilterBlockBuilder::GenerateFilter() {
    const size_t num_keys = start_.size();
    if (num_keys == 0) {
        filter_offsets_.push_back(static_cast<uint32_t>(result_.size()));
        return;
    }
    start_.push_back(keys_.size()); // 哨兵，方便计算最后一个 key 的长度
    std::vector<Slice> tmp_keys(num_keys);
    for (size_t i = 0; i < num_keys; i++) {
        const char* base = keys_.data() + start_[i];
        size_t      len  = start_[i + 1] - start_[i];
        tmp_keys[i]      = Slice(base, len);
    }
    filter_offsets_.push_back(static_cast<uint32_t>(result_.size()));
    policy_->CreateFilter(&tmp_keys[0], static_cast<int>(num_keys), &result_);
    keys_.clear();
    start_.clear();
}

// ---------------------------------------------------------------------------
// FilterBlockReader
// ---------------------------------------------------------------------------

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy, const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
    size_t n = contents.size();
    if (n < 5) return; // 最少 4 字节偏移量数组起始位置 + 1 字节 base_lg

    base_lg_ = static_cast<size_t>(contents[n - 1]);

    uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
    if (last_word > n - 5) return;

    data_   = contents.data();
    offset_ = data_ + last_word;
    num_    = (n - 5 - last_word) / 4;
}

// KeyMayMatch 根据 block_offset 定位对应的 Filter segment，调用 FilterPolicy 做多哈希检测。
// 若确定不存在（某位为 0）返回 false，从而省去磁盘 I/O；若不确定则返回 true（可能假阳性）。
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
    uint64_t index = block_offset >> base_lg_;
    if (index < num_) {
        uint32_t start = DecodeFixed32(offset_ + index * 4);
        uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
        if (start <= limit &&
            limit <= static_cast<size_t>(offset_ - data_)) {
            Slice filter = Slice(data_ + start, limit - start);
            return policy_->KeyMayMatch(key, filter);
        } else if (start == limit) {
            return false; // 该 segment 为空
        }
    }
    // 越界或异常情况：保守返回 true，交由磁盘层确认。
    return true;
}

} // namespace minidb
