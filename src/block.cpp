#include "block.h"
#include "coding.h"
#include "comparator.h"
#include <vector>
#include <string>
#include <cassert>

namespace minidb {

// ---------------------------------------------------------------------------
// EmptyIterator：用于 Block 为空或已损坏时，代替 nullptr 返回给调用方。
// 调用方可统一使用 Valid() 检查，无需做 null guard。
// ---------------------------------------------------------------------------
class EmptyIterator : public Iterator {
public:
    explicit EmptyIterator(const Status& s) : status_(s) {}
    bool   Valid()        const override { return false; }
    void   Seek(const Slice&)  override {}
    void   SeekToFirst()       override {}
    void   SeekToLast()        override {}
    void   Next()              override { assert(false); }
    void   Prev()              override { assert(false); }
    Slice  key()          const override { assert(false); return Slice(); }
    Slice  value()        const override { assert(false); return Slice(); }
    Status status()       const override { return status_; }
private:
    Status status_;
};

// ---------------------------------------------------------------------------
// Block 迭代器：基于重启点的二分 + 线性扫描混合查找。
// ---------------------------------------------------------------------------
class Iter : public Iterator {
public:
    Iter(const Comparator* comparator,
         const char* data,
         uint32_t restarts,
         uint32_t num_restarts)
        : comparator_(comparator),
          data_(data),
          restarts_(restarts),
          num_restarts_(num_restarts),
          current_(restarts_),
          restart_index_(num_restarts_) {
        assert(num_restarts_ > 0);
    }

    bool   Valid()  const override { return current_ < restarts_; }
    Status status() const override { return status_; }
    Slice  key()    const override { assert(Valid()); return Slice(key_); }
    Slice  value()  const override { assert(Valid()); return value_; }

    void Next() override {
        assert(Valid());
        ParseNextKey();
    }

    void Prev() override {
        assert(Valid());
        const uint32_t original = current_;
        // 向前查找需要退回到当前重启点，再线性扫描到 original 之前的最后一个 key。
        while (GetRestartPoint(restart_index_) >= original) {
            if (restart_index_ == 0) {
                current_       = restarts_;
                restart_index_ = num_restarts_;
                return;
            }
            restart_index_--;
        }
        SeekToRestartPoint(restart_index_);
        do {} while (ParseNextKey() && current_ < original);
    }

    void SeekToFirst() override {
        SeekToRestartPoint(0);
    }

    void SeekToLast() override {
        SeekToRestartPoint(num_restarts_ - 1);
        while (Valid()) {
            uint32_t next_entry_offset =
                static_cast<uint32_t>((value_.data() + value_.size()) - data_);
            if (next_entry_offset >= restarts_) break;
            ParseNextKey();
        }
    }

    // Seek 先用二分定位最接近 target 的重启点，再线性扫描至第一个 >= target 的 key。
    // 线性部分最多扫描 block_restart_interval 条记录（默认 16）。
    void Seek(const Slice& target) override {
        uint32_t left  = 0;
        uint32_t right = num_restarts_ - 1;

        while (left < right) {
            uint32_t mid           = (left + right + 1) / 2;
            uint32_t region_offset = GetRestartPoint(mid);

            uint32_t shared, non_shared, value_length;
            const char* p     = data_ + region_offset;
            const char* limit = data_ + restarts_;

            if (!GetVarint32(&p, limit, &shared) ||
                !GetVarint32(&p, limit, &non_shared) ||
                !GetVarint32(&p, limit, &value_length)) {
                CorruptionError();
                return;
            }
            // 重启点处 shared 必须为 0（完整 key，无前缀依赖）。
            assert(shared == 0);
            Slice mid_key(p, non_shared);
            if (comparator_->Compare(mid_key, target) < 0) {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        SeekToRestartPoint(left);
        while (Valid() && comparator_->Compare(key(), target) < 0) {
            ParseNextKey();
        }
    }

private:
    const Comparator* const comparator_;
    const char* const       data_;
    uint32_t                restarts_;
    uint32_t                num_restarts_;

    uint32_t    current_;
    uint32_t    restart_index_;
    std::string key_;    // 当前解码出的完整 key（动态拼接前缀压缩）
    Slice       value_;  // 当前 value 的零拷贝视图（指向 data_ 内部）
    Status      status_;

    void CorruptionError() {
        current_       = restarts_;
        restart_index_ = num_restarts_;
        status_        = Status::Corruption("bad entry in block");
        key_.clear();
        value_.clear();
    }

    uint32_t GetRestartPoint(uint32_t index) const {
        assert(index < num_restarts_);
        return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
    }

    void SeekToRestartPoint(uint32_t index) {
        key_.clear();
        restart_index_ = index;
        uint32_t offset = GetRestartPoint(index);
        // 将 value_ 置为长度为 0、末尾指向 offset 处的空 Slice，
        // 使 ParseNextKey 从正确位置开始解析第一条记录。
        value_ = Slice(data_ + offset, 0);
        current_ = offset;
        ParseNextKey();
    }

    bool ParseNextKey() {
        current_ = static_cast<uint32_t>((value_.data() + value_.size()) - data_);
        const char* p     = data_ + current_;
        const char* limit = data_ + restarts_;

        if (p >= limit) {
            current_       = restarts_;
            restart_index_ = num_restarts_;
            return false;
        }

        uint32_t shared, non_shared, value_length;
        if (!GetVarint32(&p, limit, &shared)     ||
            !GetVarint32(&p, limit, &non_shared)  ||
            !GetVarint32(&p, limit, &value_length)) {
            CorruptionError();
            return false;
        }
        if (shared > key_.size() || p + non_shared + value_length > limit) {
            CorruptionError();
            return false;
        }

        // 还原前缀压缩：保留前 shared 字节，追加非共享部分。
        key_.resize(shared);
        key_.append(p, non_shared);
        value_ = Slice(p + non_shared, value_length);

        // 推进 restart_index_ 以跟踪当前条目所属的重启点区间。
        while (restart_index_ + 1 < num_restarts_ &&
               GetRestartPoint(restart_index_ + 1) < current_) {
            ++restart_index_;
        }
        return true;
    }
};

// ---------------------------------------------------------------------------
// Block
// ---------------------------------------------------------------------------

Block::Block(const Slice& contents)
    : data_(contents.data()),
      size_(contents.size()),
      restart_offset_(0),
      owned_(false) {
    if (size_ < sizeof(uint32_t)) {
        size_ = 0;
        return;
    }
    uint32_t max_restarts_allowed =
        static_cast<uint32_t>((size_ - sizeof(uint32_t)) / sizeof(uint32_t));
    uint32_t num_restarts = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    if (num_restarts > max_restarts_allowed) {
        size_ = 0; // 块已损坏，标记为无效。
    } else {
        restart_offset_ = size_ - (1 + num_restarts) * static_cast<uint32_t>(sizeof(uint32_t));
    }
}

// NewIterator 在块有效时返回 Iter，否则返回 EmptyIterator，调用方无需检查 nullptr。
Iterator* Block::NewIterator(const Comparator* comparator) {
    if (size_ < sizeof(uint32_t)) {
        return new EmptyIterator(Status::Corruption("block is too small"));
    }
    const uint32_t num_restarts = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    if (num_restarts == 0) {
        return new EmptyIterator(Status::OK());
    }
    return new Iter(comparator, data_, restart_offset_, num_restarts);
}

} // namespace minidb
