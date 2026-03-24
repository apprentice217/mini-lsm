#pragma once
#include <cstdint>

namespace minidb {

// Snapshot 代表某一时刻数据库的一致性视图，对应一个固定的序列号。
// 持有 Snapshot 的读操作只能看到序列号 <= snapshot_sequence 的写入，
// 即使之后发生了新的写入或 compaction，查询结果也不会改变。
// Snapshot 由 DBImpl::GetSnapshot() 分配，DBImpl::ReleaseSnapshot() 释放。
class Snapshot {
public:
    uint64_t sequence() const { return sequence_; }

private:
    friend class DBImpl;
    explicit Snapshot(uint64_t seq) : sequence_(seq) {}

    uint64_t sequence_;
};

} // namespace minidb
