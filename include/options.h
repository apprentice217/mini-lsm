#pragma once
#include <cstddef>
#include "comparator.h"
#include "filter_policy.h"

namespace minidb {

class Snapshot; // 前向声明，避免循环依赖

struct Options {
    bool   create_if_missing  = false;
    size_t write_buffer_size  = 4 * 1024 * 1024; // MemTable 大小阈值，超过后触发 flush
    int    block_restart_interval = 16;           // Block 前缀压缩重启点间隔
    size_t block_size = 16 * 1024;                // Data Block 目标大小（默认 16KB）

    const Comparator*  comparator    = BytewiseComparator();
    const FilterPolicy* filter_policy = nullptr;

    // L0 文件数量达到此阈值时触发 L0->L1 compaction。
    int l0_compaction_trigger = 4;
    // 每个 Level（L1+）允许的最大总字节数（字节）。
    // L1: 10MB, L2: 100MB, L3: 1GB ...（每级 ×10）
    int64_t max_bytes_for_level_base = 10 * 1024 * 1024;

    // 仅用于实验：关闭自动 level compaction（不影响 memtable flush）。
    bool disable_auto_compaction = false;
};

struct ReadOptions {
    bool verify_checksums = false;
    bool fill_cache       = true;
    // 若非 nullptr，则读取该 Snapshot 时刻的数据；否则读取最新数据。
    const Snapshot* snapshot = nullptr;
};

struct WriteOptions {
    bool sync = false;
};

} // namespace minidb
