#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include "slice.h"
#include "options.h"

namespace minidb {

class BlockBuilder {
public:
    explicit BlockBuilder(const Options* options);
    
    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder& operator=(const BlockBuilder&) = delete;

    void Reset();
    
    // 核心契约：添加严格单调递增的键值对
    void Add(const Slice& key, const Slice& value);

    // 结束当前块的构建，追加重启点数组，返回完整内存视图
    Slice Finish();

    // 当前块预估大小，用于触发 TableBuilder 的 Flush
    size_t CurrentSizeEstimate() const;
    bool empty() const { return buffer_.empty(); }

private:
    enum { kBlockRestartInterval = 16 };

    const Options*        options_;   // 配置指针（如重启点间隔）
    std::string           buffer_;    // 块内容
    std::vector<uint32_t> restarts_;  // 重启点在 buffer_ 中的偏移量
    int                   counter_;   // 距离上一个重启点的 Key 数量
    bool                  finished_;  // 是否已调用 Finish
    std::string           last_key_;  // 暂存上一个完整 Key
};

} // namespace minidb