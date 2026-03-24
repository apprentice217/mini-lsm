#pragma once

#include <cstdint>
#include "iterator.h"
#include "slice.h"

namespace minidb {

class Comparator;

// Block 是 SSTable 中最小的物理存储单元，对应 Data Block 或 Index Block。
// 内部使用前缀压缩 + 重启点数组结构，支持 O(log n) 的二分查找定位和 O(k) 的线性扫描。
// 块内格式：[record_0][record_1]...[restart_0: 4B]...[num_restarts: 4B]
class Block {
public:
    explicit Block(const Slice& contents);

    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    // NewIterator 返回块内迭代器。若块已损坏或为空，返回一个 Valid() 始终为 false 的 EmptyIterator，
    // 调用方无需对返回值做 nullptr 检查。
    Iterator* NewIterator(const Comparator* comparator);

private:
    const char* data_;
    size_t      size_;
    uint32_t    restart_offset_; // 重启点数组在 data_ 中的起始偏移
    bool        owned_;
};

} // namespace minidb
