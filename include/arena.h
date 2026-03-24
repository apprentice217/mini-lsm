#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace minidb {

// Arena 是一个面向追加的内存池，适用于生命周期与分配者一致的场景（如 MemTable）。
// 所有分配的内存在 Arena 析构时统一释放，避免大量单节点 delete 的开销。
// memory_usage_ 使用原子变量，供上层无锁读取已用内存量（用于 flush 触发判断）。
class Arena {
public:
    Arena();
    ~Arena();

    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    // Allocate 快速路径：在当前 Block 剩余空间足够时直接移动指针，接近零开销。
    char* Allocate(size_t bytes);

    // AllocateAligned 在对齐到 alignof(max_align_t) 边界处分配，
    // 用于存储含 std::atomic 成员的跳表节点，避免非对齐访问导致的未定义行为。
    char* AllocateAligned(size_t bytes);

    size_t MemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

    char*  alloc_ptr_;
    size_t alloc_bytes_remaining_;

    std::vector<char*>   blocks_;
    std::atomic<size_t>  memory_usage_;
};

inline char* Arena::Allocate(size_t bytes) {
    assert(bytes > 0);
    if (bytes <= alloc_bytes_remaining_) {
        char* result = alloc_ptr_;
        alloc_ptr_             += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    return AllocateFallback(bytes);
}

} // namespace minidb
