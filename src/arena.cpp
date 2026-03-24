#include "arena.h"
#include <new>       // for std::bad_alloc
#include <algorithm> // for std::max

namespace minidb {

// 默认的批发大块尺寸：4KB（恰好对应操作系统的一个标准内存页，避免内部碎片）
static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr),
      alloc_bytes_remaining_(0),
      memory_usage_(0) {}

Arena::~Arena() {
    // 引擎的生命周期哲学：同生共死，一次性 O(1) 批量屠宰
    for (size_t i = 0; i < blocks_.size(); i++) {
        delete[] blocks_[i];
    }
}

char* Arena::AllocateFallback(size_t bytes) {
    // 策略一：如果单次请求的内存大于 1KB (kBlockSize / 4)
    // 为了不浪费当前 Block 仅存的一点空间，直接为这个大对象单独开辟一个 Block
    if (bytes > kBlockSize / 4) {
        char* result = AllocateNewBlock(bytes);
        return result;
    }

    // 策略二：对于小对象，直接申请一个新的 4KB 默认 Block
    // 并将后续的分配指针切换到这个新 Block 上
    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* Arena::AllocateAligned(size_t bytes) {
    // 获取当前架构下的最大对齐要求（通常 64 位系统下是 8 字节或 16 字节）
    const int align = alignof(std::max_align_t);
    
    // 架构师断言：对齐数必须是 2 的幂次方，这是后续进行位操作的基础
    assert((align & (align - 1)) == 0);

    // 核心位运算技巧：计算当前指针需要偏移多少字节才能对齐
    // 等价于 reinterpret_cast<uintptr_t>(alloc_ptr_) % align，但位操作（&）比除法指令快得多
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
    
    // 如果 current_mod 为 0，说明已经对齐（slop 为 0）
    // 否则，补齐到下一个对齐边界的距离就是 align - current_mod
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    
    // 实际需要消耗的内存 = 用户请求的内存 + 为了对齐而浪费掉的填充字节（Padding）
    size_t needed = bytes + slop;

    char* result;
    if (needed <= alloc_bytes_remaining_) {
        // fast-path: 当前 Block 空间足够包容对齐填充和数据
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        // slow-path: 空间不足。
        // AllocateFallback 内部会调用 AllocateNewBlock，而标准的 new char[] 
        // 已经向系统保证了其返回的地址满足基本对齐要求（__STDCPP_DEFAULT_NEW_ALIGNMENT__），
        // 因此直接分配即可，无需再做指针修剪。
        result = AllocateFallback(bytes);
    }
    return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
    // 核心路径严禁使用复杂的异常捕获，直接向 OS 请求内存
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    
    // 原子地累加内存使用量。
    // 为什么使用 std::memory_order_relaxed？
    // 因为这里只负责计费，并不用作线程间的 happens-before 内存屏障。
    // MemTable 自身的并发控制（比如多个写线程）会在上层由更高级的同步机制（如 SkipList 的原子指针或外部 Mutex）保证，
    // 这里只需保证计数器本身的递增不丢失即可，避免了强内存屏障带来的总线锁开销。
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
    return result;
}

} // namespace minidb