#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <new>
#include "arena.h"

namespace minidb {

// SkipList 是一个基于概率平衡的有序集合，支持：
//   - 单线程写入（Insert），无需外部加锁；
//   - 多线程无锁并发读取（Contains、迭代器）。
// 并发安全性依赖节点指针的 Acquire-Release 内存序：
//   - 写者在发布节点前使用 memory_order_release，确保节点内容对后续读者可见；
//   - 读者使用 memory_order_acquire 读取指针，确保读到完整初始化的节点。
template <typename Key, class Comparator>
class SkipList {
private:
    struct Node;

public:
    explicit SkipList(Comparator cmp, Arena* arena);

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    void Insert(const Key& key);
    bool Contains(const Key& key) const;

    class Iterator {
    public:
        explicit Iterator(const SkipList* list);
        bool         Valid()    const;
        const Key&   key()      const;
        void         Next();
        void         Prev();
        void         Seek(const Key& target);
        void         SeekToFirst();
        void         SeekToLast();
    private:
        const SkipList* list_;
        Node*           node_;
    };

private:
    enum { kMaxHeight = 12 };

    int GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
    }

    int   RandomHeight();
    Node* NewNode(const Key& key, int height);
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;
    Node* FindLessThan(const Key& key) const;
    Node* FindLast() const;

    Comparator      const compare_;
    Arena*          const arena_;
    Node*           const head_;
    std::atomic<int>      max_height_;
    uint32_t              rnd_;
};

// ---------------------------------------------------------------------------
// Node 内存布局：[Key][atomic<Node*> 0][atomic<Node*> 1]...[atomic<Node*> height-1]
// next_[1] 仅为占位，实际通过 Arena 按 height 分配额外指针空间（柔性数组模拟）。
// ---------------------------------------------------------------------------
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
    explicit Node(const Key& k) : key(k) {}
    Key const key;
    std::atomic<Node*> next_[1];

    // Acquire 语义：读者读到非空指针时，该指针指向的节点内容对读者完全可见。
    Node* Next(int n) {
        return next_[n].load(std::memory_order_acquire);
    }
    // Release 语义：写者发布指针前，节点的所有初始化操作已对后续读者可见。
    void SetNext(int n, Node* x) {
        next_[n].store(x, std::memory_order_release);
    }
    // 写者独占阶段（节点尚未链入跳表），无需内存屏障。
    Node* NoBarrier_Next(int n) {
        return next_[n].load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node* x) {
        next_[n].store(x, std::memory_order_relaxed);
    }
};

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(Key(), kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
    for (int i = 0; i < kMaxHeight; i++) {
        head_->NoBarrier_SetNext(i, nullptr);
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::NewNode(const Key& key, int height) {
    // sizeof(Node) 已包含 next_[0]，因此仅需为剩余 height-1 个指针额外分配空间。
    size_t mem_size = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
    char*  mem      = arena_->AllocateAligned(mem_size);
    return new (mem) Node(key);
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
    // 线性同余 PRNG，生成 p=1/4 的几何分布高度，避免引入标准库随机数开销。
    // 不能直接使用最低位做取模判断：当前 LCG 参数会让低 2 位按固定周期振荡，
    // 高度分布会严重退化（几乎只有 1~2 层），导致 SkipList 接近线性扫描。
    // 这里改用高 16 位做判定，确保层高分布接近几何分布。
    int height = 1;
    while (height < kMaxHeight) {
        rnd_ = rnd_ * 1103515245 + 12345;
        uint32_t r16 = rnd_ >> 16;
        if ((r16 & 0x3u) != 0u) break;
        height++;
    }
    return height;
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x     = head_;
    int   level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next != nullptr && compare_(next->key, key) < 0) {
            x = next;
        } else {
            if (prev != nullptr) prev[level] = x;
            if (level == 0) return next;
            level--;
        }
    }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);
    // MemTable 通过序列号保证每次插入的 InternalKey 全局唯一。
    assert(x == nullptr || compare_(x->key, key) != 0);

    int height = RandomHeight();
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
        }
        // 写者单线程，relaxed 即可；读者即使读到旧值也只是少搜几层，不影响正确性。
        max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        // 节点尚未链入跳表，读者不可见，使用 NoBarrier 初始化 next_ 指针。
        x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    }
    // 从底层（Level 0）开始逐层发布节点，配合 Acquire 读，保证读者看到完整节点。
    for (int i = 0; i < height; i++) {
        prev[i]->SetNext(i, x);
    }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    return x != nullptr && compare_(x->key, key) == 0;
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list)
    : list_(list), node_(nullptr) {}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Iterator::Valid() const {
    return node_ != nullptr;
}

template <typename Key, class Comparator>
const Key& SkipList<Key, Comparator>::Iterator::key() const {
    assert(Valid());
    return node_->key;
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Iterator::Next() {
    assert(Valid());
    node_ = node_->Next(0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
    Node* x     = head_;
    int   level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr || compare_(next->key, key) >= 0) {
            if (level == 0) return x;
            level--;
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Iterator::Prev() {
    assert(Valid());
    // 跳表为单向链表，Prev 需从 head 线性查找当前节点的前驱。
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_) node_ = nullptr;
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
    node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Iterator::SeekToLast() {
    Node* x     = list_->head_;
    int   level = list_->GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr) {
            if (level == 0) {
                node_ = (x == list_->head_) ? nullptr : x;
                return;
            }
            level--;
        } else {
            x = next;
        }
    }
}

} // namespace minidb
