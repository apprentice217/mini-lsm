#pragma once

#include <vector>
#include "iterator.h"
#include "comparator.h"

namespace minidb {

// MergingIterator 对 n 个有序子迭代器做 k 路归并，对外呈现全局有序视图。
// 算法：每次 Next()/Prev() 后，在所有 Valid 的子迭代器中找到当前最小（或最大）的 key。
// 时间复杂度：O(n) 每步（n = 子迭代器数量），足够应对 kNumLevels 级别的规模。
//
// 所有权：MergingIterator 拥有传入的子迭代器，析构时自动 delete。
class MergingIterator : public Iterator {
public:
    // 构造函数接管 children 的所有权。
    MergingIterator(const Comparator* comparator,
                    std::vector<Iterator*> children);
    ~MergingIterator() override;

    bool Valid() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const Slice& target) override;
    void Next() override;
    void Prev() override;
    Slice key() const override;
    Slice value() const override;
    Status status() const override;

private:
    // 在所有 Valid 子迭代器中找到 key 最小的那个，设为 current_。
    void FindSmallest();
    // 在所有 Valid 子迭代器中找到 key 最大的那个，设为 current_。
    void FindLargest();

    const Comparator*    comparator_;
    std::vector<Iterator*> children_;
    Iterator*            current_;  // 当前指向最小（forward）或最大（backward）的子迭代器
    bool                 forward_;  // 当前遍历方向
};

} // namespace minidb
