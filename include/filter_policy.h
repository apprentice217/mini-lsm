#pragma once
#include <string>
#include "slice.h"

namespace minidb {

class FilterPolicy {
public:
    virtual ~FilterPolicy() = default;

    // 返回过滤器的名字，用于在磁盘上做兼容性检查
    virtual const char* Name() const = 0;

    // 核心 1：根据传入的一批 keys，生成布隆过滤器的 bit 数组，并追加到 dst 中
    virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const = 0;

    // 核心 2：判断给定的 key 是否“可能”存在于 filter 中
    virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

// 工厂函数：创建一个布隆过滤器策略
// bits_per_key: 决定了假阳性率，通常设为 10（假阳性率约为 1%）
const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

} // namespace minidb