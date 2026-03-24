#pragma once

#include <string>
#include "slice.h"

namespace minidb {

class Comparator {
public:
    virtual ~Comparator() = default;

    // 核心契约：三向比较
    // 返回值：< 0 (a < b), == 0 (a == b), > 0 (a > b)
    virtual int Compare(const Slice& a, const Slice& b) const = 0;

    // 比较器的唯一名称，用于在打开数据库时校验 SSTable 是否使用了相同的比较规则
    virtual const char* Name() const = 0;
};

// 暴露一个基于原生字节字典序的默认比较器（单例模式）
const Comparator* BytewiseComparator();

} // namespace minidb