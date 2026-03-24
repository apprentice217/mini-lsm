#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

namespace minidb {

class Slice {
public:
    // 默认构造一个空的Slice
    Slice() : data_(""), size_(0) {}

    // 从C风格字符串构造,计算长度，时间复杂度O(N)
    Slice(const char* d):data_(d),size_(strlen(d)) {}

    // 从std::string构造,零拷贝获取底层指针和大小
    Slice(const std::string& s):data_(s.data()),size_(s.size()) {}

    // 从指针和大小构造,用于二进制数据的传递(如序列化后的日志记录)
    Slice(const char* d, size_t n) : data_(d), size_(n) {}

    // 默认的拷贝构造和赋值操作符即可满足要求(浅拷贝)
    Slice(const Slice&) = default;
    Slice& operator=(const Slice&) = default;

    // 核心数据访问接口
    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    // 重载下标操作符，支持断言边界检查（防御性编程)
    char operator[](size_t n) const {
        assert(n < size()); // 确保访问不越界
        return data_[n];
    }

    // 清空当前视图
    void clear() {
        data_ = "";
        size_ = 0;
    }

    // O(1)的截断操作：用于解析协议或WAL日志时，快速跳过已处理的header
    void remove_prefix(size_t n) {
        assert(n <= size());
        data_ += n;
        size_ -= n;
    }

    // 转换为拥有所有权的std::string,通常只在向用户返回数据时调用
    std::string ToString() const {
        return std::string(data_, size_);
    }

    // 核心比较操作，用于SkipList 和 SSTable 的键排序
    // 返回值：< 0(小于),==0(等于),>0(大于)
    int compare(const Slice& b) const ;

private:
    const char* data_; 
    size_t size_;
};

inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size()==y.size()) && 
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) {
    return !(x == y);

}

inline int Slice::compare(const Slice& b) const {
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
        if (size_ < b.size_) r = -1;
        else if (size_ > b.size_) r = +1;
    }
    return r;
}

}// namespace minidb