#include "comparator.h"
#include <cstring>

namespace minidb {

namespace {

class BytewiseComparatorImpl : public Comparator {
public:
    int Compare(const Slice& a, const Slice& b) const override {
        const size_t min_len = (a.size() < b.size()) ? a.size() : b.size();
        int r = memcmp(a.data(), b.data(), min_len);
        if (r == 0) {
            if (a.size() < b.size()) r = -1;
            else if (a.size() > b.size()) r = +1;
        }
        return r;
    }

    const char* Name() const override {
        return "minidb.BytewiseComparator";
    }
};

} // anonymous namespace

const Comparator* BytewiseComparator() {
    // 工业级单例：C++11 保证局部静态变量的初始化是线程安全的
    static BytewiseComparatorImpl singleton;
    return &singleton;
}

} // namespace minidb