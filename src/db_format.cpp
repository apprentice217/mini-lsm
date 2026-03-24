#include "db_format.h"

namespace minidb {

// InternalKeyComparator 的排序规则：
//   1. UserKey 按用户配置的比较器升序排列；
//   2. 相同 UserKey 下，序列号（高 56 位）降序排列，确保最新版本排在前面，
//      使 Seek 时优先命中最新数据。
int InternalKeyComparator::Compare(const Slice& a, const Slice& b) const {
    Slice user_a = ExtractUserKey(a);
    Slice user_b = ExtractUserKey(b);
    int r = user_comparator_->Compare(user_a, user_b);
    if (r != 0) return r;

    uint64_t num_a = DecodeFixed64(a.data() + a.size() - 8);
    uint64_t num_b = DecodeFixed64(b.data() + b.size() - 8);
    if (num_a > num_b) return -1; // a 的序列号更大（更新），排在 b 前面
    if (num_a < num_b) return  1;
    return 0;
}

} // namespace minidb
