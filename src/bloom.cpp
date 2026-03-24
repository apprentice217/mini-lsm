#include "filter_policy.h"
#include "hash.h"

namespace minidb {

// BloomFilterPolicy 实现了经典的 Bloom Filter，通过 k 次独立哈希函数设置/查询 bit 数组。
// 每个 key 的哈希由一个基础哈希和一个 delta（基础哈希的旋转变体）迭代生成，
// 近似于 k 个独立哈希函数，计算代价极低。
class BloomFilterPolicy : public FilterPolicy {
public:
    explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
        // 最优探测次数 k ≈ bits_per_key × ln(2) ≈ bits_per_key × 0.69。
        k_ = static_cast<size_t>(bits_per_key * 0.69);
        if (k_ < 1)  k_ = 1;
        if (k_ > 30) k_ = 30;
    }

    const char* Name() const override {
        return "minidb.BuiltinBloomFilter2";
    }

    // CreateFilter 为给定的 key 集合生成 bit 数组，追加到 dst。
    // 末尾额外存储 k_ 值，供 KeyMayMatch 读取。
    void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
        size_t bits  = static_cast<size_t>(n) * bits_per_key_;
        if (bits < 64) bits = 64;
        size_t bytes = (bits + 7) / 8;
        bits = bytes * 8;

        const size_t init_size = dst->size();
        dst->resize(init_size + bytes, 0);
        dst->push_back(static_cast<char>(k_));

        char* array = &(*dst)[init_size];
        for (int i = 0; i < n; i++) {
            uint32_t h     = Hash(keys[i].data(), keys[i].size(), 0xbc9f1d34);
            uint32_t delta = (h >> 17) | (h << 15); // 旋转哈希，模拟独立哈希函数
            for (size_t j = 0; j < k_; j++) {
                uint32_t bitpos = h % static_cast<uint32_t>(bits);
                array[bitpos / 8] |= (1 << (bitpos % 8));
                h += delta;
            }
        }
    }

    // KeyMayMatch 重复 CreateFilter 中的哈希过程，检查每个 bit 是否被设置。
    // 任意一个 bit 为 0 则确定不存在；全部为 1 则可能存在（存在假阳性）。
    bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
        const size_t len = bloom_filter.size();
        if (len < 2) return false;

        const char*  array = bloom_filter.data();
        const size_t bits  = (len - 1) * 8;
        const size_t k     = static_cast<uint8_t>(array[len - 1]);
        if (k > 30) return true; // k 异常，保守放行

        uint32_t h     = Hash(key.data(), key.size(), 0xbc9f1d34);
        uint32_t delta = (h >> 17) | (h << 15);
        for (size_t j = 0; j < k; j++) {
            uint32_t bitpos = h % static_cast<uint32_t>(bits);
            if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
                return false;
            }
            h += delta;
        }
        return true;
    }

private:
    size_t bits_per_key_;
    size_t k_;
};

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
    return new BloomFilterPolicy(bits_per_key);
}

} // namespace minidb
