#include "hash.h"

namespace minidb {

uint32_t Hash(const char* data, size_t n, uint32_t seed) {
    // 经典的哈希乘数
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char* limit = data + n;
    uint32_t h = seed ^ (n * m);

    // 每次处理 4 个字节
    while (data + 4 <= limit) {
        uint32_t w;
        // 假设是小端序机器，直接读取
        __builtin_memcpy(&w, data, sizeof(w)); 
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }

    // 处理尾部剩余的字节
    switch (limit - data) {
        case 3:
            h += static_cast<uint8_t>(data[2]) << 16;
            [[fallthrough]];
        case 2:
            h += static_cast<uint8_t>(data[1]) << 8;
            [[fallthrough]];
        case 1:
            h += static_cast<uint8_t>(data[0]);
            h *= m;
            h ^= (h >> r);
            break;
    }
    return h;
}

} // namespace minidb