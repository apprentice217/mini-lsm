#include "crc32c.h"
#include <mutex>

namespace minidb {
namespace crc32c {

static uint32_t table[256];
static std::once_flag init_flag;

// 动态构建 Castagnoli 多项式 (0x82f63b78) 的查表
static void InitTable() {
    const uint32_t kPoly = 0x82f63b78;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            if (c & 1) {
                c = (c >> 1) ^ kPoly;
            } else {
                c >>= 1;
            }
        }
        table[i] = c;
    }
}

uint32_t Extend(uint32_t init_crc, const char* data, size_t n) {
    // 工业级保证：多线程并发下仅初始化一次，且无后续加锁开销
    std::call_once(init_flag, InitTable);
    
    // 按位取反以匹配标准的初始与结束状态
    uint32_t crc = init_crc ^ 0xffffffff;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(data);
    
    for (size_t i = 0; i < n; i++) {
        crc = table[(crc ^ p[i]) & 0xff] ^ (crc >> 8);
    }
    return crc ^ 0xffffffff;
}

uint32_t Value(const char* data, size_t n) {
    return Extend(0, data, n);
}

} // namespace crc32c
} // namespace minidb