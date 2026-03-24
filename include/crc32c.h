#pragma once

#include <cstddef>
#include <cstdint>

namespace minidb {
namespace crc32c {

// 计算底层数据的 CRC32C (Castagnoli) 校验和
uint32_t Value(const char* data, size_t n);

// 在现有的 CRC 基础上继续追加数据的校验和（用于 Chunk 切片时的连续计算）
uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

// 核心掩码机制：
// 避免用户的 Payload 数据与日志 Header 格式发生概率性碰撞，
// 导致 WAL Reader 在奔溃恢复时将 Payload 误认为合法 Header。
inline uint32_t Mask(uint32_t crc) {
    // 循环右移 15 位并加上魔数
    return ((crc >> 15) | (crc << 17)) + 0xa282ead8ul;
}

// 掩码逆运算：用于 WAL Reader 校验时还原真实 CRC
inline uint32_t Unmask(uint32_t masked_crc) {
    uint32_t rot = masked_crc - 0xa282ead8ul;
    // 循环右移 17 位（等价于 32 位下的循环左移 15 位）
    return ((rot >> 17) | (rot << 15));
}

} // namespace crc32c
} // namespace minidb