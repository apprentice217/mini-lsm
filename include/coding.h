#pragma once

#include <cstdint>
#include <string>

namespace minidb {

// =========================================================================
// 定长编码 (Fixed) - 强制小端序 (Little-Endian)
// 无论当前 CPU 是 x86 (小端) 还是 ARM/PowerPC 的某些大端模式，
// 存入磁盘的字节流必须是绝对一致的。
// =========================================================================

inline void EncodeFixed32(char* dst, uint32_t value) {
    auto* buffer = reinterpret_cast<uint8_t*>(dst);
    buffer[0] = static_cast<uint8_t>(value);
    buffer[1] = static_cast<uint8_t>(value >> 8);
    buffer[2] = static_cast<uint8_t>(value >> 16);
    buffer[3] = static_cast<uint8_t>(value >> 24);
}

inline void EncodeFixed64(char* dst, uint64_t value) {
    auto* buffer = reinterpret_cast<uint8_t*>(dst);
    buffer[0] = static_cast<uint8_t>(value);
    buffer[1] = static_cast<uint8_t>(value >> 8);
    buffer[2] = static_cast<uint8_t>(value >> 16);
    buffer[3] = static_cast<uint8_t>(value >> 24);
    buffer[4] = static_cast<uint8_t>(value >> 32);
    buffer[5] = static_cast<uint8_t>(value >> 40);
    buffer[6] = static_cast<uint8_t>(value >> 48);
    buffer[7] = static_cast<uint8_t>(value >> 56);
}

inline uint32_t DecodeFixed32(const char* ptr) {
    const auto* buffer = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint32_t>(buffer[0])) |
           (static_cast<uint32_t>(buffer[1]) << 8) |
           (static_cast<uint32_t>(buffer[2]) << 16) |
           (static_cast<uint32_t>(buffer[3]) << 24);
}

inline uint64_t DecodeFixed64(const char* ptr) {
    const auto* buffer = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint64_t>(buffer[0])) |
           (static_cast<uint64_t>(buffer[1]) << 8) |
           (static_cast<uint64_t>(buffer[2]) << 16) |
           (static_cast<uint64_t>(buffer[3]) << 24) |
           (static_cast<uint64_t>(buffer[4]) << 32) |
           (static_cast<uint64_t>(buffer[5]) << 40) |
           (static_cast<uint64_t>(buffer[6]) << 48) |
           (static_cast<uint64_t>(buffer[7]) << 56);
}

// 辅助追加函数：无缝将定长编码追加到 std::string 尾部
inline void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
}

inline void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
}

// =========================================================================
// 变长编码 (Varint)
// =========================================================================

// 将 32/64 位无符号整数进行 Varint 压缩，并直接追加到 string 尾部
void PutVarint32(std::string* dst, uint32_t v);
void PutVarint64(std::string* dst, uint64_t v);

// 核心解码契约 (TLV 游走法)：
// 从 *p 开始解析，最多解析到 limit 之前。
// 如果解析成功，将值写入 *value，并将 *p 向前推进到下一个数据的起始位置，返回 true。
// 如果数据不完整（超出 limit）或格式损坏，返回 false，且不保证 *p 的状态。
bool GetVarint32(const char** p, const char* limit, uint32_t* value);
bool GetVarint64(const char** p, const char* limit, uint64_t* value);

// 辅助函数：计算一个数字被 Varint 编码后会占据多少字节
int VarintLength(uint64_t v);

} // namespace minidb