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
// 用长度可变的字节序列表示无符号整数:数值越小，通常需要的字节越少。
// 每个字节的低 7 位用于存储数据，高位用于标记是否还有后续字节:
//  最高位为1: 还需要继续读取下一个字节
//  最高位为0: 当前字节是这个最后一个字节
// 数值的低位部分先写入，高位部分后写入。
//
// 例如:
// 0~127 使用一个字节。
// 128~16383 使用两个字节。
// uint32_t 最多使用五个字节
// uint64_t 最多使用十个字节
//
// 注意: Varint 并不总是比固定长度编码更省空间
// 较大的 uint32_t 可能占 5 字节，比固定 4 字节还多。

// 将 v 编码为 Varint，并追加到 dst 指向的字符串末尾。
// 保留字符串中已有的内容；dst 必须指向一个有效的 std::string 对象。
// 追加的是二进制字节，不是数字的十进制文本。
void PutVarint32(std::string* dst, uint32_t v);
void PutVarint64(std::string* dst, uint64_t v);

// 将 v 编码为 Varint，直接写入 dst 指向的可写缓冲区。
// 调用者负责提供足够的空间；预留 5 字节可以容纳任意 uint32_t。
// 本函数不分配内存，也不额外添加字符串结束符 '\0'。
// 返回最后一个已写字节之后的位置，方便紧接着写入其他数据。
// 返回指针与原 dst 的差值，就是本次写入的字节数。
char* EncodeVarint32(char* dst, uint32_t v);

// 从 *p 指向的位置读取一个 Varint，并解码为对应的无符号整数。
//
// 参数：
//   p     ：读取位置变量的地址。*p 是当前要读取的字节地址。
//           使用二级指针，是为了让函数能够修改调用者的读取位置。
//   limit ：可读区域末尾之后的位置；可以读取的范围是 [*p, limit)。
//           limit 指向的位置不能读取。
//   value ：接收解码结果的变量地址。
//
// 成功时：
//   将解码得到的整数写入 *value。
//   将 *p 移动到本次整数编码之后，方便继续读取后面的字段。
//   返回 true。
//
// 失败时：
//   如果输入提前结束，或编码被实现判定为无效，返回 false。
//   调用者不应使用本次解码结果，也不应假定 *p 仍停在原位置。
bool GetVarint32(const char** p, const char* limit, uint32_t* value);
bool GetVarint64(const char** p, const char* limit, uint64_t* value);

// 辅助函数：计算一个数字被 Varint 编码后会占据多少字节
int VarintLength(uint64_t v);

} // namespace minidb