#include "coding.h"

namespace minidb {

int VarintLength(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

void PutVarint32(std::string* dst, uint32_t v) {
    // 32位整数最多被编码为 5 个字节 (32 / 7 = 4.5)
    char buf[5];
    char* ptr = buf;

    // 只要数值大于等于 128 (即占用了 8 位或更多)，就需要延续位
    while (v >= 128) {
        // v & 0x7f：提取最低的 7 位有效数据
        // | 0x80 (128)：将第 8 位（最高位）置为 1，表示后面还有字节
        *(ptr++) = static_cast<char>((v & 0x7f) | 0x80);
        v >>= 7; // 数据右移 7 位，处理下一批数据
    }
    // 最后一个字节，最高位保持为 0
    *(ptr++) = static_cast<char>(v);

    // 零拷贝追加：直接将栈上的 buf 刷入 string 的底层缓冲
    dst->append(buf, ptr - buf);
}

void PutVarint64(std::string* dst, uint64_t v) {
    // 64位整数最多被编码为 10 个字节 (64 / 7 = 9.1)
    char buf[10];
    char* ptr = buf;

    while (v >= 128) {
        *(ptr++) = static_cast<char>((v & 0x7f) | 0x80);
        v >>= 7;
    }
    *(ptr++) = static_cast<char>(v);
    
    dst->append(buf, ptr - buf);
}

bool GetVarint32(const char** p, const char* limit, uint32_t* value) {
    const char* ptr = *p;
    uint32_t result = 0;
    
    // shift 表示当前正在还原的 7 位数据需要向左偏移的位数
    for (uint32_t shift = 0; shift <= 28 && ptr < limit; shift += 7) {
        // 必须转为无符号，避免符号扩展带来的灾难
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(ptr));
        ptr++;

        if (byte & 0x80) {
            // 延续位为 1：剥离最高位，左移后合并到 result
            result |= ((byte & 0x7f) << shift);
        } else {
            // 延续位为 0：这是最后一个字节
            result |= (byte << shift);
            *value = result;
            *p = ptr; // 游标推进！
            return true;
        }
    }
    // 循环结束还没有 return true，说明遇到了 limit 边界，或者数据损坏（超过 5 字节）
    return false;
}

bool GetVarint64(const char** p, const char* limit, uint64_t* value) {
    const char* ptr = *p;
    uint64_t result = 0;
    
    for (uint32_t shift = 0; shift <= 63 && ptr < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const uint8_t*>(ptr));
        ptr++;

        if (byte & 0x80) {
            result |= ((byte & 0x7f) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            *p = ptr;
            return true;
        }
    }
    return false;
}

} // namespace minidb