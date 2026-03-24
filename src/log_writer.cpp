#include "log_writer.h"
#include "crc32c.h"
#include <cassert>

namespace minidb {
namespace log {

// 预先计算每种 RecordType（1 字节）对应的 CRC 前缀，避免在热路径中重复计算。
static void InitTypeCrc(uint32_t* type_crc) {
    for (int i = 0; i <= kMaxRecordType; i++) {
        char t = static_cast<char>(i);
        type_crc[i] = crc32c::Value(&t, 1);
    }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest), block_offset_(0) {
    InitTypeCrc(type_crc_);
}

// AddRecord 将一条逻辑记录切分为若干物理 Chunk，每个 Chunk 不跨越 32KB Block 边界。
// 根据所处位置，Chunk 类型为 kFullType / kFirstType / kMiddleType / kLastType。
Status Writer::AddRecord(const Slice& slice) {
    const char* ptr  = slice.data();
    size_t      left = slice.size();
    bool        begin = true;
    Status      s;

    do {
        const int leftover = kBlockSize - block_offset_;
        assert(leftover >= 0);

        // Block 尾部剩余空间不足一个 Header（7 字节）时，用零填充对齐到下一个 Block。
        if (leftover < kHeaderSize) {
            if (leftover > 0) {
                static const char padding[6] = {0, 0, 0, 0, 0, 0};
                (void)dest_->Append(Slice(padding, leftover));
            }
            block_offset_ = 0;
        }

        const size_t avail           = static_cast<size_t>(kBlockSize - block_offset_ - kHeaderSize);
        const size_t fragment_length = (left < avail) ? left : avail;
        const bool   end             = (left == fragment_length);

        RecordType type;
        if (begin && end) {
            type = kFullType;
        } else if (begin) {
            type = kFirstType;
        } else if (end) {
            type = kLastType;
        } else {
            type = kMiddleType;
        }

        s     = EmitPhysicalRecord(type, ptr, fragment_length);
        ptr  += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (s.ok() && left > 0);

    return s;
}

// EmitPhysicalRecord 写出一个物理 Chunk，格式：
// [crc32c: 4B][length: 2B][type: 1B][payload]
// CRC 覆盖 type + payload，掩码后存储防止与数据内容中的合法 CRC 值碰撞。
Status Writer::EmitPhysicalRecord(RecordType type, const char* ptr, size_t length) {
    assert(length <= 0xffff);
    assert(block_offset_ + kHeaderSize + length <= static_cast<size_t>(kBlockSize));

    char buf[kHeaderSize];
    buf[4] = static_cast<char>(length & 0xff);
    buf[5] = static_cast<char>(length >> 8);
    buf[6] = static_cast<char>(type);

    uint32_t crc = crc32c::Extend(type_crc_[type], ptr, length);
    crc          = crc32c::Mask(crc);
    buf[0] = static_cast<char>(crc & 0xff);
    buf[1] = static_cast<char>((crc >>  8) & 0xff);
    buf[2] = static_cast<char>((crc >> 16) & 0xff);
    buf[3] = static_cast<char>((crc >> 24) & 0xff);

    Status s = dest_->Append(Slice(buf, kHeaderSize));
    if (s.ok()) {
        s = dest_->Append(Slice(ptr, length));
        if (s.ok()) s = dest_->Flush();
    }
    block_offset_ += kHeaderSize + length;
    return s;
}

} // namespace log
} // namespace minidb
