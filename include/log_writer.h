#pragma once

#include <cstdint>
#include "slice.h"
#include "status.h"
#include "log_format.h"
#include "env.h"

namespace minidb {


namespace log {

class Writer {
public:
    // 构造函数接管文件句柄的所有权（或借用），通常由上层 Env 环境类提供
    explicit Writer(WritableFile* dest);

    // 严禁拷贝
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    // 核心契约：将一条用户的完整数据安全地写入物理日志
    Status AddRecord(const Slice& slice);

private:
    // 内部实现：将一个物理切片（Chunk）封装 Header 后刷入磁盘
    Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

    WritableFile* dest_;
    
    // 当前 Block 已经消耗的字节数，取值范围 [0, kBlockSize)
    int block_offset_;

    // 为了极致性能，预先计算好每种 Type 对应的 CRC 校验和的前缀
    uint32_t type_crc_[kMaxRecordType + 1];
};

} // namespace log
} // namespace minidb