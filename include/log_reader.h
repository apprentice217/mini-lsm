#pragma once

#include <cstdint>
#include <string>
#include "env.h"
#include "slice.h"
#include "status.h"
#include "log_format.h"


namespace minidb {

namespace log {

class Reader {
public:
    // 工业级日志读取器必须具备容错汇报机制
    class Reporter {
    public:
        virtual ~Reporter() = default;
        // 当物理层发现 CRC 错误、长度越界等情况时，回调此接口
        virtual void Corruption(size_t bytes, const Status& status) = 0;
    };

    // checksum: 是否开启严苛的 CRC 校验（通常在灾难恢复时必须开启）
    // initial_offset: 开始读取的物理偏移量
    Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset);
    
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;
    ~Reader();

    // 核心接口：读取一条完整的逻辑记录
    // 如果记录跨越了 Block，将使用 scratch 拼接；
    // 否则 record 直接指向底层物理 Buffer，实现零拷贝。
    // 返回值：true 表示成功读取一条数据；false 表示到达文件尾部。
    bool ReadRecord(Slice* record, std::string* scratch);

private:
    enum {
        kEof = kMaxRecordType + 1,
        // 遇到彻底损坏的 Chunk 时的标识
        kBadRecord = kMaxRecordType + 2
    };

    // 读取并校验一个物理切片，返回其类型（RecordType）
    unsigned int ReadPhysicalRecord(Slice* result);

    // 内部报告错误的辅助方法
    void ReportCorruption(uint64_t bytes, const char* reason);
    void ReportDrop(uint64_t bytes, const Status& reason);

    SequentialFile* const file_;
    Reporter* const reporter_;
    bool const checksum_;
    
    // 32KB 物理块的内存缓冲池
    char* const backing_store_;
    Slice buffer_;

    // 指示是否已经到达文件末尾
    bool eof_;
};

} // namespace log
} // namespace minidb