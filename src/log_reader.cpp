#include "log_reader.h"
#include "crc32c.h"
#include <cstdio>

namespace minidb {
namespace log {

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false) {
    // 若指定了初始偏移，将其向下对齐到 Block 边界，避免从 Block 中间开始解析。
    if (initial_offset > 0) {
        uint64_t block_start = (initial_offset / kBlockSize) * kBlockSize;
        (void)file_->Skip(block_start);
    }
}

Reader::~Reader() {
    delete[] backing_store_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
    ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
    if (reporter_ != nullptr && bytes > 0) {
        reporter_->Corruption(static_cast<size_t>(bytes), reason);
    }
}

// ReadRecord 将物理 Chunk（kFirstType/kMiddleType/kLastType）拼装为完整的逻辑记录。
// kFullType 记录可零拷贝直接返回对 backing_store_ 的视图；跨块记录通过 scratch 拼接。
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
    scratch->clear();
    record->clear();
    bool     in_fragmented_record = false;

    Slice fragment;
    while (true) {
        unsigned int record_type = ReadPhysicalRecord(&fragment);
        switch (record_type) {
            case kFullType:
                if (in_fragmented_record) {
                    ReportCorruption(scratch->size(), "partial record without end(1)");
                }
                scratch->clear();
                *record = fragment; // 零拷贝：直接暴露 backing_store_ 内的视图
                return true;

            case kFirstType:
                if (in_fragmented_record) {
                    ReportCorruption(scratch->size(), "partial record without end(2)");
                }
                scratch->assign(fragment.data(), fragment.size());
                in_fragmented_record = true;
                break;

            case kMiddleType:
                if (!in_fragmented_record) {
                    ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
                } else {
                    scratch->append(fragment.data(), fragment.size());
                }
                break;

            case kLastType:
                if (!in_fragmented_record) {
                    ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
                } else {
                    scratch->append(fragment.data(), fragment.size());
                    *record = Slice(*scratch);
                    return true;
                }
                break;

            case kEof:
                if (in_fragmented_record) {
                    // 文件在 First/Middle 后意外截断（如写 WAL 时进程崩溃）。
                    ReportCorruption(scratch->size(), "partial record without end(3)");
                    scratch->clear();
                }
                return false;

            case kBadRecord:
                if (in_fragmented_record) {
                    ReportCorruption(scratch->size(), "error in middle of record");
                    in_fragmented_record = false;
                    scratch->clear();
                }
                break;

            default:
                ReportCorruption(
                    static_cast<uint64_t>(scratch->size() + fragment.size()),
                    "unknown record type");
                in_fragmented_record = false;
                scratch->clear();
                break;
        }
    }
}

// ReadPhysicalRecord 从文件中读取并解析一个物理 Chunk 的 Header（7 字节），
// 可选地校验 CRC，然后返回 Chunk 类型和 payload 的 Slice 视图。
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
    while (true) {
        if (buffer_.size() < static_cast<size_t>(kHeaderSize)) {
            if (!eof_) {
                buffer_.clear();
                Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
                if (!status.ok()) {
                    ReportDrop(kBlockSize, status);
                    eof_ = true;
                    return kEof;
                } else if (buffer_.size() < static_cast<size_t>(kBlockSize)) {
                    eof_ = true;
                }
                continue;
            } else {
                return kEof;
            }
        }

        const char*  header = buffer_.data();
        const uint32_t a    = static_cast<uint8_t>(header[4]);
        const uint32_t b    = static_cast<uint8_t>(header[5]);
        const unsigned int type   = static_cast<uint8_t>(header[6]);
        const uint32_t length     = a | (b << 8);

        if (static_cast<size_t>(kHeaderSize) + length > buffer_.size()) {
            size_t drop_size = buffer_.size();
            buffer_.clear();
            if (!eof_) {
                ReportCorruption(drop_size, "bad record length");
                return kBadRecord;
            }
            return kEof;
        }

        // 零填充区（Block 尾部对齐字节），直接跳过。
        if (type == kZeroType && length == 0) {
            buffer_.clear();
            return kBadRecord;
        }

        // 校验 CRC（覆盖 type 字节 + payload），防止静默数据损坏。
        if (checksum_) {
            uint32_t expected_crc =
                static_cast<uint8_t>(header[0])        |
                (static_cast<uint8_t>(header[1]) <<  8) |
                (static_cast<uint8_t>(header[2]) << 16) |
                (static_cast<uint8_t>(header[3]) << 24);
            expected_crc        = crc32c::Unmask(expected_crc);
            uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
            if (actual_crc != expected_crc) {
                size_t drop_size = buffer_.size();
                buffer_.clear();
                ReportCorruption(drop_size, "checksum mismatch");
                return kBadRecord;
            }
        }

        buffer_.remove_prefix(kHeaderSize + length);

        if (type < kZeroType || type > static_cast<unsigned int>(kMaxRecordType)) {
            ReportCorruption(length, "unrecognized record type");
            return kBadRecord;
        }

        *result = Slice(header + kHeaderSize, length);
        return type;
    }
}

} // namespace log
} // namespace minidb
