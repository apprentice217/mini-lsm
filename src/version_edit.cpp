#include "version_edit.h"
#include "coding.h"

namespace minidb {

void VersionEdit::Clear() {
    has_log_number_       = false;
    has_next_file_number_ = false;
    has_last_sequence_    = false;
    deleted_files_.clear();
    new_files_.clear();
}

void VersionEdit::AddFile(int level, uint64_t file, uint64_t file_size,
                          const Slice& smallest, const Slice& largest) {
    FileMetaData f;
    f.number    = file;
    f.file_size = file_size;
    f.smallest  = smallest.ToString();
    f.largest   = largest.ToString();
    new_files_.push_back(std::make_pair(level, f));
}

void VersionEdit::DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
}

// EncodeTo 将 VersionEdit 序列化为 TLV（Tag-Length-Value）格式的字节流，
// 追加到 dst，用于写入 MANIFEST 日志。
void VersionEdit::EncodeTo(std::string* dst) const {
    if (has_log_number_) {
        PutVarint32(dst, kLogNumber);
        PutVarint64(dst, log_number_);
    }
    if (has_next_file_number_) {
        PutVarint32(dst, kNextFileNumber);
        PutVarint64(dst, next_file_number_);
    }
    if (has_last_sequence_) {
        PutVarint32(dst, kLastSequence);
        PutVarint64(dst, last_sequence_);
    }
    for (const auto& del : deleted_files_) {
        PutVarint32(dst, kDeletedFile);
        PutVarint32(dst, static_cast<uint32_t>(del.first));
        PutVarint64(dst, del.second);
    }
    for (const auto& nf : new_files_) {
        const FileMetaData& f = nf.second;
        PutVarint32(dst, kNewFile);
        PutVarint32(dst, static_cast<uint32_t>(nf.first));
        PutVarint64(dst, f.number);
        PutVarint64(dst, f.file_size);
        PutVarint32(dst, static_cast<uint32_t>(f.smallest.size()));
        dst->append(f.smallest.data(), f.smallest.size());
        PutVarint32(dst, static_cast<uint32_t>(f.largest.size()));
        dst->append(f.largest.data(), f.largest.size());
    }
}

// DecodeFrom 从 MANIFEST 日志记录中解析 VersionEdit。
// 未知 Tag 静默跳过，以保证向前兼容（旧版本代码可读取新版本生成的 MANIFEST）。
Status VersionEdit::DecodeFrom(const Slice& src) {
    Clear();
    const char* p     = src.data();
    const char* limit = p + src.size();
    uint32_t tag, level;
    uint64_t number, size;

    while (p < limit) {
        if (!GetVarint32(&p, limit, &tag)) break;
        switch (tag) {
            case kLogNumber:
                if (GetVarint64(&p, limit, &log_number_)) has_log_number_ = true;
                break;
            case kNextFileNumber:
                if (GetVarint64(&p, limit, &next_file_number_)) has_next_file_number_ = true;
                break;
            case kLastSequence:
                if (GetVarint64(&p, limit, &last_sequence_)) has_last_sequence_ = true;
                break;
            case kDeletedFile:
                if (GetVarint32(&p, limit, &level) && GetVarint64(&p, limit, &number)) {
                    deleted_files_.insert(std::make_pair(static_cast<int>(level), number));
                }
                break;
            case kNewFile:
                if (GetVarint32(&p, limit, &level) &&
                    GetVarint64(&p, limit, &number) &&
                    GetVarint64(&p, limit, &size)) {
                    uint32_t sm_len, lg_len;
                    if (GetVarint32(&p, limit, &sm_len) && p + sm_len <= limit) {
                        Slice smallest(p, sm_len);
                        p += sm_len;
                        if (GetVarint32(&p, limit, &lg_len) && p + lg_len <= limit) {
                            Slice largest(p, lg_len);
                            p += lg_len;
                            AddFile(static_cast<int>(level), number, size, smallest, largest);
                        }
                    }
                }
                break;
            default:
                break; // 未知 Tag，向前兼容忽略
        }
    }

    if (p != limit) return Status::Corruption("VersionEdit decode failed");
    return Status::OK();
}

} // namespace minidb
