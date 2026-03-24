#pragma once

#include <set>
#include <vector>
#include <string>
#include "slice.h"
#include "status.h"

namespace minidb {

// SSTable 文件的元数据
struct FileMetaData {
    int refs;             // 引用计数
    int allowed_seeks;    // 允许的查询 miss 次数（用于触发合并）
    uint64_t number;      // 文件编号 (如 000005.sst 中的 5)
    uint64_t file_size;   // 物理文件大小
    std::string smallest; // 文件中包含的最小 InternalKey
    std::string largest;  // 文件中包含的最大 InternalKey

    FileMetaData() : refs(0), allowed_seeks(1 << 30), number(0), file_size(0) {}
};

class VersionEdit {
public:
    VersionEdit() { Clear(); }
    ~VersionEdit() = default;

    void Clear();

    // 修改全局元数据
    void SetLogNumber(uint64_t num) { has_log_number_ = true; log_number_ = num; }
    void SetNextFile(uint64_t num) { has_next_file_number_ = true; next_file_number_ = num; }
    void SetLastSequence(uint64_t seq) { has_last_sequence_ = true; last_sequence_ = seq; }

    // 记录由于 Compaction 导致的文件增删
    void AddFile(int level, uint64_t file, uint64_t file_size, const Slice& smallest, const Slice& largest);
    void DeleteFile(int level, uint64_t file);

    // 序列化与反序列化，与 MANIFEST 文件交互
    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(const Slice& src);

private:
    friend class VersionSet;

    // 增量标签 (Tags) 定义，用于可扩展的 TLV 序列化协议
    enum Tag {
        kLogNumber        = 2,
        kNextFileNumber   = 3,
        kLastSequence     = 4,
        kDeletedFile      = 6,
        kNewFile          = 7
    };

    bool has_log_number_;
    uint64_t log_number_; // 当前正在使用的 WAL 编号

    bool has_next_file_number_;
    uint64_t next_file_number_; // 用于分配给下一个生成的 .sst 或 .log 的编号

    bool has_last_sequence_;
    uint64_t last_sequence_; // 全局逻辑时钟水位

    // 记录哪些文件被删除了：对 <Level层级, 文件编号>
    typedef std::pair<int, uint64_t> DeletedFileSet;
    std::set<DeletedFileSet> deleted_files_;

    // 记录哪些新文件被加入了：对 <Level层级, 文件元数据>
    std::vector<std::pair<int, FileMetaData>> new_files_;
};

} // namespace minidb