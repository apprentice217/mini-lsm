#pragma once

#include <string>
#include <vector>
#include <memory>
#include "status.h"
#include "slice.h"
#include "version_edit.h"
#include "options.h"

namespace minidb {

class TableCache;
class Comparator;
class VersionSet;
class WritableFile;
namespace log { class Writer; }

constexpr int kNumLevels = 7;

// Version 是某一时刻数据库文件集合的不可变快照。
// 读请求持有 Version 的 shared_ptr，即使发生 compaction 也不会影响正在进行的查询。
class Version {
public:
    explicit Version(VersionSet* vset) : vset_(vset) {}
    ~Version() = default;

    Version(const Version&) = delete;
    Version& operator=(const Version&) = delete;

    // 在当前版本所有 Level 的文件中查找 key（InternalKey 格式）。
    // L0 逆序扫描（文件间可能重叠）；L1+ 二分定位后单文件查询。
    Status Get(const ReadOptions& options, const Slice& k, std::string* value);

    // 返回指定层级的文件列表（只读）。
    const std::vector<std::shared_ptr<FileMetaData>>& files(int level) const {
        return files_[level];
    }

private:
    friend class VersionSet;

    VersionSet* vset_;
    // files_[level] 存储该 level 所有 SSTable 的元数据。
    // 使用 shared_ptr 使多个 Version 可以安全共享同一批文件元数据。
    std::vector<std::shared_ptr<FileMetaData>> files_[kNumLevels];
};

// Compaction 描述一次归并压缩任务：从 input_level 层选取的输入文件，以及目标层。
struct Compaction {
    int input_level;  // 源层（如 0，则目标为 1）
    // inputs[0]: 选自 input_level 的文件；inputs[1]: 选自 input_level+1 的重叠文件
    std::vector<std::shared_ptr<FileMetaData>> inputs[2];
    // 本次压缩完成后要写入 MANIFEST 的变更（由 DBImpl 填充输出文件后调用 LogAndApply）
    VersionEdit edit;
};

// VersionSet 管理版本演进，是 MANIFEST 文件的唯一写入者。
// 每次 flush/compaction 通过 LogAndApply 提交一个 VersionEdit，
// 生成新 Version 并将增量持久化到 MANIFEST，确保崩溃后可完整恢复。
class VersionSet {
public:
    VersionSet(const std::string& dbname, const Options* options,
               TableCache* table_cache, const Comparator* cmp);
    ~VersionSet();

    VersionSet(const VersionSet&) = delete;
    VersionSet& operator=(const VersionSet&) = delete;

    uint64_t NextFileNumber() { return next_file_number_++; }
    uint64_t log_number()     const { return log_number_; }
    uint64_t last_sequence()  const { return last_sequence_; }
    void     set_last_sequence(uint64_t s) { last_sequence_ = s; }

    // Recover 读取 CURRENT -> MANIFEST 链，重建上次关闭时的版本状态。
    Status Recover(bool* save_manifest);

    // LogAndApply 以 Copy-on-Write 方式创建新 Version，将 VersionEdit 追加到 MANIFEST，
    // fsync 确认后切换 current_。
    Status LogAndApply(VersionEdit* edit);

    std::shared_ptr<Version> current() const { return current_; }

    // PickCompaction 根据当前各层文件状况选取一次 compaction 任务。
    // 若不需要 compaction 则返回 nullptr。
    // 触发条件：L0 文件数 >= l0_compaction_trigger，或 L1+ 总字节数超出阈值。
    Compaction* PickCompaction();

    // 判断当前是否需要触发 compaction（用于调度决策）。
    bool NeedsCompaction() const;

    // 返回指定层级允许的最大总字节数（L1 = max_bytes_for_level_base, 每级 ×10）。
    int64_t MaxBytesForLevel(int level) const;

    // 返回指定层级所有文件的总字节数。
    int64_t TotalFileSize(int level) const;

private:
    friend class Version;

    // 在 level+1 层中找出与 [smallest, largest] 范围重叠的所有文件。
    void GetOverlappingInputs(int level,
                              const std::string& smallest,
                              const std::string& largest,
                              std::vector<std::shared_ptr<FileMetaData>>* inputs);

    std::string       dbname_;
    const Options*    options_;
    TableCache*       table_cache_;
    const Comparator* icmp_;

    uint64_t next_file_number_;
    uint64_t manifest_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;

    std::shared_ptr<Version> current_;

    WritableFile*  descriptor_file_;
    log::Writer*   descriptor_log_;

    // compaction_level_[i] 记录上次在第 i 层执行 compaction 结束时的 largest key，
    // 用于轮转选择文件，避免反复压缩同一批文件。
    std::string compaction_pointer_[kNumLevels];
};

} // namespace minidb
