#include "version_set.h"
#include "table_cache.h"
#include "comparator.h"
#include "log_writer.h"
#include "log_reader.h"
#include "env.h"
#include "db_format.h"
#include <filesystem>
#include <memory>
#include <system_error>
#include <fstream>
#include <cstdio>
#include <set>
#include <algorithm>
#include <cassert>
#include <limits>

namespace minidb {

namespace {

// 即使 Reader 没有 status() 接口，也能通过 Reporter 获取读取错误。
class ManifestReporter final : public log::Reader::Reporter {
public:
    void Corruption(size_t, const Status& s) override {
        if (status.ok()) status = s;
    }
    Status status;
};

Status ValidateFileMetadata(int level, const FileMetaData& file,
                            const Comparator* comparator) {
    if (level < 0 || level >= kNumLevels) {
        return Status::Corruption("Invalid file level");
    }
    if (file.number == 0 || file.file_size == 0) {
        return Status::Corruption("Invalid SST file metadata");
    }
    if (file.smallest.size() < 8 || file.largest.size() < 8) {
        return Status::Corruption("Invalid internal key in file metadata");
    }
    if (comparator->Compare(Slice(file.smallest), Slice(file.largest)) > 0) {
        return Status::Corruption("Reversed SST key range");
    }
    return Status::OK();
}

} // namespace

// 先写入并同步临时文件，再通过 rename 替换 CURRENT。
// 这里尚未同步目录，不能据此宣称整条路径已经具备断电持久性。
static Status SetCurrentFile(const std::string& dbname,
                             uint64_t descriptor_number) {
    char name[64];
    std::snprintf(name, sizeof(name), "MANIFEST-%06llu",
                  static_cast<unsigned long long>(descriptor_number));
    const std::string contents = std::string(name) + "\n";
    const std::string tmp = dbname + "/CURRENT.tmp";
    const std::string current = dbname + "/CURRENT";

    WritableFile* raw_file = nullptr;
    Status s = NewWritableFile(tmp, &raw_file);
    if (!s.ok()) return s;
    std::unique_ptr<WritableFile> file(raw_file);

    s = file->Append(Slice(contents));
    if (s.ok()) s = file->Sync();
    const Status close_status = file->Close();
    if (s.ok()) s = close_status;
    if (!s.ok()) return s;

    if (std::rename(tmp.c_str(), current.c_str()) != 0) {
        return Status::IOError("Cannot rename CURRENT.tmp to CURRENT");
    }
    return Status::OK();
}

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache, const Comparator* cmp)
    : dbname_(dbname),
      options_(*options),
      table_cache_(table_cache),
      icmp_(cmp),
      next_file_number_(2),      // 1 号预留给 MANIFEST
      manifest_file_number_(1),
      last_sequence_(0),
      log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr) {
    current_ = std::make_shared<Version>(this);
}

VersionSet::~VersionSet() {
    delete descriptor_log_;
    delete descriptor_file_;
}

// LogAndApply 是版本演进的核心：
// 1. 在内存中以 Copy-on-Write 方式生成新 Version；
// 2. 将本次变更（VersionEdit）序列化追加到 MANIFEST；
// 3. 刷盘确认后才切换 current_ 指针，保证持久化先于内存可见。
Status VersionSet::LogAndApply(VersionEdit* edit) {
    // VersionEdit 中的 log_number 是可选字段：
    // 若调用方未显式设置，说明本次变更不打算修改恢复起点。
    // 这里只把当前已生效的 log_number_ 补进这个待提交的 edit，
    // 使其写入 MANIFEST 后仍沿用原值。
    // 注意：这里只修改 edit，不修改 VersionSet 当前已生效的状态。
    if (edit == nullptr) {
        return Status::InvalidArgument("Null VersionEdit");
    }
    if (edit->has_log_number_ && edit->log_number_ < log_number_) {
        return Status::InvalidArgument("WAL recovery number cannot decrease");
    }
    constexpr uint64_t kMaxSequence = (uint64_t{1} << 56) - 1;
    if (last_sequence_ > kMaxSequence) {
        return Status::Corruption("Sequence number exceeds 56 bits");
    }
    for (const auto& del : edit->deleted_files_) {
        if (del.first < 0 || del.first >= kNumLevels) {
            return Status::Corruption("Invalid deleted-file level");
        }
    }
    for (const auto& added : edit->new_files_) {
        Status check = ValidateFileMetadata(added.first, added.second, icmp_);
        if (!check.ok()) return check;
    }

    if(!edit->has_log_number_) {
        edit->SetLogNumber(log_number_);
    }
    const bool create_manifest = descriptor_log_ == nullptr;
    uint64_t new_manifest_file_number = manifest_file_number_;
    std::string new_manifest_file_path;

    if(create_manifest) {
        for(;;) {
            if (next_file_number_ == std::numeric_limits<uint64_t>::max()) {
                return Status::IOError("File number exhausted");
            }
            new_manifest_file_number = NextFileNumber();

            char name[64];
            std::snprintf(name, sizeof(name), "MANIFEST-%06llu",
                          static_cast<unsigned long long>(new_manifest_file_number));
                          
            new_manifest_file_path = dbname_ + "/" + name;

            std::error_code ec;
            const bool exists = std::filesystem::exists(new_manifest_file_path, ec);
            if(ec) {
                return Status::IOError("Cannot check MANIFEST path ");
            }
            if (!exists) 
                break;
        }
    }

    edit->SetNextFile(next_file_number_);
    edit->SetLastSequence(last_sequence_);

    // 以当前 current_ 为基础，叠加 edit 中的文件增删，构建新 Version。
    std::shared_ptr<Version> v = std::make_shared<Version>(this);
    if (current_ != nullptr) {
        for (int i = 0; i < kNumLevels; i++) {
            v->files_[i] = current_->files_[i];
        }
    }

    // 先执行删除，再执行新增，保证被替换的旧文件不出现在新 Version 中。
    for (const auto& del_pair : edit->deleted_files_) {
        int level       = del_pair.first;
        uint64_t number = del_pair.second;
        auto& vec = v->files_[level];
        vec.erase(std::remove_if(vec.begin(), vec.end(),
                                 [number](const std::shared_ptr<FileMetaData>& f) {
                                     return f->number == number;
                                 }),
                  vec.end());
    }
    for (const auto& new_file_pair : edit->new_files_) {
        int level = new_file_pair.first;
        auto& files = v->files_[level];
        const uint64_t number = new_file_pair.second.number;
        files.erase(std::remove_if(files.begin(), files.end(),
                                  [number](const std::shared_ptr<FileMetaData>& f) {
                                      return f->number == number;
                                  }), files.end());
        files.push_back(std::make_shared<FileMetaData>(new_file_pair.second));
    }

    // L1+ 层文件之间不允许 key 范围重叠，且 Version::Get 的二分查找依赖 smallest key 有序。
    // 每次修改文件列表后重新排序（compaction 操作频率低，排序开销可忽略）。
    for (int i = 1; i < kNumLevels; ++i) {
        std::sort(v->files_[i].begin(), v->files_[i].end(),
                  [this](const std::shared_ptr<FileMetaData>& a,
                         const std::shared_ptr<FileMetaData>& b) {
                      return icmp_->Compare(Slice(a->smallest), Slice(b->smallest)) < 0;
                  });
    }

    // 将 VersionEdit 序列化后追加到 MANIFEST 日志。
    Status s;

    if(create_manifest) {
        VersionEdit snapshot;
        snapshot.SetLogNumber(edit->log_number_);
        snapshot.SetNextFile(next_file_number_);
        snapshot.SetLastSequence(last_sequence_);

        for(int level = 0; level < kNumLevels; ++level) {
            for(const auto& file : v->files_[level]) {
                snapshot.AddFile(
                    level, 
                    file->number, 
                    file->file_size,
                    Slice(file->smallest), 
                    Slice(file->largest));
            }
        }

        std::string record;
        snapshot.EncodeTo(&record);

        WritableFile* raw_file = nullptr;
        s=NewWritableFile(new_manifest_file_path, &raw_file);
        if(!s.ok()) return s;

        std::unique_ptr<WritableFile> new_file(raw_file);
        auto new_log = std::make_unique<log::Writer>(new_file.get());

        s=new_log->AddRecord(Slice(record));

        if(s.ok()) {
            s=new_file->Sync();
        }
        if(s.ok()) {
            s=SetCurrentFile(dbname_, new_manifest_file_number);
        }
        if(!s.ok()) return s;

        // 新 MANIFEST 发布成功后，才接管这些对象。
        descriptor_file_ = new_file.release();
        descriptor_log_ = new_log.release();
        manifest_file_number_ = new_manifest_file_number;
    } else {
         // 已有完整基础状态，后续只追加本次增量。
        std::string record;
        edit->EncodeTo(&record);

        s = descriptor_log_->AddRecord(Slice(record));
        if (s.ok()) {
            s = descriptor_file_->Sync();
        }
        if (!s.ok()) {
            return s;
        }
    }

    assert(edit->has_log_number_);
    assert(edit->log_number_ >= log_number_);

    current_ = v;
    log_number_ = edit->log_number_;

    return Status::OK();
}

// Recover 从 CURRENT -> MANIFEST 链重建版本集合，恢复上次关闭时的文件视图。
Status VersionSet::Recover(bool* save_manifest) {
    if (save_manifest == nullptr) {
        return Status::InvalidArgument("Null save_manifest argument");
    }
    *save_manifest = false;
    if (descriptor_log_ != nullptr) {
        return Status::InvalidArgument("Cannot recover an active VersionSet");
    }

    const std::string current_path = dbname_ + "/CURRENT";
    std::error_code ec;
    const bool exists = std::filesystem::exists(current_path, ec);
    if (ec) return Status::IOError("Cannot check CURRENT file");
    if (!exists) return Status::NotFound("CURRENT file does not exist");

    std::ifstream in(current_path);
    if (!in.is_open()) return Status::IOError("Cannot open CURRENT file");

    std::string manifest_filename;
    in >> manifest_filename;
    if (in.bad()) return Status::IOError("Cannot read CURRENT file");
    if (manifest_filename.empty()) {
        return Status::Corruption("CURRENT file is empty");
    }
    // CURRENT 必须只包含本目录中的 MANIFEST 文件名。
    const std::string prefix = "MANIFEST-";
    if (manifest_filename.compare(0, prefix.size(), prefix) != 0 ||
        manifest_filename.size() == prefix.size() ||
        manifest_filename.find_first_not_of("0123456789", prefix.size()) !=
            std::string::npos) {
        return Status::Corruption("Invalid MANIFEST name in CURRENT");
    }
    std::string extra;
    if (in >> extra) return Status::Corruption("Extra content in CURRENT");
    if (in.bad()) return Status::IOError("Cannot read CURRENT file");

    SequentialFile* raw_file = nullptr;
    Status s = NewSequentialFile(dbname_ + "/" + manifest_filename, &raw_file);
    if (!s.ok()) return s;
    std::unique_ptr<SequentialFile> manifest_file(raw_file);
    ManifestReporter reporter;
    log::Reader reader(manifest_file.get(), &reporter, true, 0);

    auto v = std::make_shared<Version>(this);
    uint64_t recovered_log = 0;
    uint64_t recovered_next = 2;
    uint64_t recovered_sequence = 0;
    uint64_t largest_file_number = 0;
    bool has_log = false;
    bool has_next = false;
    bool has_sequence = false;
    constexpr uint64_t kMaxSequence = (uint64_t{1} << 56) - 1;

    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch)) {
        // 旧版 Reader 可能跳过坏记录后返回下一条；必须先检查 Reporter。
        if (!reporter.status.ok()) return reporter.status;
        VersionEdit edit;
        s = edit.DecodeFrom(record);
        if (!s.ok()) return s;

        if (edit.has_log_number_) {
            if (has_log && edit.log_number_ < recovered_log) {
                return Status::Corruption("Decreasing WAL number in MANIFEST");
            }
            recovered_log = edit.log_number_;
            has_log = true;
        }
        if (edit.has_next_file_number_) {
            if (has_next && edit.next_file_number_ < recovered_next) {
                return Status::Corruption("Decreasing next file number");
            }
            recovered_next = edit.next_file_number_;
            has_next = true;
        }
        if (edit.has_last_sequence_) {
            if (edit.last_sequence_ > kMaxSequence ||
                (has_sequence && edit.last_sequence_ < recovered_sequence)) {
                return Status::Corruption("Invalid sequence in MANIFEST");
            }
            recovered_sequence = edit.last_sequence_;
            has_sequence = true;
        }

        for (const auto& del : edit.deleted_files_) {
            const int level = del.first;
            const uint64_t number = del.second;
            if (level < 0 || level >= kNumLevels) {
                return Status::Corruption("Invalid deleted-file level");
            }
            largest_file_number = std::max(largest_file_number, number);
            auto& files = v->files_[level];
            files.erase(std::remove_if(files.begin(), files.end(),
                                       [number](const std::shared_ptr<FileMetaData>& f) {
                                           return f->number == number;
                                       }), files.end());
        }
        for (const auto& added : edit.new_files_) {
            s = ValidateFileMetadata(added.first, added.second, icmp_);
            if (!s.ok()) return s;
            const auto& metadata = added.second;
            largest_file_number = std::max(largest_file_number, metadata.number);
            auto& files = v->files_[added.first];
            files.erase(std::remove_if(files.begin(), files.end(),
                                       [&metadata](const std::shared_ptr<FileMetaData>& f) {
                                           return f->number == metadata.number;
                                       }), files.end());
            files.push_back(std::make_shared<FileMetaData>(metadata));
        }
    }
    if (!reporter.status.ok()) return reporter.status;
    if (!has_log || !has_next || !has_sequence) {
        return Status::Corruption("MANIFEST lacks required metadata");
    }
    if (recovered_next < 2 || recovered_next <= recovered_log ||
        recovered_next <= largest_file_number) {
        return Status::Corruption("Invalid next file number in MANIFEST");
    }

    for (int level = 1; level < kNumLevels; ++level) {
        auto& files = v->files_[level];
        std::sort(files.begin(), files.end(),
                  [this](const std::shared_ptr<FileMetaData>& a,
                         const std::shared_ptr<FileMetaData>& b) {
                      return icmp_->Compare(Slice(a->smallest), Slice(b->smallest)) < 0;
                  });
    }

    // 全部恢复成功后，才发布文件视图和标量元数据。
    current_ = v;
    log_number_ = recovered_log;
    next_file_number_ = recovered_next;
    last_sequence_ = recovered_sequence;
    *save_manifest = true;
    return Status::OK();
}

// --------------------------------------------------------------------------
// Compaction selection
// --------------------------------------------------------------------------

int64_t VersionSet::MaxBytesForLevel(int level) const {
    // L1 = max_bytes_for_level_base，每向上一级乘以 10。
    int64_t result = options_.max_bytes_for_level_base;
    while (level > 1) {
        result *= 10;
        --level;
    }
    return result;
}

int64_t VersionSet::TotalFileSize(int level) const {
    int64_t total = 0;
    if (current_ == nullptr) return 0;
    for (const auto& f : current_->files_[level]) {
        total += static_cast<int64_t>(f->file_size);
    }
    return total;
}

bool VersionSet::NeedsCompaction() const {
    if (current_ == nullptr) return false;
    // L0：文件数超阈值。
    if (static_cast<int>(current_->files_[0].size()) >= options_.l0_compaction_trigger) {
        return true;
    }
    // L1-L6：总字节数超阈值。
    for (int level = 1; level < kNumLevels - 1; ++level) {
        if (TotalFileSize(level) > MaxBytesForLevel(level)) {
            return true;
        }
    }
    return false;
}

// GetOverlappingInputs 在 level 层中收集与 [smallest, largest] key 范围重叠的文件。
// L1+ 层文件之间不重叠且已按 smallest key 排序，可做二分剪枝；
// L0 层文件可能重叠，需全量扫描。
void VersionSet::GetOverlappingInputs(int level,
                                      const std::string& smallest,
                                      const std::string& largest,
                                      std::vector<std::shared_ptr<FileMetaData>>* inputs) {
    inputs->clear();
    if (current_ == nullptr) return;

    Slice small_slice(smallest);
    Slice large_slice(largest);

    for (const auto& f : current_->files_[level]) {
        // f 与 [smallest, largest] 无重叠的条件：f.largest < smallest 或 f.smallest > largest
        if (icmp_->Compare(Slice(f->largest), small_slice) < 0 ||
            icmp_->Compare(Slice(f->smallest), large_slice) > 0) {
            continue;
        }
        inputs->push_back(f);
    }
}

// PickCompaction 从 current_ 中选出最需要压缩的层和文件。
// 优先级：L0 > L1 > ... > L5（按触发条件检查）。
// 在同一层内，使用 compaction_pointer_ 实现轮转选择，避免反复压缩同一批文件。
Compaction* VersionSet::PickCompaction() {
    if (current_ == nullptr) return nullptr;

    int level = -1;

    // 优先检查 L0：文件数过多会拖慢读性能（需要检查每个文件的 Bloom Filter）。
    if (static_cast<int>(current_->files_[0].size()) >= options_.l0_compaction_trigger) {
        level = 0;
    } else {
        // 检查 L1-L5 是否有层的总字节数超出阈值。
        for (int l = 1; l < kNumLevels - 1; ++l) {
            if (TotalFileSize(l) > MaxBytesForLevel(l)) {
                level = l;
                break;
            }
        }
    }

    if (level < 0) return nullptr;

    Compaction* c = new Compaction();
    c->input_level = level;

    // 在 level 层中选一个起点文件（轮转策略：从上次 compaction 结束的 key 之后开始）。
    const std::string& pointer = compaction_pointer_[level];
    std::shared_ptr<FileMetaData> picked;

    if (!pointer.empty()) {
        // 找到第一个 largest >= pointer 的文件。
        for (const auto& f : current_->files_[level]) {
            if (icmp_->Compare(Slice(f->largest), Slice(pointer)) >= 0) {
                picked = f;
                break;
            }
        }
    }
    if (!picked && !current_->files_[level].empty()) {
        picked = current_->files_[level].front();
    }
    if (!picked) {
        delete c;
        return nullptr;
    }
    c->inputs[0].push_back(picked);

    // L0 层文件之间可能 key 范围重叠，若选中一个文件后有其他 L0 文件与之重叠，
    // 必须一并纳入 inputs[0]，否则合并结果会破坏 L1 的有序性。
    if (level == 0) {
        std::string cur_small = picked->smallest;
        std::string cur_large = picked->largest;
        bool expanded = true;
        while (expanded) {
            expanded = false;
            for (const auto& f : current_->files_[level]) {
                // 已在列表中则跳过。
                bool already_in = false;
                for (const auto& fin : c->inputs[0]) {
                    if (fin->number == f->number) { already_in = true; break; }
                }
                if (already_in) continue;

                if (icmp_->Compare(Slice(f->largest),  Slice(cur_small)) >= 0 &&
                    icmp_->Compare(Slice(f->smallest), Slice(cur_large)) <= 0) {
                    c->inputs[0].push_back(f);
                    // 更新合并后的边界，可能触发更多文件加入。
                    if (icmp_->Compare(Slice(f->smallest), Slice(cur_small)) < 0)
                        cur_small = f->smallest;
                    if (icmp_->Compare(Slice(f->largest),  Slice(cur_large)) > 0)
                        cur_large = f->largest;
                    expanded = true;
                }
            }
        }
        // 计算完整的合并范围后再收集 level+1 层的重叠文件。
        GetOverlappingInputs(level + 1, cur_small, cur_large, &c->inputs[1]);
    } else {
        GetOverlappingInputs(level + 1, picked->smallest, picked->largest, &c->inputs[1]);
    }

    return c;
}

// --------------------------------------------------------------------------
// Version::Get
// --------------------------------------------------------------------------

// Version::Get 按层级顺序查找 key（InternalKey 格式）：
//   L0：逆序扫描（最新文件优先），因为 L0 文件间 key 范围可能重叠；
//   L1-L6：二分定位后单文件查询（同层文件不重叠且已有序）。
Status Version::Get(const ReadOptions& options, const Slice& k, std::string* value) {
    // 范围判断必须用 user_comparator 比较 user_key，而不是 InternalKeyComparator
    // 比较整段 InternalKey。后者会按 (user_key ASC, seq DESC) 排序，
    // 当 lookup 的 snapshot_seq 大于文件 boundary 的 seq 时，
    // 同 user_key 下 lookup 反而被判定为小于 smallest，导致正确包含 key 的
    // SST 被跳过 → 表面上是已经写入的 key 读不到 (BUG-001)。
    // icmp_ 在 VersionSet 里被声明为 const Comparator* 但运行期一定是 InternalKeyComparator
    // （由 DBImpl 构造时传入），这里向下转换以拿到底层 user_comparator。
    const InternalKeyComparator* ikcmp =
        static_cast<const InternalKeyComparator*>(vset_->icmp_);
    const Comparator* ucmp = ikcmp->user_comparator();
    const Slice user_k = ExtractUserKey(k);

    // --- L0：逆序扫描 ---
    const auto& level_0_files = files_[0];
    for (auto it = level_0_files.rbegin(); it != level_0_files.rend(); ++it) {
        const std::shared_ptr<FileMetaData>& f = *it;
        const Slice user_smallest = ExtractUserKey(Slice(f->smallest));
        const Slice user_largest  = ExtractUserKey(Slice(f->largest));
        if (ucmp->Compare(user_k, user_smallest) >= 0 &&
            ucmp->Compare(user_k, user_largest)  <= 0) {
            Status s = vset_->table_cache_->Get(options, f->number, f->file_size, k, value);
            if (s.ok()) return s;
            if (!s.IsNotFound()) return s; // I/O 错误，立即上报
        }
    }

    // --- L1-L6：二分定位 + 单文件查询 ---
    for (int level = 1; level < kNumLevels; ++level) {
        const auto& files = files_[level];
        if (files.empty()) continue;

        // L1+ 层文件按 user_key 有序且不重叠，二分找到第一个 largest_user >= user_k 的文件。
        size_t lo = 0, hi = files.size();
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            const Slice mid_largest_user = ExtractUserKey(Slice(files[mid]->largest));
            if (ucmp->Compare(mid_largest_user, user_k) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if (lo < files.size()) {
            const std::shared_ptr<FileMetaData>& f = files[lo];
            const Slice user_smallest = ExtractUserKey(Slice(f->smallest));
            // 还需确认 user_k >= f.smallest_user，否则 k 落在两个文件的间隙中。
            if (ucmp->Compare(user_k, user_smallest) >= 0) {
                Status s = vset_->table_cache_->Get(options, f->number, f->file_size, k, value);
                if (s.ok()) return s;
                if (!s.IsNotFound()) return s;
            }
        }
    }

    return Status::NotFound("Key not found in Version");
}

} // namespace minidb
