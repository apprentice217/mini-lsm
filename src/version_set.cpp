#include "version_set.h"
#include "table_cache.h"
#include "comparator.h"
#include "log_writer.h"
#include "log_reader.h"
#include "env.h"
#include "db_format.h"
#include <fstream>
#include <cstdio>
#include <set>
#include <algorithm>

namespace minidb {

// CURRENT 文件记录当前生效的 MANIFEST 文件名（如 "MANIFEST-000001"）。
// 使用 write-then-rename 原子更新，防止断电时 CURRENT 损坏。
static Status SetCurrentFile(const std::string& dbname, uint64_t descriptor_number) {
    char buf[100];
    std::snprintf(buf, sizeof(buf), "MANIFEST-%06llu",
                  (unsigned long long)descriptor_number);
    std::string manifest_filename = buf;

    std::string tmp     = dbname + "/CURRENT.tmp";
    std::string current = dbname + "/CURRENT";

    std::ofstream out(tmp, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        return Status::IOError("Cannot create CURRENT.tmp");
    }
    out << manifest_filename << "\n";
    out.close();

    // rename 是 POSIX 保证的原子操作，写完临时文件再 rename 可避免 CURRENT 写到一半崩溃。
    if (std::rename(tmp.c_str(), current.c_str()) != 0) {
        return Status::IOError("Cannot rename CURRENT.tmp to CURRENT");
    }
    return Status::OK();
}

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache, const Comparator* cmp)
    : dbname_(dbname),
      options_(options),
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
    // 补全 edit 中缺失的全局水位信息，确保 MANIFEST 记录完整。
    if (edit->has_log_number_) {
        log_number_ = edit->log_number_;
    } else {
        edit->SetLogNumber(log_number_);
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
        v->files_[level].push_back(std::make_shared<FileMetaData>(new_file_pair.second));
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
    std::string record;
    edit->EncodeTo(&record);

    Status s;
    if (descriptor_log_ == nullptr) {
        char buf[100];
        std::snprintf(buf, sizeof(buf), "%s/MANIFEST-%06llu",
                      dbname_.c_str(), (unsigned long long)manifest_file_number_);
        s = NewWritableFile(buf, &descriptor_file_);
        if (!s.ok()) return s;
        descriptor_log_ = new log::Writer(descriptor_file_);
    }

    s = descriptor_log_->AddRecord(record);
    if (s.ok()) {
        // fsync 确保 MANIFEST 记录在崩溃时不丢失。
        s = descriptor_file_->Sync();
    }
    if (s.ok()) {
        s = SetCurrentFile(dbname_, manifest_file_number_);
    }

    // 只有磁盘确认成功后，才将新 Version 设为 current_。
    if (s.ok()) {
        current_ = v;
    }

    return s;
}

// Recover 从 CURRENT -> MANIFEST 链重建版本集合，恢复上次关闭时的文件视图。
Status VersionSet::Recover(bool* save_manifest) {
    std::string current_path = dbname_ + "/CURRENT";
    std::ifstream in(current_path);
    if (!in.is_open()) {
        return Status::NotFound("CURRENT file does not exist. (First boot)");
    }

    std::string manifest_filename;
    in >> manifest_filename;
    if (manifest_filename.empty()) {
        return Status::Corruption("CURRENT file is empty");
    }
    if (!manifest_filename.empty() && manifest_filename.back() == '\n') {
        manifest_filename.pop_back();
    }

    std::string manifest_path = dbname_ + "/" + manifest_filename;
    SequentialFile* manifest_file = nullptr;
    Status s = NewSequentialFile(manifest_path, &manifest_file);
    if (!s.ok()) return s;

    log::Reader reader(manifest_file, nullptr, /*checksum=*/true, 0);
    Slice record;
    std::string scratch;

    // 将所有 VersionEdit 依次重放到一个新 Version 上，最终得到最新的文件集合。
    std::shared_ptr<Version> v = std::make_shared<Version>(this);
    std::set<std::pair<int, uint64_t>> deleted_files;

    while (reader.ReadRecord(&record, &scratch)) {
        VersionEdit edit;
        s = edit.DecodeFrom(record);
        if (!s.ok()) break;

        if (edit.has_log_number_)       log_number_       = edit.log_number_;
        if (edit.has_next_file_number_) next_file_number_ = edit.next_file_number_;
        if (edit.has_last_sequence_)    last_sequence_    = edit.last_sequence_;

        for (const auto& del : edit.deleted_files_) {
            deleted_files.insert(del);
        }
        for (const auto& new_file_pair : edit.new_files_) {
            int level = new_file_pair.first;
            std::pair<int, uint64_t> file_id =
                std::make_pair(level, new_file_pair.second.number);
            if (deleted_files.find(file_id) == deleted_files.end()) {
                v->files_[level].push_back(
                    std::make_shared<FileMetaData>(new_file_pair.second));
            }
        }
    }

    delete manifest_file;

    if (s.ok()) {
        current_       = v;
        *save_manifest = false;
    }

    return s;
}

// --------------------------------------------------------------------------
// Compaction selection
// --------------------------------------------------------------------------

int64_t VersionSet::MaxBytesForLevel(int level) const {
    // L1 = max_bytes_for_level_base，每向上一级乘以 10。
    int64_t result = options_->max_bytes_for_level_base;
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
    if (static_cast<int>(current_->files_[0].size()) >= options_->l0_compaction_trigger) {
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
    if (static_cast<int>(current_->files_[0].size()) >= options_->l0_compaction_trigger) {
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
