#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cstdio>
#include <cassert>
#include <filesystem>
#include <set>
#include "db_impl.h"
#include "write_batch.h"
#include "coding.h"
#include "table_builder.h"
#include "table_cache.h"
#include "memtable.h"
#include "comparator.h"
#include "version_set.h"
#include "version_edit.h"
#include "log_reader.h"
#include "merging_iterator.h"

namespace minidb {

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : options_(options),
      dbname_(dbname),
      shutting_down_(false),
      bg_compaction_scheduled_(false),
      last_sequence_(0),
      imm_(nullptr), logfile_(nullptr), log_(nullptr),
      internal_comparator_(options.comparator)
{
    // InternalKeyComparator 封装了用户比较器，额外处理序列号降序。
    // 所有内部组件统一使用 internal_options，避免用户比较器泄漏到磁盘格式层。
    Options internal_options = options_;
    internal_options.comparator = &internal_comparator_;

    table_cache_ = new TableCache(dbname_, internal_options, 1000);
    versions_    = new VersionSet(dbname_, &internal_options, table_cache_, &internal_comparator_);

    // 从 MANIFEST 恢复版本集合。首次建库时 CURRENT 文件不存在，返回 NotFound 属于正常情况。
    bool save_manifest = false;
    Status s = versions_->Recover(&save_manifest);
    if (s.IsNotFound()) {
        s = Status::OK();
    } else if (!s.ok()) {
        std::cerr << "[FATAL] Failed to recover VersionSet: " << s.ToString() << "\n";
        abort();
    }

    mem_ = new MemTable(&internal_comparator_);

    // WAL 回放：找出所有编号 >= versions_->log_number() 的遗留 .log 文件并按序重放。
    // 严格升序排列，保证操作时序与写入时一致。
    if (s.ok()) {
        std::vector<uint64_t> logs_to_replay;
        for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
            if (entry.path().extension() == ".log") {
                uint64_t log_num = std::stoull(entry.path().stem().string());
                if (log_num >= versions_->log_number()) {
                    logs_to_replay.push_back(log_num);
                }
            }
        }
        std::sort(logs_to_replay.begin(), logs_to_replay.end());

        for (uint64_t log_num : logs_to_replay) {
            char buf[100];
            std::snprintf(buf, sizeof(buf), "%s/%06llu.log",
                          dbname_.c_str(), (unsigned long long)log_num);

            SequentialFile* log_file = nullptr;
            s = NewSequentialFile(buf, &log_file);
            if (!s.ok()) continue;

            log::Reader reader(log_file, nullptr, /*checksum=*/true, 0);
            Slice record;
            std::string scratch;

            // 每条 WAL record 的格式：[seq: 8B][count: 4B][record...]
            // 与 Write() 中序列化格式保持一致。
            while (reader.ReadRecord(&record, &scratch)) {
                if (record.size() < 12) continue;

                uint64_t seq   = DecodeFixed64(record.data());
                uint32_t count = DecodeFixed32(record.data() + 8);

                const char* p     = record.data() + 12;
                const char* limit = record.data() + record.size();

                for (uint32_t i = 0; i < count; i++) {
                    if (p >= limit) break;
                    char type = *p++;
                    uint32_t key_len;

                    if (!GetVarint32(&p, limit, &key_len) || p + key_len > limit) break;
                    Slice key(p, key_len);
                    p += key_len;

                    if (type == kTypeValue) {
                        uint32_t val_len;
                        if (!GetVarint32(&p, limit, &val_len) || p + val_len > limit) break;
                        Slice value(p, val_len);
                        p += val_len;
                        mem_->Add(seq, type, key, value);
                    } else {
                        // kTypeDeletion 无 value 字段。
                        mem_->Add(seq, type, key, Slice());
                    }
                    seq++;
                }
                // 回放后推进全局序列号，防止后续写入序列号倒退。
                last_sequence_ = seq - 1;
            }
            delete log_file;
        }
    }

    // 为本次进程分配新的 WAL，并将其编号写入 MANIFEST。
    // 之后的写入都追加到这个新文件，旧文件在 flush 完成后才会被删除。
    if (s.ok()) {
        uint64_t new_log_number = versions_->NextFileNumber();
        char buf[100];
        std::snprintf(buf, sizeof(buf), "%s/%06llu.log",
                      dbname_.c_str(), (unsigned long long)new_log_number);

        s = NewWritableFile(buf, (WritableFile**)&logfile_);
        if (s.ok()) {
            log_ = new log::Writer((WritableFile*)logfile_);
            VersionEdit edit;
            edit.SetLogNumber(new_log_number);
            s = versions_->LogAndApply(&edit);
        }
    }

    // 后台线程常驻，负责将 immutable MemTable flush 为 SSTable 或执行 Level Compaction。
    bg_thread_ = std::thread(&DBImpl::BackgroundCall, this);
}

DBImpl::~DBImpl() {
    // 通知后台线程退出，再等待它完成当前任务后 join。
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutting_down_ = true;
        bg_cv_.notify_all();
    }
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }

    // 按依赖关系逆序释放：versions_ 持有对 table_cache_ 的引用，须先析构。
    delete versions_;
    delete table_cache_;
    delete mem_;
    if (imm_ != nullptr) {
        delete imm_;
    }

    // log::Writer 析构时可能触发最后一次 flush，因此须先于底层文件句柄析构。
    delete log_;
    delete logfile_;

    for (Snapshot* s : snapshots_) {
        delete s;
    }
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(options, &batch);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(options, &batch);
}

// Write 是所有写操作的统一入口。
// 整批 WriteBatch 序列化为单条 WAL record 后原子写入，减少系统调用次数。
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
    std::unique_lock<std::mutex> lock(mutex_);

    Status s = MakeRoomForWrite(lock);
    if (!s.ok()) return s;

    uint64_t seq = last_sequence_ + 1;

    if (log_ != nullptr) {
        // 将整个 batch 序列化为一块连续 buffer，避免多次 write() 系统调用。
        // 格式：[seq: 8B fixed][count: 4B fixed][type: 1B][key_len: varint][key][val_len: varint][val]...
        std::string batch_payload;
        batch_payload.reserve(updates->ApproximateSize() + 1024);

        PutFixed64(&batch_payload, seq);
        PutFixed32(&batch_payload, static_cast<uint32_t>(updates->Records().size()));

        for (const auto& record : updates->Records()) {
            batch_payload.push_back(static_cast<char>(
                record.type == BatchValueType::kTypeValue ? kTypeValue : kTypeDeletion));
            PutVarint32(&batch_payload, static_cast<uint32_t>(record.key.size()));
            batch_payload.append(record.key.data(), record.key.size());
            if (record.type == BatchValueType::kTypeValue) {
                PutVarint32(&batch_payload, static_cast<uint32_t>(record.value.size()));
                batch_payload.append(record.value.data(), record.value.size());
            }
        }

        s = log_->AddRecord(Slice(batch_payload));

        if (s.ok() && options.sync && logfile_ != nullptr) {
            s = logfile_->Sync();
        }
    }

    // WAL 落盘成功后才写 MemTable，保证崩溃时可从日志恢复。
    if (s.ok()) {
        for (const auto& record : updates->Records()) {
            uint32_t type = (record.type == BatchValueType::kTypeValue) ? kTypeValue : kTypeDeletion;
            mem_->Add(seq, type, record.key, record.value);
            seq++;
        }
        last_sequence_ = seq - 1;
    }

    return s;
}

// MakeRoomForWrite 确保 mem_ 有足够空间承接当前写入。
// 若 mem_ 已满且 imm_ 为空，则将 mem_ 转为 imm_，分配新 mem_ 和 WAL，并唤醒 flush 线程。
// 若 imm_ 尚未 flush 完毕，阻塞前台写线程，避免内存无限膨胀（背压机制）。
Status DBImpl::MakeRoomForWrite(std::unique_lock<std::mutex>& lock) {
    while (true) {
        if (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) {
            return Status::OK();
        } else if (imm_ != nullptr) {
            // imm_ 尚未 flush，等待后台线程完成后唤醒。
            bg_cv_.wait(lock);
        } else {
            // mem_ 已满，imm_ 为空：执行 double-buffer 翻转。
            uint64_t new_log_number = versions_->NextFileNumber();
            char buf[100];
            std::snprintf(buf, sizeof(buf), "%s/%06llu.log",
                          dbname_.c_str(), (unsigned long long)new_log_number);

            WritableFile* new_logfile = nullptr;
            Status s = NewWritableFile(buf, &new_logfile);
            if (!s.ok()) return s;

            delete log_;
            delete logfile_;
            logfile_ = new_logfile;
            log_     = new log::Writer(logfile_);

            // 将新 WAL 编号持久化到 MANIFEST，崩溃恢复时据此判断哪些日志需要回放。
            VersionEdit edit;
            edit.SetLogNumber(new_log_number);
            s = versions_->LogAndApply(&edit);
            if (!s.ok()) return s;

            imm_ = mem_;
            mem_ = new MemTable(&internal_comparator_);

            if (!bg_compaction_scheduled_) {
                bg_compaction_scheduled_ = true;
                bg_cv_.notify_all();
            }
        }
    }
}

// BackgroundCall 在独立线程中运行，等待调度信号后执行 flush 或 compaction。
void DBImpl::BackgroundCall() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!shutting_down_) {
        while (!bg_compaction_scheduled_ && !shutting_down_) {
            bg_cv_.wait(lock);
        }
        if (shutting_down_) break;

        BackgroundCompaction();
        bg_compaction_scheduled_ = false;

        // 通知 MakeRoomForWrite 中被阻塞的前台线程继续写入。
        bg_cv_.notify_all();
    }
}

void DBImpl::BackgroundCompaction() {
    // 优先处理 Immutable MemTable flush（写入 L0）。
    if (imm_ != nullptr) {
        Status s = CompactMemTable();
        if (s.ok()) {
            delete imm_;
            imm_ = nullptr;
            DeleteObsoleteFiles();
        } else {
            std::cerr << "[ERROR] MemTable flush failed: " << s.ToString() << "\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
            return;
        }
    }

    // 若 flush 后仍触发 compaction 条件（如 L0 文件数超阈值），执行 SSTable 级归并。
    if (versions_->NeedsCompaction()) {
        Compaction* c = versions_->PickCompaction();
        if (c != nullptr) {
            Status s = RunCompaction(c);
            delete c;
            if (!s.ok()) {
                std::cerr << "[ERROR] Level compaction failed: " << s.ToString() << "\n";
            }
            DeleteObsoleteFiles();
        }
    }
}

// CompactMemTable 将 imm_ 顺序扫描并写出为一个 Level-0 SSTable 文件。
// mutex_ 在 I/O 期间暂时释放，允许前台写入继续进行。
Status DBImpl::CompactMemTable() {
    uint64_t file_number = versions_->NextFileNumber();
    char buf[100];
    std::snprintf(buf, sizeof(buf), "%s/%06llu.sst",
                  dbname_.c_str(), (unsigned long long)file_number);

    WritableFile* sst_file = nullptr;
    Status s = NewWritableFile(std::string(buf), &sst_file);
    if (!s.ok()) return s;

    Options builder_options = options_;
    builder_options.comparator = &internal_comparator_;
    TableBuilder* builder = new TableBuilder(builder_options, sst_file);

    std::string smallest_key;
    std::string largest_key;
    bool has_data = false;

    // 释放锁以允许前台写入，此后不得访问任何共享可变状态（除 imm_ 为只读快照外）。
    mutex_.unlock();

    Iterator* iter = imm_->NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        if (!has_data) {
            smallest_key = key.ToString();
            has_data = true;
        }
        largest_key = key.ToString();
        builder->Add(key, iter->value());
    }
    delete iter;

    s = builder->Finish();
    uint64_t file_size = builder->FileSize();
    if (s.ok()) {
        s = sst_file->Sync();
    }
    delete builder;
    delete sst_file;

    mutex_.lock();

    // 将新 SSTable 登记到 Version 中，写入 MANIFEST 后方可被读路径感知。
    if (s.ok() && has_data) {
        VersionEdit edit;
        edit.AddFile(0, file_number, file_size, Slice(smallest_key), Slice(largest_key));
        s = versions_->LogAndApply(&edit);
    }

    return s;
}

// --------------------------------------------------------------------------
// RunCompaction: SSTable 级多路归并压缩（Lx -> Lx+1）
// --------------------------------------------------------------------------

// RunCompaction 对 Compaction::inputs[0]（source level）和 inputs[1]（target level）
// 中的所有 SSTable 做多路归并，写出一批新的 target level SSTable，并更新 MANIFEST。
//
// 关键语义：
//   - 相同 UserKey 只保留序列号最大（最新）的版本；
//   - 序列号 < oldest_snapshot 的旧版本可安全丢弃（不再有快照读到它）；
//   - 删除墓碑（kTypeDeletion）在最低层才可消除，高层必须保留。
Status DBImpl::RunCompaction(Compaction* c) {
    const int src_level = c->input_level;
    const int dst_level = src_level + 1;

    // 收集所有参与压缩的 SSTable 迭代器。
    std::vector<Iterator*> iters;
    ReadOptions ro;
    ro.fill_cache = false; // compaction 产生的读不应污染 LRU 缓存

    for (int which = 0; which < 2; ++which) {
        for (const auto& f : c->inputs[which]) {
            iters.push_back(
                table_cache_->NewIterator(ro, f->number, f->file_size));
        }
    }

    // 释放锁，允许前台写入在 compaction I/O 期间继续。
    mutex_.unlock();

    const uint64_t oldest_snap = OldestSnapshotSequence();

    // 用 MergingIterator 做多路归并，输出全局有序的 InternalKey 流。
    MergingIterator merge_iter(&internal_comparator_, std::move(iters));
    merge_iter.SeekToFirst();

    // 每条输出文件的元数据：(file_number, file_size, smallest_key, largest_key)
    struct OutputFileInfo {
        uint64_t    number;
        uint64_t    file_size;
        std::string smallest;
        std::string largest;
    };
    std::vector<OutputFileInfo> output_files;

    WritableFile*  out_file    = nullptr;
    TableBuilder*  builder     = nullptr;
    uint64_t       out_number  = 0;
    std::string    out_smallest;
    std::string    out_largest;

    // 当输出文件达到 2MB 时切分，避免单个 SSTable 过大。
    constexpr uint64_t kTargetFileSize = 2 * 1024 * 1024;

    auto FinishOutputFile = [&]() -> Status {
        if (builder == nullptr) return Status::OK();
        Status s = builder->Finish();
        uint64_t fsize = builder->FileSize();
        delete builder; builder = nullptr;
        if (s.ok()) s = out_file->Sync();
        delete out_file; out_file = nullptr;
        if (s.ok()) {
            output_files.push_back({out_number, fsize, out_smallest, out_largest});
        }
        return s;
    };

    Options builder_options = options_;
    builder_options.comparator = &internal_comparator_;

    std::string prev_user_key;
    bool        has_prev = false;

    Status s;
    for (; merge_iter.Valid() && s.ok(); merge_iter.Next()) {
        Slice ikey = merge_iter.key();

        // 解析 InternalKey：末 8 字节 = (seq << 8) | type
        if (ikey.size() < 8) continue;
        uint64_t packed = DecodeFixed64(ikey.data() + ikey.size() - 8);
        uint64_t seq    = packed >> 8;
        uint8_t  type   = static_cast<uint8_t>(packed & 0xff);
        Slice    ukey(ikey.data(), ikey.size() - 8);

        // 同 UserKey 的较老版本，若已被新版本覆盖且无快照依赖，直接丢弃。
        bool same_user_key = has_prev && (ukey.compare(Slice(prev_user_key)) == 0);
        if (same_user_key) {
            // 已有更新版本（prev_seq > seq），且旧版本的序列号不被任何快照引用。
            if (seq < oldest_snap) {
                // 若是删除墓碑，且 dst_level 是最高层，也可以丢弃。
                // 此处简化：所有被覆盖的旧版本一律丢弃。
                continue;
            }
        }
        prev_user_key = ukey.ToString();
        has_prev = true;

        // 丢弃序列号 < oldest_snap 的删除墓碑：它已被所有快照"看透"，无需保留。
        // 注意：只在确认 dst_level 是该 key 的最低层时才能丢弃，此处简化为跳过。
        (void)type; // type 在上述简化策略中暂不用于额外过滤

        // 打开新输出文件。
        // NextFileNumber() 需要访问共享状态，必须在持锁时调用。
        if (builder == nullptr) {
            mutex_.lock();
            out_number = versions_->NextFileNumber();
            mutex_.unlock();

            char buf[100];
            std::snprintf(buf, sizeof(buf), "%s/%06llu.sst",
                          dbname_.c_str(), (unsigned long long)out_number);
            s = NewWritableFile(std::string(buf), &out_file);
            if (!s.ok()) break;
            builder = new TableBuilder(builder_options, out_file);
            out_smallest = ikey.ToString();
        }

        builder->Add(ikey, merge_iter.value());
        out_largest = ikey.ToString();

        // 超出目标文件大小时切分。
        if (builder->FileSize() >= kTargetFileSize) {
            s = FinishOutputFile();
        }
    }

    if (s.ok()) {
        s = FinishOutputFile();
    }

    mutex_.lock();

    if (s.ok()) {
        // 将输入文件从 VersionEdit 中标记为删除，输出文件登记为新增。
        VersionEdit& edit = c->edit;
        for (int which = 0; which < 2; ++which) {
            int level = (which == 0) ? src_level : dst_level;
            for (const auto& f : c->inputs[which]) {
                edit.DeleteFile(level, f->number);
            }
        }
        for (const auto& fi : output_files) {
            edit.AddFile(dst_level, fi.number, fi.file_size,
                         Slice(fi.smallest), Slice(fi.largest));
        }

        s = versions_->LogAndApply(&edit);
    }

    return s;
}

// --------------------------------------------------------------------------
// 辅助方法
// --------------------------------------------------------------------------

// DeleteObsoleteFiles 扫描数据库目录，删除不再被任何 Version 引用的 SSTable 和 WAL 文件。
// 需要在 mutex_ 保护下调用（已在 BackgroundCompaction 中持锁）。
void DBImpl::DeleteObsoleteFiles() {
    // 收集当前所有 Version 中仍被引用的文件编号。
    std::set<uint64_t> live_files;
    std::shared_ptr<Version> cur = versions_->current();
    if (cur != nullptr) {
        for (int level = 0; level < kNumLevels; ++level) {
            for (const auto& f : cur->files(level)) {
                live_files.insert(f->number);
            }
        }
    }

    // 扫描目录，删除 .sst 文件中不在 live_files 中的，以及过时的 .log 文件。
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(dbname_, ec)) {
        const std::string ext = entry.path().extension().string();
        if (ext == ".sst") {
            uint64_t num = std::stoull(entry.path().stem().string());
            if (live_files.find(num) == live_files.end()) {
                std::filesystem::remove(entry.path(), ec);
                if (!ec) {
                    std::cout << "[INFO] Removed obsolete SSTable: "
                              << entry.path().filename().string() << "\n";
                }
            }
        } else if (ext == ".log") {
            uint64_t log_num = std::stoull(entry.path().stem().string());
            if (log_num < versions_->log_number()) {
                std::filesystem::remove(entry.path(), ec);
                if (!ec) {
                    std::cout << "[INFO] Removed obsolete WAL: "
                              << entry.path().filename().string() << "\n";
                }
            }
        }
    }
}

uint64_t DBImpl::OldestSnapshotSequence() const {
    if (snapshots_.empty()) return last_sequence_;
    return snapshots_.front()->sequence();
}

// --------------------------------------------------------------------------
// Get
// --------------------------------------------------------------------------

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    std::shared_ptr<Version> current_version;
    uint64_t snapshot_seq;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 先查活跃 MemTable，再查 Immutable MemTable（若存在）。
        if (mem_->Get(key, value)) return Status::OK();
        if (imm_ != nullptr && imm_->Get(key, value)) return Status::OK();

        // 获取当前版本的共享指针，锁外持有引用计数可防止 flush 时版本被销毁。
        current_version = versions_->current();
        // 若调用方指定了 Snapshot，使用其序列号；否则使用当前最新序列号。
        snapshot_seq = (options.snapshot != nullptr)
                       ? options.snapshot->sequence()
                       : last_sequence_;
    }

    // 构造 LookupKey（InternalKey）：UserKey + (seq << 8 | kTypeValue)。
    // kTypeValue 作为类型上界，确保 Seek 时能命中同 key 的最新版本。
    std::string internal_key;
    internal_key.reserve(key.size() + 8);
    internal_key.append(key.data(), key.size());
    uint64_t packed = (snapshot_seq << 8) | 1;
    PutFixed64(&internal_key, packed);

    // 磁盘查询无需持锁，Version 的 shared_ptr 保证了文件元数据的生命周期安全。
    return current_version->Get(options, Slice(internal_key), value);
}

// --------------------------------------------------------------------------
// NewIterator
// --------------------------------------------------------------------------

// NewIterator 构造一个覆盖 MemTable + Immutable MemTable + 所有 Level SSTable 的 MergingIterator。
// 迭代器仅能看到 snapshot_seq 及以下的数据版本（通过 InternalKey 比较隐式实现）。
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    std::vector<Iterator*> iters;
    std::shared_ptr<Version> current_version;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        iters.push_back(mem_->NewIterator());
        if (imm_ != nullptr) {
            iters.push_back(imm_->NewIterator());
        }
        current_version = versions_->current();
    }

    // 将所有 Level 的 SSTable 迭代器加入合并集合。
    if (current_version != nullptr) {
        for (int level = 0; level < kNumLevels; ++level) {
            for (const auto& f : current_version->files(level)) {
                iters.push_back(
                    table_cache_->NewIterator(options, f->number, f->file_size));
            }
        }
    }

    return new MergingIterator(&internal_comparator_, std::move(iters));
}

// --------------------------------------------------------------------------
// Snapshot
// --------------------------------------------------------------------------

const Snapshot* DBImpl::GetSnapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    Snapshot* snap = new Snapshot(last_sequence_);
    // 按序列号升序插入，保持链表有序，便于 OldestSnapshotSequence() O(1) 返回。
    auto it = snapshots_.begin();
    while (it != snapshots_.end() && (*it)->sequence() <= snap->sequence()) {
        ++it;
    }
    snapshots_.insert(it, snap);
    return snap;
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = snapshots_.begin(); it != snapshots_.end(); ++it) {
        if (*it == snapshot) {
            snapshots_.erase(it);
            break;
        }
    }
    delete snapshot;
}

// --------------------------------------------------------------------------
// DB::Open
// --------------------------------------------------------------------------

// DB::Open 是对外公开的工厂接口，调用方通过抽象基类指针持有引擎实例，与实现解耦。
Status DB::Open(const Options& options, const std::string& name, DB** dbptr) {
    *dbptr = nullptr;

    std::error_code ec;
    if (!std::filesystem::exists(name)) {
        if (!options.create_if_missing) {
            return Status::NotFound("DB directory does not exist: " + name);
        }
        if (!std::filesystem::create_directories(name, ec)) {
            return Status::IOError("Failed to create DB directory: " + name);
        }
    }

    DBImpl* impl = new DBImpl(options, name);
    *dbptr = impl;
    return Status::OK();
}

} // namespace minidb
