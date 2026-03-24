#pragma once

#include "db.h"
#include "env.h"
#include "log_writer.h"
#include "memtable.h"
#include "db_format.h"
#include "snapshot.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <string>
#include <cstdint>
#include <list>

namespace minidb {

class TableCache;
class MemTable;
class VersionSet;
class WriteBatch;
class Table;
struct Compaction;

class DBImpl : public DB {
public:
    DBImpl(const Options& options, const std::string& dbname);
    ~DBImpl() override;

    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
    Status Delete(const WriteOptions& options, const Slice& key) override;
    Status Write(const WriteOptions& options, WriteBatch* updates) override;
    Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;

    // NewIterator 返回覆盖全部数据（MemTable + 所有 Level SSTable）的合并迭代器。
    Iterator* NewIterator(const ReadOptions& options) override;

    // GetSnapshot 创建当前时刻的只读快照，ReleaseSnapshot 释放。
    const Snapshot* GetSnapshot() override;
    void ReleaseSnapshot(const Snapshot* snapshot) override;

private:
    // MakeRoomForWrite 在写入前确保 mem_ 有足够空间。
    // 若内存已满，执行 double-buffer 翻转并唤醒后台 flush 线程；
    // 若 imm_ 尚未 flush 完毕，阻塞前台线程（背压机制）。
    Status MakeRoomForWrite(std::unique_lock<std::mutex>& lock);

    void   BackgroundCall();
    void   BackgroundCompaction();
    Status CompactMemTable();

    // RunCompaction 执行一次 SSTable 级的多路归并压缩（L0->L1 或 Lx->Lx+1）。
    Status RunCompaction(Compaction* c);

    // DeleteObsoleteFiles 删除已不在任何 Version 中引用的过期文件。
    void DeleteObsoleteFiles();


    // 返回所有活跃 Snapshot 中最小的序列号；无 Snapshot 时返回 last_sequence_。
    // Compaction 中遇到序列号 < oldest_snapshot 的重复 key 可安全丢弃。
    uint64_t OldestSnapshotSequence() const;

    const Options     options_;
    const std::string dbname_;

    std::mutex              mutex_;
    std::condition_variable bg_cv_;
    bool                    shutting_down_;
    bool                    bg_compaction_scheduled_;

    // 全局逻辑时钟，单调递增，每条写入操作消耗一个序列号，受 mutex_ 保护。
    uint64_t last_sequence_;

    MemTable* mem_; // 接收前台写入的活跃内存表
    MemTable* imm_; // 正在被后台线程 flush 的只读内存表（可为 nullptr）

    WritableFile*  logfile_;
    log::Writer*   log_;

    std::thread bg_thread_;

    InternalKeyComparator internal_comparator_;
    TableCache*           table_cache_;
    VersionSet*           versions_;

    // 活跃快照链表（按序列号升序排列），受 mutex_ 保护。
    std::list<Snapshot*> snapshots_;
};

} // namespace minidb
