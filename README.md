# MiniLevelDB

一个基于 LSM-tree（Log-Structured Merge-Tree）思想的嵌入式 KV 存储引擎，用 C++17 实现，对标 LevelDB 的核心读写链路。

## 功能概述

- **持久化写入**：WAL（Write-Ahead Log）预写日志 + MemTable 内存缓冲，保证写入高吞吐的同时具备崩溃恢复能力
- **有序磁盘存储**：Immutable MemTable 由后台线程异步 flush 为 Level-0 SSTable（`.sst`）文件
- **多层 Compaction**：L0 文件数超阈值或 L1-L6 总字节超限时，后台线程自动触发多路归并压缩（L0→L1→…→L6）
- **点查支持**：读路径按 MemTable → Immutable MemTable → Level-0 → Level-1-6 顺序查找；L1+ 使用二分定位
- **范围迭代**：`NewIterator` 返回覆盖 MemTable + 所有 Level SSTable 的 MergingIterator，支持全量有序遍历
- **Snapshot 隔离读**：`GetSnapshot`/`ReleaseSnapshot` 管理只读快照，持有快照的读操作不受后续写入影响
- **Bloom Filter**：可选的布隆过滤器（每 2KB 数据一个 segment），在读磁盘前快速排除不存在的 key
- **版本管理**：MANIFEST + CURRENT 记录文件集合的增量变更，支持启动时版本恢复
- **LRU Table Cache**：缓存已打开的 SSTable 对象（Index Block + Bloom Filter 常驻内存），减少重复 I/O

## 架构说明

```
写入路径：
  WriteBatch
    → WAL (log::Writer, 32KB block 分帧, CRC32c 校验)
    → MemTable (SkipList + Arena)
    → [内存满] Immutable MemTable
    → [后台线程] SSTable (TableBuilder → .sst 写入 Level-0)
    → VersionEdit → MANIFEST

Compaction 路径（后台线程）：
  PickCompaction (L0 文件数 >= 4 或 L1-L6 字节超限)
    → 收集 inputs[0]（source level）和 inputs[1]（target level 重叠文件）
    → MergingIterator 多路归并（去重旧版本，丢弃过时删除墓碑）
    → 写出新 SSTable（单文件 ≤ 2MB，超出自动切分）
    → VersionEdit（DeleteFile 旧文件 + AddFile 新文件）→ MANIFEST
    → DeleteObsoleteFiles 删除不再引用的 .sst / .log

读取路径：
  Get(key, snapshot)
    → MemTable::Get
    → Immutable MemTable::Get（若存在）
    → Version::Get
         ├── L0：逆序扫描（文件间可重叠，最新文件优先）
         └── L1-L6：二分定位（文件间不重叠，按 smallest key 有序）
               └── TableCache::Get → Table::Get
                    └── Index Block 二分 → Bloom Filter 过滤 → Data Block 精确匹配

范围迭代路径：
  NewIterator(ReadOptions)
    → MergingIterator(MemTable iter + Imm iter + 所有 Level SSTable iters)
    → SeekToFirst / Seek / Next → 全局有序视图（InternalKey 排序，上层可解析 UserKey）

崩溃恢复：
  DB::Open → VersionSet::Recover (CURRENT → MANIFEST)
           → WAL 回放 (重建 MemTable，序列号前进)
           → 分配新 WAL，写入 MANIFEST
```

## 核心模块

| 模块 | 文件 | 说明 |
|------|------|------|
| 数据库引擎 | `src/db_impl.cpp` | 写入、读取、Snapshot、NewIterator、后台 Compaction、崩溃恢复 |
| 多路归并迭代器 | `src/merging_iterator.cpp` | k 路归并，O(n) 每步，支持正向/反向遍历及方向切换 |
| 内存表 | `src/memtable.cpp`, `include/skiplist.h` | SkipList + Arena，单写多读无锁 |
| WAL | `src/log_writer.cpp`, `src/log_reader.cpp` | 分帧日志，CRC32c 校验，支持回放 |
| SSTable 构建 | `src/table_builder.cpp`, `src/block_builder.cpp` | 前缀压缩 + 重启点 + CRC + Footer |
| SSTable 读取 | `src/table.cpp`, `src/block.cpp` | 两层迭代器，Index Block 二分 |
| 布隆过滤器 | `src/bloom.cpp`, `src/filter_block.cpp` | 多哈希 Bloom Filter，按 2KB 分 segment |
| 版本管理 | `src/version_set.cpp`, `src/version_edit.cpp` | Copy-on-Write Version，MANIFEST 持久化，PickCompaction 轮转选文件 |
| Table 缓存 | `src/table_cache.cpp` | LRU 缓存，容量 1000 个 SSTable |
| 底层 I/O | `src/env_posix.cpp` | POSIX 文件接口，pread 线程安全随机读 |
| InternalKey | `src/db_format.cpp` | UserKey 升序 + 序列号降序比较器，MVCC 基础 |

## 编译与运行

**依赖**：CMake >= 3.10，GCC/Clang（C++17）

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j4
./db_bench
```

## 公共接口

```cpp
// 打开或创建数据库
Status DB::Open(const Options& options, const std::string& name, DB** dbptr);

// 点写/点读
Status db->Put(WriteOptions, key, value);
Status db->Get(ReadOptions, key, &value);
Status db->Delete(WriteOptions, key);
Status db->Write(WriteOptions, WriteBatch*);  // 原子批量写

// 范围迭代（调用方负责 delete）
Iterator* db->NewIterator(ReadOptions);

// Snapshot 隔离读
const Snapshot* snap = db->GetSnapshot();
ReadOptions ro; ro.snapshot = snap;
db->Get(ro, key, &value);           // 只看到 snap 创建时刻的数据
db->ReleaseSnapshot(snap);
```

## Benchmark 结果

测试配置：100,000 条记录，Key = 16 字节，Value = 100 字节，batch size = 1000，sync = false

| 测试项 | 耗时（micros/op） | 吞吐（ops/sec） | 数据量（MB/s） |
|--------|-----------------|----------------|--------------|
| FillSeq（顺序写） | 764 | 1,309 | 0.1 |
| ReadRandom（随机读） | 88 | 11,398 | 1.3 |
| FillRandom（随机写） | 431 | 2,319 | 0.3 |

随机读命中率：99,995 / 100,000

> 测试环境：Linux 6.8，单线程，Debug 构建（Release 性能更优）

## 当前实现范围

**已实现**：
- 完整的写入链路（WAL + MemTable + SSTable + MANIFEST）
- 崩溃恢复（版本恢复 + WAL 回放）
- 多层查询路径（L0 逆序扫描 + L1-L6 二分定位）
- L0→L1→…→L6 多路归并 Compaction（轮转选文件，过时版本清理）
- 范围迭代（MergingIterator，覆盖 MemTable + 所有 Level SSTable）
- Snapshot 隔离读（GetSnapshot / ReleaseSnapshot）
- Bloom Filter、前缀压缩、CRC32c 校验
- 后台 flush 线程、背压机制、LRU Table Cache

**计划中（TODO）**：
- 压缩（Snappy/zstd）Block 数据
- 统计信息（每层文件数、字节数、Compaction 次数）
- 更精细的 Compaction 触发策略（如 seek-based compaction）
