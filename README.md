# MiniLevelDB

一个用 C++ 编写的本地键值存储引擎，参考 LevelDB 的 LSM-tree 设计。应用可以通过接口写入、查询和删除数据，数据保存在本地文件中。

这个项目用于学习数据库底层实现，主要完成了从日志写入、内存存储到磁盘文件读写和后台合并的流程。开发中使用了 AI 编程辅助，目前仍在逐步检查和完善代码。

## 主要做了什么

- **读写接口**：提供 `Put`、`Get`、`Delete`，通过 `WriteBatch` 一次提交多条修改。
- **日志与恢复**：修改先写入 WAL 日志，再加入内存表；启动时读取文件元数据并回放日志。
- **内存与磁盘存储**：用跳表保存内存中的记录，内存表达到阈值后，由后台线程写成有序的 SSTable 文件。
- **后台合并**：将 SSTable 分为 L0～L6，合并重叠文件、整理旧版本，并更新 MANIFEST 中的文件记录。
- **查询辅助**：用文件索引定位数据，配合 Bloom Filter 和已打开文件的缓存，减少不必要的读取。
- **测试与实验**：提供基础正确性测试、单线程和多线程基准，以及跳表与 `std::map`、开启与关闭 Compaction 的对比程序。

写入时先记日志，再放进内存；查询时先找内存，再找磁盘文件。后台负责把内存数据写成文件，并持续合并整理已有文件。

## 编译与运行

环境：Linux、支持 C++17 的 GCC 或 Clang、CMake 3.10 及以上。

在仓库根目录执行：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4

# 基础正确性测试
./build/db_correctness

# 单线程读写基准
./build/db_bench --num_entries=100000 --batch_size=1000 --sync_write=1
```

`--sync_write=1` 表示每次提交写入后等待 WAL 同步；设为 `0` 可以减少等待，但不保证本次返回时日志已经持久化。

其他测试程序：

| 程序 | 用途 |
|---|---|
| `db_bench_mt` | 测量多线程写入吞吐和延迟 |
| `memtable_ds_bench` | 比较跳表与 `std::map` 的插入、查询开销 |
| `compaction_ab_bench` | 比较开启和关闭后台文件合并时的表现 |

运行 `./run_all_bench.sh` 可统一编译并执行测试与基准，结果保存在 `test_results/`。性能结果需要结合数据量、批次大小、同步设置和运行环境一起看。

## 代码从哪里看

| 文件 | 主要内容 |
|---|---|
| `include/db.h`、`src/db_impl.cpp` | 对外接口，以及读写、恢复和后台任务的组织 |
| `include/write_batch.h` | 保存一批待执行的写入和删除操作 |
| `src/memtable.cpp`、`include/skiplist.h` | 内存表与跳表 |
| `src/log_writer.cpp`、`src/log_reader.cpp` | WAL 的写入和读取 |
| `src/table_builder.cpp`、`src/table.cpp` | SSTable 的构建和查询 |
| `src/version_set.cpp`、`src/version_edit.cpp` | 管理各层文件及其变更，选择待合并的文件 |

## 当前限制

这是学习项目，仍有需要修正和验证的部分：

- 快照接口已存在，但内存查询还没有按快照序列号筛选记录。
- 对外迭代器目前合并的是内部记录，尚未完成旧版本去重、删除标记过滤和快照过滤。
- 查询在内存表中遇到删除标记时，目前会返回成功状态，需要修正为“未找到”。
- `WriteBatch` 提供批量提交；异常情况下的批次原子性和崩溃恢复仍需进一步验证。

现有正确性测试主要覆盖批量写入、覆盖写和正常关闭后重新打开，不能代替故障恢复测试。历史排查和修复记录见 [docs/BUGS.md](docs/BUGS.md)。
