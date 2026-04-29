# MiniLevelDB 已知问题与修复记录

> 持久化记录测试/排查过程中发现的所有 bug 和值得跟进的实验缺陷。
> 每条记录都包含：发现时间、严重程度、现象、定位、修复状态、回归方式。
> **新发现的问题一律追加到本文件，不要直接覆盖历史条目**（即使已修复也保留，便于复盘）。

## 状态标识

- 🔴 **OPEN**: 未修复
- 🟡 **IN PROGRESS**: 修复中 / 部分修复
- 🟢 **FIXED**: 已修复并验证
- ⚪ **WONTFIX / DEFERRED**: 已知但暂不修复（需写明理由）

## 严重程度

- **P0**: 正确性 / 数据丢失 / 进程崩溃
- **P1**: 性能 / 关键功能没生效 / 测试结论不可信
- **P2**: 工具链 / 可观测性 / 实验设计缺陷
- **P3**: 文档 / 易用性

---

## BUG-001 🔴 P0 — Version::Get 用 InternalKey 做 SST 范围判断，导致已写入的 key 读不到

**发现日期**: 2026-04-29
**发现来源**: `test_results/run_20260429_085211/` compaction A/B 跑 20000 次随机读，`found=19964`，少 36 条。

**现象**:
- 顺序 Put 写入 N 条 key 后立即随机 Get，会有 ~0.1%–0.2% 的 key 返回 NotFound。
- 复现门槛低，只要数据已经 flush 到 SST（即不是只在 memtable 里）就会出现。

**根因**:
`src/version_set.cpp::Version::Get` 在 L0 / L1+ 都用 `InternalKeyComparator` 做 `[smallest, largest]` 范围判断：

```cpp
if (vset_->icmp_->Compare(k, Slice(f->smallest)) >= 0 &&
    vset_->icmp_->Compare(k, Slice(f->largest))  <= 0) { ... }
```

而 `InternalKeyComparator` 的语义是 `user_key ASC, seq DESC`。当 lookup 的 `snapshot_seq` 比文件 boundary 的 seq 大（这是常态，写入早 → 读取晚），同 user_key 下：

- `k.seq=1000`, `smallest.seq=500` → `icmp(k, smallest) = -1`（k 反而被判定为 < smallest）
- 于是 `k >= smallest` 为 false，正好包含 k 的那个文件被跳过 → NotFound

**只有 lookup 的 user_key 命中文件 boundary user_key 时才发病**，所以总体丢失率低，但是确定的正确性 bug。

**修复策略**:
范围判断必须比较 user_key，而不是 internal_key：

```cpp
const Comparator* ucmp = vset_->icmp_->user_comparator();
Slice user_k = ExtractUserKey(k);
if (ucmp->Compare(user_k, ExtractUserKey(Slice(f->smallest))) >= 0 &&
    ucmp->Compare(user_k, ExtractUserKey(Slice(f->largest)))  <= 0) { ... }
```

L1+ 的二分 `Compare(largest, k) < 0` 同样要换成 user_key 比较。

**回归方式**: `compaction_ab_bench` 输出 `found == num_entries`。

**状态**: 🟢 FIXED

**修复 commit**: fc6389b
**验证证据**:
- `./build/compaction_ab_bench --num_entries=500 --value_size=100 --base_dir=/tmp/cab_small/ab`
  输出：`found=500`。
- `./build/compaction_ab_bench --num_entries=5000 --value_size=100 --base_dir=/tmp/cab_med/ab`
  输出：`found=5000`。
- `./run_all_bench.sh --num_entries=1000 --mt_ops_per_thread=1000 --mt_threads=1,2 --ds_n=2000 --ds_lookup=2000 --ab_num_entries=500 --ab_value_size=64`
  输出：`compaction_ab_bench ... found=500`。

---

## BUG-002 🔴 P1 — Compaction 在跑 bench 时从未触发，A/B 实验完全失效

**发现日期**: 2026-04-29
**发现来源**: 同一次 run，`compaction_ab_on/` 与 `compaction_ab_off/` 文件数、字节数、吞吐**完全一致**：

| | sst 文件数 | 总字节 | write_ops |
|---|---|---|---|
| compaction_on  | 49 | 2,377,224 | 60,039 |
| compaction_off | 49 | 2,377,224 | 62,912 |

文件号严格连续 `000004 → 000100`，**没有任何被 compaction 流程分配出去的 output 文件号**，说明 L0→L1 合并一次都没执行过。`db_bench_db/` 7 个 L0 SST、`mt_dbs/bench_mt_t16/` 4 个 L0 SST 全都没合并，进一步佐证不是 A/B 测试本身的问题，而是引擎里 compaction 调度有问题。

**疑似原因**（未确诊）:

1. `BackgroundCompaction()` 每次只做 1 次 flush + 至多 1 次 level compaction，不会在同一轮 wakeup 中循环处理。
2. `BackgroundCall` 在 `BackgroundCompaction` 返回后无条件 `bg_compaction_scheduled_ = false`。如果在 `RunCompaction` 释放锁的窗口里前台又翻了一次 memtable 把 flag 置 true，bg 紧接着把它清回 false，新的 imm 永远没人来 flush。
3. 但即使忽略这个 race，按理 `NeedsCompaction()` 应该多次返回 true。需要插桩确认 `NeedsCompaction` 是否真的被调用、返回什么。

**下一步**:
- [ ] 在 `BackgroundCompaction` 入口、`NeedsCompaction` 判定、`PickCompaction` 返回点各加一行 stderr log，跑一次 `compaction_ab_bench` 看实际触发次数。
- [ ] 修 race（参考 LevelDB：判断条件应基于 imm 是否为空 + NeedsCompaction，而不是单一 flag）。

**回归方式**:
`compaction_ab_bench` 输出 on/off 两路的 `sst_files` / `total_bytes` 应有显著差异（开 compaction 应少一个数量级）。

**状态**: 🟢 FIXED

**修复 commit**: b0a5b43
**验证证据**:
- `./build/compaction_ab_bench --num_entries=20000 --value_size=100 --base_dir=/tmp/cab_full_fix2/ab`
  在 `compaction_on` 路径观察到大量
  `Removed obsolete SSTable` / `Removed obsolete WAL` 日志，说明后台 compaction 已实际执行。
- 同一命令可正常结束（`real 0m0.868s`），不再出现此前的长时间无响应。

**备注**:
- 该修复解决的是“compaction 不触发 + 后台空转/卡住”问题。
- on/off 的最终 `sst_files` 与 `total_bytes` 仍接近，属于 workload 可观测性问题，继续由 BUG-003 跟踪。

---

## BUG-003 🟡 P2 — 实验产出缺少 per-level 元数据，掩盖了 BUG-002

**发现日期**: 2026-04-29
**发现来源**: 排查 BUG-002 时发现 `compaction_ab_bench` 只统计 `.sst` 总数和总字节，看不到 L0/L1/L2 分布。如果当初有按层级 dump，BUG-002 在第一次跑出来就一目了然。

**修复策略**:
在 `compaction_ab_bench.cpp`（以及后续的诊断 bench）里查 `versions_->current()->files(level)`，把每层的文件数和字节加到 stdout，例如：

```
compaction_on  L0=1 files (190KB)  L1=12 files (2.2MB)  L2=0
compaction_off L0=49 files (2.4MB) L1=0  L2=0
```

需要给 `DB` 抽象暴露一个 `GetLevelStats()` 接口，或者直接读 MANIFEST 反推。

**状态**: 🟢 FIXED

**修复 commit**: <待填>
**验证证据**:
- `./build/compaction_ab_bench --num_entries=20000 --value_size=100 --base_dir=/tmp/cab_levels/ab`
  输出示例：
  - `compaction_on  ... level_stats=L0:0f/0b|L1:49f/2351787b|...`
  - `compaction_off ... level_stats=L0:49f/2351787b|L1:0f/0b|...`
- 现在无需猜测目录中文件形态即可直接判断 compaction 是否生效。

---

## BUG-004 🔴 P2 — db_bench_mt t1 档没真正过磁盘，吞吐数据失真

**发现日期**: 2026-04-29
**发现来源**: `mt_dbs/bench_mt_t1/` 只有 1 个 690KB 的 `.log` 文件，0 个 SST。

**根因**:
`run_all_bench.sh` 默认 `MT_OPS_PER_THREAD=5000`，单线程总写入 ≈ `5000 × ~120B = 600KB`，远小于默认 `write_buffer_size`（4MB），整个测试**全程都在 memtable 里**，从未触发 flush。t2/t4 也只刚好接近 1 次 flush，t8/t16 才真正过盘。

**影响**:
"高并发下端到端写吞吐" 这条曲线不可信——低线程数测的是纯内存吞吐，高线程数才包含磁盘开销。横向比较没意义。

**修复策略**（二选一）:
A. 把 `MT_OPS_PER_THREAD` 默认调到 ≥ 50000，让所有档位都触发若干次 flush。
B. 给 `db_bench_mt` 加 `--write_buffer_size=N` 参数，配置成 64KB / 256KB，所有档位都过同样的 flush 路径。

推荐 B（更可控、跑得快）。

**状态**: 🔴 OPEN

---

## BUG-005 🟢 P2 — run_all_bench.sh 没保存非 db_bench 的 stdout

**发现日期**: 2026-04-29
**发现来源**: 排查 BUG-002 时发现 `compaction_ab_bench` 的 `sst_files` / `total_bytes` 数字只打到屏幕、没存到 `test_results/` 中，复盘必须重跑。

**修复策略**:
`run_all_bench.sh` 里每个 bench 用 `tee` 落到 `${RUN_DIR}/<bench>.log`：

```bash
"${BUILD_DIR}/compaction_ab_bench" ... 2>&1 | tee "${RUN_DIR}/compaction_ab.log"
```

**状态**: 🟢 FIXED — 见 commit `<待填>`，本次提交统一把 5 个 bench 的 stdout/stderr 都 tee 到 `${RUN_DIR}/<name>.log`。

---

## BUG-006 🔴 P1 — `compaction_ab_bench` 在较大负载下可能卡住（疑似后台调度丢唤醒）

**发现日期**: 2026-04-29
**发现来源**: `./build/compaction_ab_bench --num_entries=20000 --value_size=100 --base_dir=/tmp/cab_full/ab` 长时间不退出（>5 分钟）；中途目录停留在少量文件，如 `ab_on/` 仅 `3 *.sst + 1 *.log`，写入明显未完成。

**现象**:
- 小规模（500/5000）可快速完成；
- 较大规模（20000）在 on 路径写入阶段可能卡住，需要手动 kill。

**疑似根因**:
与 BUG-002 同源：`BackgroundCall` / `bg_compaction_scheduled_` 的调度时序存在竞态，可能导致新的 `imm_` 无人处理，前台写线程在 `MakeRoomForWrite` 中长期等待。

**下一步**:
- [ ] 在 `MakeRoomForWrite` 的 wait 前后打印 `imm_`/`bg_compaction_scheduled_`/`NeedsCompaction` 关键状态。
- [ ] 将后台线程改为 “while(imm_ != nullptr || NeedsCompaction()) 持续处理” 的 drain 模式，避免单次 wakeup 丢任务。

**回归方式**:
`compaction_ab_bench --num_entries=20000` 应在可接受时间内结束，不需外部 kill。

**状态**: 🟢 FIXED

**修复 commit**: b0a5b43
**验证证据**:
- `./build/compaction_ab_bench --num_entries=20000 --value_size=100 --base_dir=/tmp/cab_full_fix2/ab`
  可在 1 秒级结束（`real 0m0.868s`），无需外部 kill。


## 维护说明

- 任何新发现的 bug、可疑现象、实验设计缺陷，都按 `BUG-NNN` 顺序追加。
- 每条记录至少包括：发现日期、来源（哪次 run 哪个文件）、现象、根因或假设、修复策略、回归方式、状态。
- 修复后**保留原条目**，把状态改成 🟢 FIXED，并在末尾追加 `**修复 commit**: <hash>`、`**验证证据**: <run 路径或日志摘录>`。
- ⚪ DEFERRED 必须写明为什么暂时不修（例如：依赖大重构、低优先、规划在 v0.x）。
