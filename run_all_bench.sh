#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
RESULTS_ROOT="${ROOT_DIR}/test_results"
RUN_DIR="${RESULTS_ROOT}/run_$(date +%Y%m%d_%H%M%S)"
CSV_PATH="${RUN_DIR}/benchmark_results.csv"
MT_DIR="${RUN_DIR}/mt_dbs"
AB_BASE_DIR="${RUN_DIR}/compaction_ab"
BASELINE_DB_DIR="${RUN_DIR}/db_bench_db"

# Defaults (can be overridden by environment variables or CLI args)
# 更大数据量 + 默认 WAL fsync（sync_write=1）更接近“持久化写入”语义；吞吐会显著低于仅写 page cache。
# 快速迭代：SYNC_WRITE=0 ./run_all_bench.sh 或减小 NUM_ENTRIES / MT_OPS_PER_THREAD。
BUILD_TYPE="${BUILD_TYPE:-Release}"
SYNC_WRITE="${SYNC_WRITE:-1}"
NUM_ENTRIES="${NUM_ENTRIES:-500000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
MT_OPS_PER_THREAD="${MT_OPS_PER_THREAD:-8000}"
MT_THREADS="${MT_THREADS:-1,2,4,8,16}"
MT_WRITE_BUFFER_SIZE="${MT_WRITE_BUFFER_SIZE:-262144}"
VALUE_SIZE="${VALUE_SIZE:-100}"
DS_N="${DS_N:-200000}"
DS_LOOKUP="${DS_LOOKUP:-200000}"
DS_SEED="${DS_SEED:-42}"
AB_NUM_ENTRIES="${AB_NUM_ENTRIES:-80000}"
AB_VALUE_SIZE="${AB_VALUE_SIZE:-100}"
AB_CHURN_ROUNDS="${AB_CHURN_ROUNDS:-3}"
AB_HOT_KEY_SPACE="${AB_HOT_KEY_SPACE:-8000}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build_type=*) BUILD_TYPE="${1#*=}" ;;
    --num_entries=*) NUM_ENTRIES="${1#*=}" ;;
    --batch_size=*) BATCH_SIZE="${1#*=}" ;;
    --mt_ops_per_thread=*) MT_OPS_PER_THREAD="${1#*=}" ;;
    --mt_threads=*) MT_THREADS="${1#*=}" ;;
    --mt_write_buffer_size=*) MT_WRITE_BUFFER_SIZE="${1#*=}" ;;
    --value_size=*) VALUE_SIZE="${1#*=}" ;;
    --ds_n=*) DS_N="${1#*=}" ;;
    --ds_lookup=*) DS_LOOKUP="${1#*=}" ;;
    --ds_seed=*) DS_SEED="${1#*=}" ;;
    --ab_num_entries=*) AB_NUM_ENTRIES="${1#*=}" ;;
    --ab_value_size=*) AB_VALUE_SIZE="${1#*=}" ;;
    --ab_churn_rounds=*) AB_CHURN_ROUNDS="${1#*=}" ;;
    --ab_hot_key_space=*) AB_HOT_KEY_SPACE="${1#*=}" ;;
    --sync_write=*) SYNC_WRITE="${1#*=}" ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

case "${SYNC_WRITE}" in
  0|1) ;;
  *)
    echo "sync_write must be 0 or 1 (got ${SYNC_WRITE})" >&2
    exit 1
    ;;
esac

echo "==> Bench defaults: num_entries=${NUM_ENTRIES} batch_size=${BATCH_SIZE} sync_write=${SYNC_WRITE} | mt_ops/thread=${MT_OPS_PER_THREAD} | ab_num_entries=${AB_NUM_ENTRIES} ab_hot_key_space=${AB_HOT_KEY_SPACE} | ds_n=${DS_N}"

echo "==> Configure & build (${BUILD_TYPE})"
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
cmake --build "${BUILD_DIR}" -j4

mkdir -p "${RUN_DIR}" "${MT_DIR}"
echo "==> Results directory: ${RUN_DIR}"

# 所有 bench 的 stdout/stderr 都 tee 到 ${RUN_DIR}/<name>.log，方便复盘 (BUG-005)
# pipefail 已经在脚本顶部 set，pipe 任何一段失败都会让脚本退出。
echo "==> [1/6] db_correctness  (log: db_correctness.log)"
"${BUILD_DIR}/db_correctness" 2>&1 | tee "${RUN_DIR}/db_correctness.log"

echo "==> [2/6] db_bench  (log: db_bench.log)"
"${BUILD_DIR}/db_bench" \
  --num_entries="${NUM_ENTRIES}" \
  --batch_size="${BATCH_SIZE}" \
  --value_size="${VALUE_SIZE}" \
  "--sync_write=${SYNC_WRITE}" \
  --db_name="${BASELINE_DB_DIR}" \
  --output_csv="${CSV_PATH}" 2>&1 | tee "${RUN_DIR}/db_bench.log"

echo "==> [3/6] db_bench_mt  (threads: ${MT_THREADS})"
IFS=',' read -r -a THREAD_LIST <<< "${MT_THREADS}"
for t in "${THREAD_LIST[@]}"; do
  echo "  -> threads=${t}  (log: db_bench_mt_t${t}.log)"
  "${BUILD_DIR}/db_bench_mt" \
    --threads="${t}" \
    --ops_per_thread="${MT_OPS_PER_THREAD}" \
    --value_size="${VALUE_SIZE}" \
    --write_buffer_size="${MT_WRITE_BUFFER_SIZE}" \
    "--sync_write=${SYNC_WRITE}" \
    --db_name="${MT_DIR}/bench_mt_t${t}" 2>&1 | tee "${RUN_DIR}/db_bench_mt_t${t}.log"
done

echo "==> [4/6] memtable_ds_bench  (log: memtable_ds_bench.log)"
"${BUILD_DIR}/memtable_ds_bench" \
  --n="${DS_N}" \
  --lookup="${DS_LOOKUP}" \
  --seed="${DS_SEED}" 2>&1 | tee "${RUN_DIR}/memtable_ds_bench.log"

echo "==> [5/6] compaction_ab_bench  (log: compaction_ab_bench.log)"
"${BUILD_DIR}/compaction_ab_bench" \
  --num_entries="${AB_NUM_ENTRIES}" \
  --value_size="${AB_VALUE_SIZE}" \
  --churn_rounds="${AB_CHURN_ROUNDS}" \
  --hot_key_space="${AB_HOT_KEY_SPACE}" \
  "--sync_write=${SYNC_WRITE}" \
  --base_dir="${AB_BASE_DIR}" 2>&1 | tee "${RUN_DIR}/compaction_ab_bench.log"

echo "==> Done."
echo "Results CSV: ${CSV_PATH}"
echo "All logs:    ${RUN_DIR}/*.log"
