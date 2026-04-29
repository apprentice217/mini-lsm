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
BUILD_TYPE="${BUILD_TYPE:-Release}"
NUM_ENTRIES="${NUM_ENTRIES:-100000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
MT_OPS_PER_THREAD="${MT_OPS_PER_THREAD:-5000}"
MT_THREADS="${MT_THREADS:-1,2,4,8,16}"
VALUE_SIZE="${VALUE_SIZE:-100}"
DS_N="${DS_N:-50000}"
DS_LOOKUP="${DS_LOOKUP:-50000}"
DS_SEED="${DS_SEED:-42}"
AB_NUM_ENTRIES="${AB_NUM_ENTRIES:-20000}"
AB_VALUE_SIZE="${AB_VALUE_SIZE:-100}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build_type=*) BUILD_TYPE="${1#*=}" ;;
    --num_entries=*) NUM_ENTRIES="${1#*=}" ;;
    --batch_size=*) BATCH_SIZE="${1#*=}" ;;
    --mt_ops_per_thread=*) MT_OPS_PER_THREAD="${1#*=}" ;;
    --mt_threads=*) MT_THREADS="${1#*=}" ;;
    --value_size=*) VALUE_SIZE="${1#*=}" ;;
    --ds_n=*) DS_N="${1#*=}" ;;
    --ds_lookup=*) DS_LOOKUP="${1#*=}" ;;
    --ds_seed=*) DS_SEED="${1#*=}" ;;
    --ab_num_entries=*) AB_NUM_ENTRIES="${1#*=}" ;;
    --ab_value_size=*) AB_VALUE_SIZE="${1#*=}" ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

echo "==> Configure & build (${BUILD_TYPE})"
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
cmake --build "${BUILD_DIR}" -j4

mkdir -p "${RUN_DIR}" "${MT_DIR}"
echo "==> Results directory: ${RUN_DIR}"

echo "==> [1/6] db_correctness"
"${BUILD_DIR}/db_correctness"

echo "==> [2/6] db_bench"
"${BUILD_DIR}/db_bench" \
  --num_entries="${NUM_ENTRIES}" \
  --batch_size="${BATCH_SIZE}" \
  --value_size="${VALUE_SIZE}" \
  --db_name="${BASELINE_DB_DIR}" \
  --output_csv="${CSV_PATH}"

echo "==> [3/6] db_bench_mt (threads: ${MT_THREADS})"
IFS=',' read -r -a THREAD_LIST <<< "${MT_THREADS}"
for t in "${THREAD_LIST[@]}"; do
  echo "  -> threads=${t}"
  "${BUILD_DIR}/db_bench_mt" \
    --threads="${t}" \
    --ops_per_thread="${MT_OPS_PER_THREAD}" \
    --value_size="${VALUE_SIZE}" \
    --db_name="${MT_DIR}/bench_mt_t${t}"
done

echo "==> [4/6] memtable_ds_bench"
"${BUILD_DIR}/memtable_ds_bench" \
  --n="${DS_N}" \
  --lookup="${DS_LOOKUP}" \
  --seed="${DS_SEED}"

echo "==> [5/6] compaction_ab_bench"
"${BUILD_DIR}/compaction_ab_bench" \
  --num_entries="${AB_NUM_ENTRIES}" \
  --value_size="${AB_VALUE_SIZE}" \
  --base_dir="${AB_BASE_DIR}"

echo "==> Done."
echo "Results CSV: ${CSV_PATH}"
