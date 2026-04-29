#!/usr/bin/env bash
# clean_results.sh — 清理 test_results/ 下的测试产物
#
# 用法：
#   ./clean_results.sh          删除全部 test_results/
#   ./clean_results.sh --days=N 仅删除 N 天前的 run_* 历史记录，保留最新
#   ./clean_results.sh --dry-run 预览将要删除的内容，不实际删除

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_ROOT="${ROOT_DIR}/test_results"

KEEP_DAYS=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days=*) KEEP_DAYS="${1#*=}" ;;
    --dry-run) DRY_RUN=true ;;
    -h|--help)
      echo "Usage: $0 [--days=N] [--dry-run]"
      echo "  (no args)    Delete all of test_results/"
      echo "  --days=N     Delete run_* directories older than N days"
      echo "  --dry-run    Preview only, do not delete anything"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

do_remove() {
  local target="$1"
  if [[ "${DRY_RUN}" == true ]]; then
    echo "[dry-run] would remove: ${target}  ($(du -sh "${target}" 2>/dev/null | cut -f1))"
  else
    echo "Removing: ${target}"
    rm -rf "${target}"
  fi
}

if [[ ! -d "${RESULTS_ROOT}" ]]; then
  echo "Nothing to clean: ${RESULTS_ROOT} does not exist."
  exit 0
fi

if [[ -z "${KEEP_DAYS}" ]]; then
  do_remove "${RESULTS_ROOT}"
else
  shopt -s nullglob
  for d in "${RESULTS_ROOT}"/run_* "${RESULTS_ROOT}"/legacy \
            "${RESULTS_ROOT}"/db_bench_default \
            "${RESULTS_ROOT}"/db_bench_mt_default \
            "${RESULTS_ROOT}"/compaction_ab_default; do
    [[ -d "$d" ]] || continue
    if [[ $(find "$d" -maxdepth 0 -mtime +"${KEEP_DAYS}" 2>/dev/null | wc -l) -gt 0 ]]; then
      do_remove "$d"
    fi
  done
fi

if [[ "${DRY_RUN}" == false && -z "${KEEP_DAYS}" ]]; then
  echo "Done. test_results/ removed."
else
  echo "Done."
fi
