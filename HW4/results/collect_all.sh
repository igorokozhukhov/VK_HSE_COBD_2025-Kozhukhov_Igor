#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/submission}"
STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${OUT_DIR}/run_${STAMP}"

# Fast defaults to "verify no errors". You can override with env vars.
EX1_ROWS="${EX1_ROWS:-1000000}"
DURATION_SEC="${DURATION_SEC:-120}"
PERSON_ROWS="${PERSON_ROWS:-1000000}"
RUNS="${RUNS:-3}"

mkdir -p "${RUN_DIR}"
cd "${ROOT_DIR}"

echo "Collecting ALL exercises into: ${RUN_DIR}"

run_step() {
  local name="$1"
  shift
  echo "==> ${name}"
  # capture stdout+stderr
  ( "$@" ) > "${RUN_DIR}/${name}.txt" 2>&1
}

{
  echo "### timestamp: ${STAMP}"
  echo "### EX1_ROWS=${EX1_ROWS}"
  echo "### DURATION_SEC=${DURATION_SEC}"
  echo "### PERSON_ROWS=${PERSON_ROWS}"
  echo "### RUNS=${RUNS}"
  echo
  echo "### uname:"
  uname -a || true
  echo
  echo "### docker compose ps:"
  docker compose ps
} > "${RUN_DIR}/env.txt" 2>&1 || true

docker compose up -d > "${RUN_DIR}/compose_up.txt" 2>&1 || true
"${ROOT_DIR}/scripts/wait_clickhouse.sh" > "${RUN_DIR}/wait_clickhouse.txt" 2>&1

# Copy SQL/scripts snapshots used
mkdir -p "${RUN_DIR}/sql" "${RUN_DIR}/scripts"
cp -R "${ROOT_DIR}/sql/" "${RUN_DIR}/" 2>/dev/null || true
cp -R "${ROOT_DIR}/scripts/" "${RUN_DIR}/" 2>/dev/null || true

run_step "ex1_run" env EX1_ROWS="${EX1_ROWS}" "${ROOT_DIR}/scripts/ex1_run.sh"
run_step "ex1_checks" "${ROOT_DIR}/scripts/ex1_checks.sh"
run_step "ex1_metrics" "${ROOT_DIR}/scripts/ex1_metrics.sh"

run_step "ex2_run" env DURATION_SEC="${DURATION_SEC}" "${ROOT_DIR}/scripts/ex2_run.sh"

run_step "ex3_generate" env PERSON_ROWS="${PERSON_ROWS}" "${ROOT_DIR}/scripts/ex3_generate.sh"
run_step "ex3_benchmark_base" env RUNS="${RUNS}" TABLE="dz4.person_data" PREFIX="dz4_ex3_base_${STAMP}" "${ROOT_DIR}/scripts/ex3_benchmark.sh"

run_step "ex3_create_opt_table" "${ROOT_DIR}/scripts/ex3_create_opt_table.sh"
run_step "ex3_benchmark_opt" env RUNS="${RUNS}" TABLE="dz4.person_data_opt" PREFIX="dz4_ex3_opt_${STAMP}" "${ROOT_DIR}/scripts/ex3_benchmark.sh"

run_step "ex4_create_codec_table" "${ROOT_DIR}/scripts/ex4_create_codec_table.sh"
run_step "ex4_benchmark_codec" env RUNS="${RUNS}" TABLE="dz4.person_data_opt_codec" PREFIX="dz4_ex4_codec_${STAMP}" "${ROOT_DIR}/scripts/ex3_benchmark.sh"

# Docker logs tail
docker logs --tail=300 dz4-clickhouse1 > "${RUN_DIR}/clickhouse1_docker_logs_tail.txt" 2>&1 || true
docker logs --tail=300 dz4-clickhouse2 > "${RUN_DIR}/clickhouse2_docker_logs_tail.txt" 2>&1 || true
docker logs --tail=300 dz4-clickhouse3 > "${RUN_DIR}/clickhouse3_docker_logs_tail.txt" 2>&1 || true
docker logs --tail=300 dz4-zookeeper   > "${RUN_DIR}/zookeeper_docker_logs_tail.txt" 2>&1 || true

echo
echo "DONE."
echo "1) Add your DBeaver screenshot file into:"
echo "   ${RUN_DIR}/"
echo "2) Create zip:"
echo "   cd \"${OUT_DIR}\" && zip -r \"dz4_submission_${STAMP}.zip\" \"run_${STAMP}\""
echo
echo "Tip: For full-scale homework (can be long/heavy):"
echo "  EX1_ROWS=10000000 PERSON_ROWS=100000000 DURATION_SEC=600 RUNS=5 make collect-all"


