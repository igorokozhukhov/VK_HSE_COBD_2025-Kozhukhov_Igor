#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/submission}"
STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${OUT_DIR}/run_${STAMP}"

mkdir -p "${RUN_DIR}"

cd "${ROOT_DIR}"

echo "Collecting results into: ${RUN_DIR}"

{
  echo "### timestamp: ${STAMP}"
  echo "### uname:"
  uname -a || true
  echo
  echo "### docker compose ps:"
  docker compose ps
  echo
  echo "### docker images (clickhouse/zookeeper):"
  docker images | egrep -i 'clickhouse|zookeeper' || true
} > "${RUN_DIR}/env.txt"

# Ensure services are up
docker compose up -d > "${RUN_DIR}/compose_up.txt" 2>&1 || true
"${ROOT_DIR}/scripts/wait_clickhouse.sh" > "${RUN_DIR}/wait_clickhouse.txt" 2>&1

# Versions / cluster info
"${ROOT_DIR}/scripts/ch.sh" -q "SELECT version() AS version, hostName() AS host" > "${RUN_DIR}/ch_version.txt"
"${ROOT_DIR}/scripts/ch.sh" -q "SELECT * FROM system.clusters ORDER BY cluster, shard_num, replica_num" > "${RUN_DIR}/system_clusters.tsv"
"${ROOT_DIR}/scripts/ch.sh" -q "SELECT * FROM system.macros ORDER BY macro" > "${RUN_DIR}/system_macros.tsv"

# Exercise 1: checks + metrics
"${ROOT_DIR}/scripts/ex1_checks.sh" > "${RUN_DIR}/ex1_checks.txt" 2>&1 || true
"${ROOT_DIR}/scripts/ex1_metrics.sh" > "${RUN_DIR}/ex1_metrics.txt" 2>&1 || true

# Helpful: show created tables in dz4
"${ROOT_DIR}/scripts/ch.sh" -q "SHOW DATABASES" > "${RUN_DIR}/show_databases.txt"
"${ROOT_DIR}/scripts/ch.sh" -q "SHOW TABLES FROM dz4" > "${RUN_DIR}/show_tables_dz4.txt" 2>&1 || true

# Keep a copy of SQL used
mkdir -p "${RUN_DIR}/sql"
cp -R "${ROOT_DIR}/sql/ex1.sql" "${ROOT_DIR}/sql/ex1_checks.sql" "${ROOT_DIR}/sql/ex1_metrics.sql" "${RUN_DIR}/sql/" 2>/dev/null || true

# Logs (last lines)
docker logs --tail=300 dz4-clickhouse1 > "${RUN_DIR}/clickhouse1_docker_logs_tail.txt" 2>&1 || true
docker logs --tail=300 dz4-clickhouse2 > "${RUN_DIR}/clickhouse2_docker_logs_tail.txt" 2>&1 || true
docker logs --tail=300 dz4-clickhouse3 > "${RUN_DIR}/clickhouse3_docker_logs_tail.txt" 2>&1 || true
docker logs --tail=300 dz4-zookeeper   > "${RUN_DIR}/zookeeper_docker_logs_tail.txt" 2>&1 || true

echo
echo "DONE."
echo "Now add your DBeaver screenshot file into: ${RUN_DIR}/"
echo "Then create zip:"
echo "  cd \"${OUT_DIR}\" && zip -r \"dz4_submission_${STAMP}.zip\" \"run_${STAMP}\""


