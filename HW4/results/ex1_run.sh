#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose up -d
"${ROOT_DIR}/scripts/wait_clickhouse.sh"

echo "Running exercise 1 SQL..."
EX1_ROWS="${EX1_ROWS:-1000000}"
sed "s/{{EX1_ROWS}}/${EX1_ROWS}/g" "${ROOT_DIR}/sql/ex1.sql" | "${ROOT_DIR}/scripts/ch.sh" --multiquery

echo
echo "Now run checks (system tables/functions):"
echo "  ${ROOT_DIR}/scripts/ex1_checks.sh"
echo
echo "Now run metrics queries:"
echo "  ${ROOT_DIR}/scripts/ex1_metrics.sh"


