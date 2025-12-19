#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose up -d
"${ROOT_DIR}/scripts/wait_clickhouse.sh"

"${ROOT_DIR}/scripts/ch.sh" --multiquery < "${ROOT_DIR}/sql/ex1_metrics.sql"


