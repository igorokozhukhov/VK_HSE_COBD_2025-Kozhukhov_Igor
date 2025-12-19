#!/usr/bin/env bash
set -euo pipefail

SERVICE="${CH_SERVICE:-clickhouse1}"
MAX_SECONDS="${MAX_SECONDS:-120}"

start="$(date +%s)"
while true; do
  if docker compose exec -T "${SERVICE}" clickhouse-client -q "SELECT 1" >/dev/null 2>&1; then
    exit 0
  fi
  now="$(date +%s)"
  if (( now - start >= MAX_SECONDS )); then
    echo "ClickHouse (${SERVICE}) is not ready after ${MAX_SECONDS}s" >&2
    exit 1
  fi
  sleep 2
done


