#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DURATION_SEC="${DURATION_SEC:-600}"          
SMALL_BATCH_ROWS="${SMALL_BATCH_ROWS:-50}"   
SMALL_SLEEP_SEC="${SMALL_SLEEP_SEC:-0.2}"

BIG_BATCH_ROWS="${BIG_BATCH_ROWS:-50000}"    
BIG_SLEEP_SEC="${BIG_SLEEP_SEC:-3}"

docker compose up -d
"${ROOT_DIR}/scripts/wait_clickhouse.sh"

echo "Creating Exercise 2 tables..."
"${ROOT_DIR}/scripts/ch.sh" --multiquery < "${ROOT_DIR}/sql/ex2_schema.sql"

echo "Starting insert experiment for ${DURATION_SEC}s..."
end_ts=$(( $(date +%s) + DURATION_SEC ))

small_loop() {
  while [ "$(date +%s)" -lt "${end_ts}" ]; do
    "${ROOT_DIR}/scripts/ch.sh" -nq "
      INSERT INTO dz4.small_target
      SELECT
        now64(3) AS dt_create,
        *
      FROM generateRandom(
        'int_1 UInt64, int_2 UInt64, int_3 UInt64, int_4 UInt64, int_5 UInt64,
         str_1 String, str_2 String, str_3 String, str_4 String, str_5 String,
         dt_1 DateTime64(3), dt_2 DateTime64(3), dt_3 DateTime64(3), dt_4 DateTime64(3), dt_5 DateTime64(3),
         uuid_1 UUID, uuid_2 UUID, uuid_3 UUID, uuid_4 UUID, uuid_5 UUID'
      )
      LIMIT ${SMALL_BATCH_ROWS};
    " >/dev/null
    sleep "${SMALL_SLEEP_SEC}"
  done
}

big_loop() {
  while [ "$(date +%s)" -lt "${end_ts}" ]; do
    "${ROOT_DIR}/scripts/ch.sh" -nq "
      INSERT INTO dz4.big_buffer
      SELECT
        now64(3) AS dt_create,
        *
      FROM generateRandom(
        'int_1 UInt64, int_2 UInt64, int_3 UInt64, int_4 UInt64, int_5 UInt64,
         str_1 String, str_2 String, str_3 String, str_4 String, str_5 String,
         dt_1 DateTime64(3), dt_2 DateTime64(3), dt_3 DateTime64(3), dt_4 DateTime64(3), dt_5 DateTime64(3),
         uuid_1 UUID, uuid_2 UUID, uuid_3 UUID, uuid_4 UUID, uuid_5 UUID'
      )
      LIMIT ${BIG_BATCH_ROWS};
    " >/dev/null
    sleep "${BIG_SLEEP_SEC}"
  done
}

small_loop & pid1=$!
big_loop & pid2=$!

wait "${pid1}" "${pid2}"

echo "Insert experiment finished. Flushing buffer and logs..."
"${ROOT_DIR}/scripts/ch.sh" -nq "SYSTEM FLUSH LOGS;" >/dev/null || true
sleep 5

echo
echo "Final row counts:"
"${ROOT_DIR}/scripts/ch.sh" -q "
  SELECT 'small_target' AS table, count() AS rows FROM dz4.small_target
  UNION ALL
  SELECT 'big_target' AS table, count() AS rows FROM dz4.big_target;
"

echo
echo "Parts (active/inactive):"
"${ROOT_DIR}/scripts/ch.sh" -q "
  SELECT
      table,
      countIf(active = 1) AS active_parts,
      countIf(active = 0) AS inactive_parts
  FROM system.parts
  WHERE database = 'dz4' AND table IN ('small_target', 'big_target')
  GROUP BY table
  ORDER BY table;
"


