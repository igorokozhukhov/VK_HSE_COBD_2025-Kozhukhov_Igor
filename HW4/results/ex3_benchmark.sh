#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUNS="${RUNS:-5}"
TABLE="${TABLE:-dz4.person_data}"
PREFIX="${PREFIX:-dz4_ex3}"

DB="${TABLE%.*}"
TB="${TABLE#*.}"

docker compose up -d
"${ROOT_DIR}/scripts/wait_clickhouse.sh"

q1="
SELECT
    t.region,
    countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
    countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
FROM ${TABLE} AS t
WHERE t.date_birth BETWEEN toDate('2000-01-01') AND toDate('2000-01-31')
  AND t.region IN ('20', '25', '43', '59')
GROUP BY t.region
ORDER BY t.region
FORMAT Null;
"

q2="
SELECT
    countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
    countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
FROM ${TABLE} AS t
WHERE t.is_marital = 1
  AND t.region IN ('80')
GROUP BY t.region
ORDER BY t.region
FORMAT Null;
"

echo "Benchmarking ${TABLE}: RUNS=${RUNS}"
for i in $(seq 1 "${RUNS}"); do
  "${ROOT_DIR}/scripts/ch.sh" --query_id "${PREFIX}_q1_${i}" -q "${q1}"
done
for i in $(seq 1 "${RUNS}"); do
  "${ROOT_DIR}/scripts/ch.sh" --query_id "${PREFIX}_q2_${i}" -q "${q2}"
done

"${ROOT_DIR}/scripts/ch.sh" -nq "SYSTEM FLUSH LOGS;" >/dev/null

echo
echo "Averages from system.query_log:"
"${ROOT_DIR}/scripts/ch.sh" -q "
  SELECT
      'q1' AS q,
      round(avg(query_duration_ms), 2) AS avg_ms,
      formatReadableSize(round(avg(memory_usage))) AS avg_mem,
      formatReadableQuantity(round(avg(read_rows))) AS avg_read_rows,
      formatReadableSize(round(avg(read_bytes))) AS avg_read_bytes
  FROM system.query_log
  WHERE type='QueryFinish' AND query_id LIKE '${PREFIX}_q1_%'
  UNION ALL
  SELECT
      'q2' AS q,
      round(avg(query_duration_ms), 2) AS avg_ms,
      formatReadableSize(round(avg(memory_usage))) AS avg_mem,
      formatReadableQuantity(round(avg(read_rows))) AS avg_read_rows,
      formatReadableSize(round(avg(read_bytes))) AS avg_read_bytes
  FROM system.query_log
  WHERE type='QueryFinish' AND query_id LIKE '${PREFIX}_q2_%';
"

echo
echo "Primary key memory (system.parts):"
"${ROOT_DIR}/scripts/ch.sh" -q "
  SELECT
      '${TABLE}' AS table,
      formatReadableSize(sum(primary_key_bytes_in_memory)) AS pk_mem,
      countIf(active=1) AS active_parts
  FROM system.parts
  WHERE active=1 AND database='${DB}' AND table='${TB}';
"


