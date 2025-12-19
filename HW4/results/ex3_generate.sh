#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"


PERSON_ROWS="${PERSON_ROWS:-10000000}"

docker compose up -d
"${ROOT_DIR}/scripts/wait_clickhouse.sh"

echo "Generating dz4.person_data with PERSON_ROWS=${PERSON_ROWS} (this can take a while)..."
sed "s/{{PERSON_ROWS}}/${PERSON_ROWS}/g" "${ROOT_DIR}/sql/ex3_person_data.sql" | "${ROOT_DIR}/scripts/ch.sh" --multiquery

echo "Optimizing to reduce parts..."
"${ROOT_DIR}/scripts/ch.sh" -nq "OPTIMIZE TABLE dz4.person_data FINAL;" >/dev/null

echo "Parts + primary key memory:"
"${ROOT_DIR}/scripts/ch.sh" -q "
  SELECT
      countIf(active=1) AS active_parts,
      formatReadableSize(sum(primary_key_bytes_in_memory)) AS pk_mem,
      formatReadableSize(sum(data_compressed_bytes)) AS compressed
  FROM system.parts
  WHERE database='dz4' AND table='person_data' AND active=1;
"


