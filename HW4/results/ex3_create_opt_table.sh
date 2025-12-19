#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SOURCE_TABLE="${SOURCE_TABLE:-dz4.person_data}"
TARGET_TABLE="${TARGET_TABLE:-dz4.person_data_opt}"

DB_SRC="${SOURCE_TABLE%.*}"
TB_SRC="${SOURCE_TABLE#*.}"
DB_TGT="${TARGET_TABLE%.*}"
TB_TGT="${TARGET_TABLE#*.}"

docker compose up -d
"${ROOT_DIR}/scripts/wait_clickhouse.sh"

echo "Creating optimized table ${TARGET_TABLE} from ${SOURCE_TABLE}..."

"${ROOT_DIR}/scripts/ch.sh" -nq "
  CREATE DATABASE IF NOT EXISTS ${DB_TGT};
  DROP TABLE IF EXISTS ${TARGET_TABLE};
  CREATE TABLE ${TARGET_TABLE}
  (
    id          UInt64,
    region      LowCardinality(String),
    date_birth  Date,
    gender      UInt8,
    is_marital  UInt8,
    dt_create   DateTime DEFAULT now()
  )
  ENGINE = MergeTree()
  ORDER BY (region, is_marital, date_birth);
"

"${ROOT_DIR}/scripts/ch.sh" -nq "
  INSERT INTO ${TARGET_TABLE}
  SELECT id, region, date_birth, gender, is_marital, dt_create
  FROM ${SOURCE_TABLE};
"

"${ROOT_DIR}/scripts/ch.sh" -nq "OPTIMIZE TABLE ${TARGET_TABLE} FINAL;" >/dev/null

echo "Size + pk memory for ${TB_TGT}:"
"${ROOT_DIR}/scripts/ch.sh" -q "
  SELECT
      formatReadableSize(sum(data_compressed_bytes)) AS compressed,
      formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
      formatReadableSize(sum(primary_key_bytes_in_memory)) AS pk_mem,
      count() AS parts,
      sum(rows) AS rows
  FROM system.parts
  WHERE active=1 AND database='${DB_TGT}' AND table='${TB_TGT}';
"


