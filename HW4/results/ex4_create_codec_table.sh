#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SOURCE_TABLE="${SOURCE_TABLE:-dz4.person_data_opt}"
TARGET_TABLE="${TARGET_TABLE:-dz4.person_data_opt_codec}"

DB_SRC="${SOURCE_TABLE%.*}"
TB_SRC="${SOURCE_TABLE#*.}"
DB_TGT="${TARGET_TABLE%.*}"
TB_TGT="${TARGET_TABLE#*.}"

docker compose up -d
"${ROOT_DIR}/scripts/wait_clickhouse.sh"

echo "Creating codec-optimized table ${TARGET_TABLE} from ${SOURCE_TABLE}..."

"${ROOT_DIR}/scripts/ch.sh" -nq "
  CREATE DATABASE IF NOT EXISTS ${DB_TGT};
  DROP TABLE IF EXISTS ${TARGET_TABLE};
  CREATE TABLE ${TARGET_TABLE}
  (
    id          UInt64 CODEC(LZ4),
    region      LowCardinality(String) CODEC(ZSTD(3)),
    date_birth  Date CODEC(Delta, ZSTD(3)),
    gender      UInt8 CODEC(T64, LZ4),
    is_marital  UInt8 CODEC(T64, LZ4),
    dt_create   DateTime CODEC(DoubleDelta, LZ4)
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

echo
echo "Disk size comparison (compressed) between ${SOURCE_TABLE} and ${TARGET_TABLE}:"
"${ROOT_DIR}/scripts/ch.sh" -q "
  WITH
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE active=1 AND database='${DB_SRC}' AND table='${TB_SRC}') AS src_bytes,
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE active=1 AND database='${DB_TGT}' AND table='${TB_TGT}') AS tgt_bytes
  SELECT
    formatReadableSize(src_bytes) AS src_compressed,
    formatReadableSize(tgt_bytes) AS tgt_compressed,
    round((src_bytes - tgt_bytes) / nullIf(src_bytes, 0) * 100, 2) AS saved_pct;
"

echo
echo "Per-column codecs (system.columns):"
"${ROOT_DIR}/scripts/ch.sh" -q "
  SELECT
      name,
      type,
      compression_codec
  FROM system.columns
  WHERE database='${DB_TGT}' AND table='${TB_TGT}'
  ORDER BY position;
"


