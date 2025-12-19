SELECT
    event_time,
    query_duration_ms,
    formatReadableSize(memory_usage) AS mem,
    formatReadableQuantity(read_rows) AS read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    query
FROM system.query_log
WHERE type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 20;


SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes))     AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes))   AS uncompressed,
    formatReadableSize(sum(primary_key_bytes_in_memory)) AS pk_mem,
    sum(rows) AS rows,
    count() AS parts
FROM system.parts
WHERE active = 1
  AND database = 'dz4'
  AND table IN ('metrics_mt', 'rep_test')
GROUP BY table
ORDER BY table;

SELECT
    table,
    name,
    formatReadableSize(sum(data_compressed_bytes))   AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 2) AS compr_rate
FROM system.columns
WHERE database = 'dz4'
  AND table = 'metrics_mt'
GROUP BY table, name
ORDER BY compressed DESC;


