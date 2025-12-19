

CREATE DATABASE IF NOT EXISTS dz4;

CREATE DATABASE IF NOT EXISTS dz4 ON CLUSTER cluster_1s_2r;


DROP TABLE IF EXISTS dz4.metrics_mt;
CREATE TABLE dz4.metrics_mt
(
    int_val   UInt16,
    uuid_val  UUID,
    dt_val    DateTime,
    str_val   FixedString(1)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt_val)
ORDER BY (str_val, int_val)
SETTINGS index_granularity = 8192;

INSERT INTO dz4.metrics_mt (int_val, uuid_val, dt_val, str_val)
SELECT
    modulo(rand(), 999) + 1                                  AS int_val,
    generateUUIDv4()                                         AS uuid_val,
    now() - INTERVAL rand() / 1000 SECOND                    AS dt_val,
    multiIf((rand() / 500000) <= 1500, 'A',
            (rand() / 500000) <= 3000, 'B',
            (rand() / 500000) <= 4500, 'C',
            (rand() / 500000) <= 6000, 'D',
            (rand() / 500000) <= 7300, 'E',
            'F')                                             AS str_val
FROM numbers({{EX1_ROWS}});


SELECT
    str_val,
    count()                      AS rows_cnt,
    uniqExact(int_val)           AS uniq_int_val,
    round(avg(int_val), 2)       AS avg_int_val
FROM dz4.metrics_mt
GROUP BY str_val
ORDER BY str_val;


DROP TABLE IF EXISTS dz4.rep_test ON CLUSTER cluster_1s_2r SYNC;
CREATE TABLE dz4.rep_test ON CLUSTER cluster_1s_2r
(
    ts          DateTime,
    contractid  UInt32,
    userid      UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster_1s_2r/{shard}/dz4/rep_test', '{replica}')
PARTITION BY toYYYYMM(ts)
ORDER BY (contractid, toDate(ts), userid);

INSERT INTO dz4.rep_test (ts, contractid, userid) VALUES (now(), 1, 1);
INSERT INTO dz4.rep_test (ts, contractid, userid) VALUES (now(), 2, 2);


SELECT hostName() AS host, count() AS rows_cnt
FROM clusterAllReplicas('cluster_1s_2r', dz4.rep_test)
GROUP BY host
ORDER BY host;

SYSTEM FLUSH LOGS;


