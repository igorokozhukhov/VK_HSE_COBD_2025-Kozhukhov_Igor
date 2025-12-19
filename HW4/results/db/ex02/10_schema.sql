

CREATE DATABASE IF NOT EXISTS dz4;

DROP TABLE IF EXISTS dz4.small_target;
CREATE TABLE dz4.small_target
(
    dt_create DateTime64(3) DEFAULT now64(3),
    int_1 UInt64,
    int_2 UInt64,
    int_3 UInt64,
    int_4 UInt64,
    int_5 UInt64,
    str_1 String,
    str_2 String,
    str_3 String,
    str_4 String,
    str_5 String,
    dt_1 DateTime64(3),
    dt_2 DateTime64(3),
    dt_3 DateTime64(3),
    dt_4 DateTime64(3),
    dt_5 DateTime64(3),
    uuid_1 UUID,
    uuid_2 UUID,
    uuid_3 UUID,
    uuid_4 UUID,
    uuid_5 UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(dt_create))
ORDER BY (dt_create, int_1);

DROP TABLE IF EXISTS dz4.big_target;
CREATE TABLE dz4.big_target AS dz4.small_target
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(dt_create))
ORDER BY (dt_create, int_1);

DROP TABLE IF EXISTS dz4.big_buffer;
CREATE TABLE dz4.big_buffer AS dz4.big_target
ENGINE = Buffer(dz4, big_target, 1, 1, 3, 1, 10000, 1048576, 10485760);


