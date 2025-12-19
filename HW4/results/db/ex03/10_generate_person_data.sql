
CREATE DATABASE IF NOT EXISTS dz4;

DROP TABLE IF EXISTS dz4.person_data;
CREATE TABLE dz4.person_data
(
  id          UInt64,
  region      LowCardinality(String),
  date_birth  Date,
  gender      UInt8,
  is_marital  UInt8,
  dt_create   DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (date_birth);

INSERT INTO dz4.person_data (id, region, date_birth, gender, is_marital)
WITH
    modulo(number, 70) + 20 AS n,
    floor(randNormal(10000, 1700)) AS k,
    (toDate('1970-01-01') + toIntervalDay(k)) AS birth
SELECT
    rand64() AS id,
    toString(n) AS region,
    toStartOfDay(birth) AS date_birth,
    if(modulo(number, 3) = 1, 1, 0) AS gender,
    if((n + k) % 3 = 0 AND dateDiff('year', birth, now()) > 18, 1, 0) AS is_marital
FROM numbers({{PERSON_ROWS}});


