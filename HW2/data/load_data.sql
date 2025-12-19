-- ============================================
-- Скрипт загрузки данных Netflix Titles в Hive
-- ============================================

-- 1. Создание базы данных
CREATE DATABASE IF NOT EXISTS netflix_hw;
USE netflix_hw;

-- 2. Staging (external) таблица для чтения CSV из HDFS
-- Формат CSV: show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
CREATE EXTERNAL TABLE IF NOT EXISTS stg_netflix_titles_csv (
  show_id      STRING,
  type         STRING,
  title        STRING,
  director     STRING,
  cast         STRING,
  country      STRING,
  date_added   STRING,
  release_year STRING,
  rating       STRING,
  duration     STRING,
  listed_in    STRING,
  description  STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/data/netflix_hw/stg_titles_csv';

-- 3. Основная таблица (Parquet) с базовыми преобразованиями типов
CREATE TABLE IF NOT EXISTS netflix_titles_raw (
  show_id        STRING,
  type           STRING,
  title          STRING,
  director       STRING,
  cast           STRING,
  country        STRING,
  date_added     STRING,
  date_added_dt  DATE,
  release_year   INT,
  rating         STRING,
  duration       STRING,
  duration_value INT,
  duration_unit  STRING,
  listed_in      STRING,
  description    STRING
)
STORED AS PARQUET;

-- 4. Загрузка данных из staging CSV в Parquet-таблицу
-- Примечание: CSV должен быть загружен в HDFS до выполнения.
-- Ожидаемое расположение на HDFS: /data/netflix_hw/stg_titles_csv/netflix_titles.csv
INSERT OVERWRITE TABLE netflix_titles_raw
SELECT
  show_id,
  type,
  title,
  director,
  cast,
  country,
  date_added,
  CASE
    WHEN date_added IS NULL OR trim(date_added) = '' THEN NULL
    ELSE to_date(from_unixtime(unix_timestamp(trim(date_added), 'MMMM d, yyyy')))
  END AS date_added_dt,
  CAST(release_year AS INT) AS release_year,
  rating,
  duration,
  CAST(regexp_extract(duration, '([0-9]+)', 1) AS INT) AS duration_value,
  upper(regexp_extract(duration, '([A-Za-z]+)', 1)) AS duration_unit,
  listed_in,
  description
FROM stg_netflix_titles_csv
WHERE show_id IS NOT NULL
  AND show_id != 'show_id' -- исключаем заголовок CSV
  AND title IS NOT NULL
  AND trim(title) != '';

