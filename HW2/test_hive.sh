#!/bin/bash
# Упрощенный скрипт проверки и выполнения SQL

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HIVE_HOME=/opt/hive
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HIVE_HOME/bin:$HADOOP_HOME/bin

echo "=== Проверка HDFS ==="
hdfs dfs -ls /data/stocks_hw/stg_prices_csv/ 2>&1 | head -5

echo ""
echo "=== Выполнение load_data.sql ==="
hive -f /data/load_data.sql 2>&1 | grep -E "OK|FAILED|Error|Table|Database" | tail -10

echo ""
echo "=== Проверка созданных таблиц ==="
hive -e "USE stocks_hw; SHOW TABLES;" 2>&1 | grep -v "SLF4J" | tail -10

echo ""
echo "=== Выполнение data_marts.sql ==="
hive -f /data/data_marts.sql 2>&1 | grep -E "OK|FAILED|Error|Table" | tail -15

echo ""
echo "=== Проверка созданных витрин ==="
hive -e "USE stocks_hw; SHOW TABLES;" 2>&1 | grep -v "SLF4J" | tail -15

echo ""
echo "=== Проверка данных в витринах ==="
hive -e "USE stocks_hw; SELECT COUNT(*) FROM avg_close_per_company;" 2>&1 | grep -v "SLF4J" | tail -5

