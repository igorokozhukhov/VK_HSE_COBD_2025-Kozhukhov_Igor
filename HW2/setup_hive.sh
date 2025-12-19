#!/bin/bash
# Скрипт для настройки Hive и загрузки данных

set -e

echo "=== Настройка Hive и загрузка данных ==="

# Ожидание запуска HDFS
echo "Ожидание запуска HDFS..."
sleep 10

# Создание директорий в HDFS
echo "Создание директорий в HDFS..."
hdfs dfs -mkdir -p /data/stocks_hw/stg_prices_csv || true
hdfs dfs -mkdir -p /user/hive/warehouse || true
hdfs dfs -chmod -R 777 /data/stocks_hw || true
hdfs dfs -chmod -R 777 /user/hive/warehouse || true

# Загрузка CSV файла в HDFS
echo "Загрузка CSV файла в HDFS..."
if [ -f /data/all_stocks_5yr.csv ]; then
    # Удаляем старые данные если есть
    hdfs dfs -rm -r /data/stocks_hw/stg_prices_csv/* 2>/dev/null || true
    # Загружаем файл
    hdfs dfs -put -f /data/all_stocks_5yr.csv /data/stocks_hw/stg_prices_csv/all_stocks_5yr.csv
    echo "Файл успешно загружен в HDFS"
    hdfs dfs -ls /data/stocks_hw/stg_prices_csv/
else
    echo "ВНИМАНИЕ: Файл /data/all_stocks_5yr.csv не найден!"
    echo "Убедитесь, что файл находится в директории data/"
    exit 1
fi

# Ожидание запуска HiveServer2
echo "Ожидание запуска HiveServer2..."
for i in {1..30}; do
    sleep 3
    if beeline -u "jdbc:hive2://localhost:10000" -n root -p "" -e "SHOW DATABASES;" 2>&1 | grep -q "default\|stocks_hw"; then
        echo "HiveServer2 готов!"
        break
    fi
    echo "Попытка подключения $i/30..."
    if [ $i -eq 30 ]; then
        echo "ОШИБКА: Не удалось подключиться к HiveServer2"
        echo "Проверьте логи: tail -n 50 /opt/hive/log.hiveserver2"
        exit 1
    fi
done

# Выполнение SQL скриптов через Beeline
echo "Выполнение SQL скриптов..."

if [ -f /data/load_data.sql ]; then
    echo "Выполнение load_data.sql..."
    beeline -u "jdbc:hive2://localhost:10000" -n root -p "" -f /data/load_data.sql
    if [ $? -ne 0 ]; then
        echo "ОШИБКА при выполнении load_data.sql"
        exit 1
    fi
fi

if [ -f /data/data_marts.sql ]; then
    echo "Выполнение data_marts.sql..."
    beeline -u "jdbc:hive2://localhost:10000" -n root -p "" -f /data/data_marts.sql
    if [ $? -ne 0 ]; then
        echo "ОШИБКА при выполнении data_marts.sql"
        exit 1
    fi
fi

# Проверка созданных витрин
echo "Проверка созданных витрин..."
beeline -u "jdbc:hive2://localhost:10000" -n root -p "" -e "USE stocks_hw; SHOW TABLES;"

echo "=== Настройка завершена ==="
echo "Для подключения к Hive используйте:"
echo "beeline -u \"jdbc:hive2://localhost:10000\" -n root -p \"\""

