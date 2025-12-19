ДЗ №2: Аналитическая витрина в Hive
Кластер (Hadoop + Hive) разворачивается в Docker, данные загружаются в Hive, после чего создаются 6 аналитических витрин, демонстрирующих агрегации, JOIN/UNION и оконные функции.​

Что сделано
Набор данных
Использован датасет all_stocks_5yr.csv (история цен акций за 5 лет) в формате CSV.
Файл в проекте: data/all_stocks_5yr.csv (поля: date, open, high, low, close, volume, Name).

Hive: база и таблицы
Создана БД stocks_hw.
Таблицы:

stg_prices_csv — внешняя таблица для чтения CSV из HDFS.

stocks_raw — основная таблица с данными в Parquet.

SQL для создания БД/таблиц и загрузки: data/load_data.sql.

Витрины (6 шт.)
Витрины построены так, чтобы покрыть обязательные конструкции: WHERE, COUNT, GROUP BY, HAVING, ORDER BY, JOIN, UNION ALL, WINDOW (включая ROW_NUMBER, LAG, PARTITION BY).​

avg_close_per_company — средняя цена закрытия по компаниям
Используется: WHERE, GROUP BY, AVG, COUNT, ORDER BY

max_volume_day — день максимального объёма торгов по каждой компании
Используется: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...), WHERE

quarterly_growth — квартальная динамика цены (сравнение с предыдущим кварталом)
Используется: LAG() OVER (PARTITION BY ... ORDER BY ...), GROUP BY, WHERE

top5_by_volume — топ-5 компаний по среднему объёму торгов
Используется: GROUP BY, HAVING, AVG, COUNT, ORDER BY

volatility_comparison — сравнение высокой/низкой волатильности
Используется: STDDEV, GROUP BY, HAVING, WHERE, UNION ALL

monthly_summary_with_avg — месячная статистика + объединение с общими средними
Используется: GROUP BY, JOIN, WHERE, COUNT, ORDER BY

Описание каждой витрины (7 тезисов) добавлено комментариями перед SQL-запросом в data/data_marts.sql.

Структура репозитория
text
HW2/
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
├── setup_hive.sh
├── conf/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── yarn-site.xml
│   ├── mapred-site.xml
│   ├── hive-site.xml
│   ├── hadoop-env.sh
│   └── hive-env.sh
├── data/
│   ├── all_stocks_5yr.csv
│   ├── load_data.sql
│   └── data_marts.sql
└── README.md
Запуск в Docker
1) Поднять контейнер
bash
docker-compose up -d
2) Дождаться старта сервисов
bash
docker exec -it hadoop-hw2 bash
jps
# Ожидаются: NameNode, DataNode, ResourceManager, NodeManager
3) Автоматическая загрузка и построение витрин
bash
bash /data/setup_hive.sh
Скрипт выполняет:

создание директорий в HDFS

загрузку CSV в HDFS

выполнение load_data.sql и data_marts.sql

4) Проверка в Hive (Beeline)
Подключение к HiveServer2 через Beeline выполняется по JDBC URL вида jdbc:hive2://localhost:10000.​

bash
beeline -u "jdbc:hive2://localhost:10000" -n root -p ""
Далее:

sql
USE stocks_hw;
SHOW TABLES;

SELECT * FROM avg_close_per_company LIMIT 10;
SELECT * FROM max_volume_day LIMIT 10;
SELECT * FROM quarterly_growth LIMIT 10;
SELECT * FROM top5_by_volume;
SELECT * FROM volatility_comparison LIMIT 20;
SELECT * FROM monthly_summary_with_avg LIMIT 10;
Ручной прогон (если скрипт не используется)
1) Положить CSV в HDFS
Команды hdfs dfs -mkdir -p и hdfs dfs -put — стандартные операции файловой оболочки Hadoop для создания каталогов и загрузки файлов.​

bash
hdfs dfs -mkdir -p /data/stocks_hw/stg_prices_csv
hdfs dfs -put /data/all_stocks_5yr.csv /data/stocks_hw/stg_prices_csv/
2) Выполнить SQL
bash
beeline -u "jdbc:hive2://localhost:10000" -n root -p "" -f /data/load_data.sql
beeline -u "jdbc:hive2://localhost:10000" -n root -p "" -f /data/data_marts.sql
Диагностика и полезные команды
bash
# HDFS
hdfs dfs -ls /
hdfs dfs -ls /data/stocks_hw

# Логи Hive
tail -n 50 /opt/hive/log.metastore
tail -n 50 /opt/hive/log.hiveserver2
Web UI с хоста:

HDFS: http://localhost:9870

YARN: http://localhost:28088

Результат
Данные загружены в Hive (через staging CSV и основную таблицу), создано 6 витрин, покрывающих требуемые SQL-конструкции.​
Полные запросы и комментарии с пояснениями находятся в data/data_marts.sql.

Автор
Кожухов Игорь