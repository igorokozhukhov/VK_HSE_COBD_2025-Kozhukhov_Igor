Отчёт по ДЗ №1: Hadoop (HDFS, YARN, MapReduce)
0. Контекст работы
Студент: Кожухов Игорь Олегович
Дисциплина: Анализ больших данных
Задача: поднять локальный Hadoop-кластер в Docker, проверить базовые операции HDFS и выполнить WordCount как MapReduce-job через YARN.​

1. Окружение и запуск
1.1. Состав контейнера
Контейнер собирается на базе ubuntu:22.04 и включает Hadoop 3.3.6, OpenJDK 11 и SSH (используется внутренними сервисными сценариями).​
Конфигурации для HDFS и YARN заранее подготовлены, поэтому после старта поднимаются компоненты хранения и планировщик задач.​

1.2. Сборка и поднятие сервисов
bash
docker-compose up -d --build
После запуска должны быть активны демоны HDFS и YARN (NameNode/DataNode/SecondaryNameNode и ResourceManager/NodeManager).​
Проверка процессов внутри контейнера:

bash
docker exec -it hadoop-master bash -lc "jps -l"
2. Выполнение заданий через run_hw.sh
Вся последовательность действий собрана в скрипте run_hw.sh, который запускается внутри контейнера одной командой.
Запуск:

bash
docker exec -it hadoop-master bash -lc "bash /opt/hadoop/run_hw.sh"
Что проверяется скриптом
№	Действие	Ожидаемый результат
1	Создать каталог /createme в HDFS	Каталог создан или уже существует (идемпотентность). ​
2	Удалить каталог /delme в HDFS	Каталог удалён, если присутствовал (обычно через hdfs dfs -rm -r). ​
3	Создать непустой файл /nonnull.txt	Файл существует и не нулевого размера (проверка записи в HDFS). ​
4	Запустить MapReduce WordCount через YARN	Job успешно выполнен под управлением YARN. ​
5	Посчитать вхождения Innsmouth и записать в /whataboutinnsmouth.txt	В файл записано итоговое число (в отчёте: 3). ​
3. Контроль результатов
3.1. Просмотр результата WordCount
bash
docker exec -it hadoop-master bash -lc 'hdfs dfs -cat /tmp/wordcount_shadow/part-r-00000 | sed -n "1,200p"'
Команда hdfs dfs -cat используется для чтения содержимого файла из HDFS.​

3.2. Проверка финального файла
bash
docker exec -it hadoop-master bash -lc "hdfs dfs -cat /whataboutinnsmouth.txt"
4. Поток данных
text
flowchart TD
  A[shadow.txt на хосте] --> B[Volume /data]
  B --> C[Контейнер hadoop-master]
  C -->|hdfs dfs -put| D[/shadow.txt в HDFS/]
  D -->|MapReduce WordCount| E[YARN ResourceManager]
  E --> F[/tmp/wordcount_shadow/part-r-00000/]
  F -->|фильтрация Innsmouth| G[/whataboutinnsmouth.txt/]
  G --> H[Вывод значения пользователю]
Mermaid-диаграммы в README оформляются fenced-блоком с идентификатором языка mermaid.​

5. Логика run_hw.sh (кратко)
Поднимает/проверяет служебные компоненты (SSH и сервисы Hadoop) перед выполнением операций.​

Выполняет команды HDFS для подготовки директорий и файлов (создание/удаление/чтение).​

Загружает входной текст (shadow.txt) в HDFS и запускает WordCount как MapReduce-задачу под управлением YARN, формируя выход в указанной директории.​

6. Пример консольного лога
text
[OK] /createme создана (или уже существовала).
[OK] /delme удалена (если была).
[OK] /nonnull.txt создан.
[INFO] Загрузка /data/shadow.txt в HDFS как /shadow.txt
... INFO mapreduce.Job: The url to track the job: http://master:8088/proxy/application_.../
[OK] WordCount завершён. Результат: /tmp/wordcount_shadow
3
7. Файлы проекта
text
dz1/
│
├── Dockerfile
├── docker-compose.yml
├── conf/
├── run_hw.sh
├── Readme.md
├── README.md
├── entrypoint.sh
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── yarn-site.xml
│   ├── mapred-site.xml
│   └── hadoop-env.sh
├── data/
│   └── shadow.txt
8. Результат выполнения
Hadoop-сервисы успешно подняты в Docker и доступны для выполнения задач через YARN.​
MapReduce WordCount отработал корректно, 
