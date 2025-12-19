-- ============================================
-- Аналитические витрины для анализа Netflix Titles
-- ============================================

USE netflix_hw;

-- ============================================
-- ВИТРИНА 1: Количество тайтлов по типу и году релиза
-- ============================================
-- Используемые конструкции: WHERE, GROUP BY, COUNT, ORDER BY
-- Описание: Витрина показывает, сколько фильмов и сериалов (type) выходило по годам релиза.
-- Это даёт базовое понимание “производства контента” в датасете и позволяет сравнить
-- распределение Movie vs TV Show по времени.
-- 
-- Тезисы:
-- 1. Считаем количество тайтлов (COUNT(*)) по году релиза и типу
-- 2. WHERE отфильтровывает записи без года релиза и типа
-- 3. GROUP BY агрегирует данные по (release_year, type)
-- 4. ORDER BY сортирует по году и затем по количеству
-- 5. Витрина полезна для быстрой проверки полноты данных по годам
-- 6. Помогает сравнить представленность фильмов и сериалов по времени
-- 7. Может быть базой для трендов и дальнейшей детализации по странам/жанрам

CREATE TABLE IF NOT EXISTS titles_by_type_and_release_year AS
SELECT
  release_year,
  type,
  COUNT(*) AS titles_cnt
FROM netflix_titles_raw
WHERE release_year IS NOT NULL
  AND type IS NOT NULL
GROUP BY release_year, type
ORDER BY release_year ASC, titles_cnt DESC;

-- ============================================
-- ВИТРИНА 2: Топ стран по количеству тайтлов (с учётом мульти-страны)
-- ============================================
-- Используемые конструкции: WHERE, LATERAL VIEW explode, GROUP BY, HAVING, ORDER BY
-- Описание: Поле country часто содержит несколько стран через запятую.
-- Витрина “нормализует” это поле через explode и считает количество тайтлов по каждой стране.
--
-- Тезисы:
-- 1. Разбиваем country на массив стран через split(',')
-- 2. explode разворачивает массив стран в строки (по одной стране на запись)
-- 3. trim удаляет пробелы и приводит страны к единообразному виду
-- 4. GROUP BY агрегирует количество тайтлов по стране
-- 5. HAVING отсекает редкие страны для более “сигнального” топа
-- 6. ORDER BY сортирует по убыванию количества
-- 7. Витрина полезна для гео-аналитики каталога Netflix

CREATE TABLE IF NOT EXISTS top_countries_by_titles AS
SELECT
  trim(country_item) AS country,
  COUNT(*) AS titles_cnt
FROM netflix_titles_raw
LATERAL VIEW explode(split(country, ',')) c AS country_item
WHERE country IS NOT NULL
  AND trim(country) != ''
  AND trim(country_item) != ''
GROUP BY trim(country_item)
HAVING COUNT(*) >= 50
ORDER BY titles_cnt DESC, country ASC;

-- ============================================
-- ВИТРИНА 3: Топ режиссёров по числу фильмов
-- ============================================
-- Используемые конструкции: WHERE, GROUP BY, HAVING, ORDER BY
-- Описание: Витрина находит наиболее “плодовитых” режиссёров по количеству Movie в каталоге.
--
-- Тезисы:
-- 1. WHERE выбирает только фильмы и убирает пустых режиссёров
-- 2. GROUP BY агрегирует по director
-- 3. COUNT(*) считает количество фильмов у каждого режиссёра
-- 4. HAVING отсекает единичные случаи (минимальный порог)
-- 5. ORDER BY сортирует по убыванию количества
-- 6. LIMIT оставляет наиболее значимую верхушку
-- 7. Витрина помогает понять, какие режиссёры чаще всего представлены в данных

CREATE TABLE IF NOT EXISTS top_directors_movies AS
SELECT
  director,
  COUNT(*) AS movies_cnt
FROM netflix_titles_raw
WHERE type = 'Movie'
  AND director IS NOT NULL
  AND trim(director) != ''
GROUP BY director
HAVING COUNT(*) >= 5
ORDER BY movies_cnt DESC, director ASC
LIMIT 50;

-- ============================================
-- ВИТРИНА 4: Самый “новый” тайтл по каждой стране (по date_added) — оконная функция
-- ============================================
-- Используемые конструкции: WINDOW (ROW_NUMBER), PARTITION BY, ORDER BY, WHERE
-- Описание: Для каждой страны выбираем один тайтл — тот, который был добавлен в Netflix позже всех.
-- С учётом мульти-страны поле country предварительно разворачивается через explode.
--
-- Тезисы:
-- 1. explode split(country, ',') делает по записи на страну
-- 2. ROW_NUMBER() ранжирует тайтлы внутри каждой страны по date_added_dt DESC
-- 3. PARTITION BY country_clean гарантирует независимое ранжирование по стране
-- 4. WHERE rn = 1 выбирает только самый новый тайтл по стране
-- 5. Дополнительно фильтруем пустые страны и NULL даты
-- 6. Витрина удобна для проверки свежести каталога по географии
-- 7. Демонстрирует практический кейс оконных функций в Hive

CREATE TABLE IF NOT EXISTS newest_title_per_country AS
SELECT
  country,
  show_id,
  type,
  title,
  date_added_dt,
  release_year,
  rating
FROM (
  SELECT
    trim(country_item) AS country,
    show_id,
    type,
    title,
    date_added_dt,
    release_year,
    rating,
    ROW_NUMBER() OVER (
      PARTITION BY trim(country_item)
      ORDER BY date_added_dt DESC, show_id ASC
    ) AS rn
  FROM netflix_titles_raw
  LATERAL VIEW explode(split(country, ',')) c AS country_item
  WHERE country IS NOT NULL
    AND trim(country) != ''
    AND date_added_dt IS NOT NULL
    AND trim(country_item) != ''
) t
WHERE rn = 1
ORDER BY date_added_dt DESC, country ASC;

-- ============================================
-- ВИТРИНА 5: Длинные vs короткие фильмы (UNION ALL)
-- ============================================
-- Используемые конструкции: WHERE, GROUP BY, HAVING, UNION ALL, ORDER BY
-- Описание: Для фильмов (Movie) длительность хранится как строка вида “90 min”.
-- Витрина делит фильмы на категории по длительности и сравнивает статистики через UNION ALL.
--
-- Тезисы:
-- 1. Берём только Movie и только записи, где duration_unit = 'MIN'
-- 2. Выделяем “SHORT” и “LONG” по порогу минут
-- 3. GROUP BY агрегирует статистику по категории
-- 4. HAVING обеспечивает достаточный объём данных в каждой категории
-- 5. UNION ALL объединяет две категории в один датасет для сравнения
-- 6. ORDER BY упорядочивает категории и выводит итоговые метрики
-- 7. Витрина демонстрирует UNION ALL как часть аналитического отчёта

CREATE TABLE IF NOT EXISTS movie_duration_segments AS
SELECT
  'SHORT' AS segment,
  COUNT(*) AS titles_cnt,
  ROUND(AVG(duration_value), 1) AS avg_minutes,
  MIN(duration_value) AS min_minutes,
  MAX(duration_value) AS max_minutes
FROM netflix_titles_raw
WHERE type = 'Movie'
  AND duration_unit = 'MIN'
  AND duration_value IS NOT NULL
  AND duration_value > 0
  AND duration_value < 300
  AND duration_value < 90
HAVING COUNT(*) >= 100

UNION ALL

SELECT
  'LONG' AS segment,
  COUNT(*) AS titles_cnt,
  ROUND(AVG(duration_value), 1) AS avg_minutes,
  MIN(duration_value) AS min_minutes,
  MAX(duration_value) AS max_minutes
FROM netflix_titles_raw
WHERE type = 'Movie'
  AND duration_unit = 'MIN'
  AND duration_value IS NOT NULL
  AND duration_value > 0
  AND duration_value < 300
  AND duration_value >= 90
HAVING COUNT(*) >= 100

ORDER BY segment;

-- ============================================
-- ВИТРИНА 6: Годовая динамика добавления по типам + доля типа в году + рост YoY (JOIN + LAG)
-- ============================================
-- Используемые конструкции: GROUP BY, JOIN, WINDOW (LAG), PARTITION BY, ORDER BY, WHERE
-- Описание: Считаем добавления по годам и типам (Movie/TV Show), рассчитываем:
-- - долю типа в общем количестве добавлений за год (JOIN на годовые итоги)
-- - рост относительно предыдущего года внутри каждого типа (LAG PARTITION BY type)
--
-- Тезисы:
-- 1. Агрегируем данные по (year_added, type) через GROUP BY
-- 2. LAG() PARTITION BY type получает прошлогоднее значение для каждого типа отдельно
-- 3. Отдельно считаем total_cnt по году, суммируя по типам
-- 4. JOIN по year_added добавляет в строку годовой итог для вычисления доли
-- 5. growth_pct считается как процент к прошлому году (внутри типа)
-- 6. WHERE исключает NULL даты и деление на ноль
-- 7. Витрина одновременно демонстрирует JOIN и оконные функции

CREATE TABLE IF NOT EXISTS yearly_additions_by_type_with_share_and_growth AS
SELECT
  c.year_added,
  c.type,
  c.titles_added_cnt,
  t.total_titles_added_cnt,
  ROUND((c.titles_added_cnt * 100.0) / t.total_titles_added_cnt, 2) AS share_in_year_pct,
  c.prev_year_cnt,
  ROUND(((c.titles_added_cnt - c.prev_year_cnt) * 100.0) / c.prev_year_cnt, 2) AS growth_pct
FROM (
  SELECT
    year_added,
    type,
    titles_added_cnt,
    LAG(titles_added_cnt, 1) OVER (PARTITION BY type ORDER BY year_added) AS prev_year_cnt
  FROM (
    SELECT
      year(date_added_dt) AS year_added,
      type,
      COUNT(*) AS titles_added_cnt
    FROM netflix_titles_raw
    WHERE date_added_dt IS NOT NULL
      AND type IS NOT NULL
    GROUP BY year(date_added_dt), type
  ) base
) c
JOIN (
  SELECT
    year_added,
    SUM(titles_added_cnt) AS total_titles_added_cnt
  FROM (
    SELECT
      year(date_added_dt) AS year_added,
      type,
      COUNT(*) AS titles_added_cnt
    FROM netflix_titles_raw
    WHERE date_added_dt IS NOT NULL
      AND type IS NOT NULL
    GROUP BY year(date_added_dt), type
  ) s
  GROUP BY year_added
) t
  ON c.year_added = t.year_added
WHERE c.prev_year_cnt IS NOT NULL
  AND c.prev_year_cnt > 0
  AND t.total_titles_added_cnt > 0
ORDER BY c.year_added ASC, c.type ASC;

