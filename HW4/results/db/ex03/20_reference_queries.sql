SELECT
    t.region,
    countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
    countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
FROM dz4.person_data AS t
WHERE t.date_birth BETWEEN toDate('2000-01-01') AND toDate('2000-01-31')
  AND t.region IN ('20', '25', '43', '59')
GROUP BY t.region
ORDER BY t.region;

SELECT
    countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
    countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
FROM dz4.person_data AS t
WHERE t.is_marital = 1
  AND t.region IN ('80')
GROUP BY t.region
ORDER BY t.region;


