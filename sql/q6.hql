-- q6 — Acceleration anomalies in late 2023 (anomaly EDA)

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q6_results;

CREATE EXTERNAL TABLE q6_results (
    repo_id        BIGINT,
    repo_name      STRING,
    language       STRING,
    q3_2023_stars  BIGINT,
    q4_2023_stars  BIGINT,
    abs_jump       BIGINT,
    acceleration   DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q6_results';

INSERT OVERWRITE TABLE q6_results
SELECT
    e.repo_id,
    r.repo_name,
    r.language,
    e.q3_stars                                           AS q3_2023_stars,
    e.q4_stars                                           AS q4_2023_stars,
    e.q4_stars - e.q3_stars                              AS abs_jump,
    ROUND(e.q4_stars * 1.0 / NULLIF(e.q3_stars, 0), 3)   AS acceleration
FROM (
    SELECT
        repo_id,
        SUM(CASE WHEN event_year = 2023 AND event_month BETWEEN 7 AND 9
                  AND event_type = 'WatchEvent'
                 THEN event_count ELSE 0 END) AS q3_stars,
        SUM(CASE WHEN event_year = 2023 AND event_month BETWEEN 10 AND 12
                  AND event_type = 'WatchEvent'
                 THEN event_count ELSE 0 END) AS q4_stars
    FROM events_part
    GROUP BY repo_id
) e
JOIN repositories_buck r ON e.repo_id = r.repo_id
WHERE e.q3_stars >= 5            -- non-trivial baseline
  AND e.q4_stars >= e.q3_stars   -- only acceleration, not decay
ORDER BY acceleration DESC, abs_jump DESC
LIMIT 20;

SELECT * FROM q6_results;
