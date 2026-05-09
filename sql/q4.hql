-- q4 — Sensitivity of the success label to threshold choice

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q4_results;

CREATE EXTERNAL TABLE q4_results (
    stars_min      INT,
    growth_min     DOUBLE,
    total_repos    BIGINT,
    success_count  BIGINT,
    success_pct    DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q4_results';

INSERT OVERWRITE TABLE q4_results
SELECT
    th.stars_min,
    th.growth_min,
    COUNT(*)                                                                         AS total_repos,
    SUM(CASE WHEN per.post_watches >= th.stars_min
              AND per.post_watches >= th.growth_min * per.pre_watches
              AND per.pre_watches > 0
             THEN 1 ELSE 0 END)                                                      AS success_count,
    ROUND(100.0 * SUM(CASE WHEN per.post_watches >= th.stars_min
                            AND per.post_watches >= th.growth_min * per.pre_watches
                            AND per.pre_watches > 0
                           THEN 1 ELSE 0 END) / COUNT(*), 5)                         AS success_pct
FROM (
    SELECT
        repo_id,
        SUM(CASE WHEN event_year = 2023 AND event_type = 'WatchEvent'
                 THEN event_count ELSE 0 END) AS pre_watches,
        SUM(CASE WHEN event_year = 2024 AND event_type = 'WatchEvent'
                 THEN event_count ELSE 0 END) AS post_watches
    FROM events_part
    GROUP BY repo_id
) per
-- Explicit CAST on the first row anchors the column types of the
-- inline UNION ALL table; without them Hive 3 has been observed to
-- coerce stars_min into BOOLEAN/TINYINT, which collapses 100/250/500/1000
-- into 0/1 in the join. Same idea for growth_min (DOUBLE).
CROSS JOIN (
                SELECT CAST(100  AS INT) AS stars_min, CAST(2.0 AS DOUBLE) AS growth_min
    UNION ALL   SELECT CAST(100  AS INT),              CAST(3.0 AS DOUBLE)
    UNION ALL   SELECT CAST(250  AS INT),              CAST(2.0 AS DOUBLE)
    UNION ALL   SELECT CAST(250  AS INT),              CAST(3.0 AS DOUBLE)
    UNION ALL   SELECT CAST(500  AS INT),              CAST(2.0 AS DOUBLE)
    UNION ALL   SELECT CAST(500  AS INT),              CAST(3.0 AS DOUBLE)
    UNION ALL   SELECT CAST(1000 AS INT),              CAST(3.0 AS DOUBLE)
) th
GROUP BY th.stars_min, th.growth_min
ORDER BY th.stars_min, th.growth_min;

SELECT * FROM q4_results;
