-- q2 — Monthly WatchEvent (star) dynamics

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q2_results;

CREATE EXTERNAL TABLE q2_results (
    event_year                 INT,
    event_month                INT,
    monthly_stars              BIGINT,
    repos_starred              BIGINT,
    avg_stars_per_starred_repo DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q2_results';

INSERT OVERWRITE TABLE q2_results
SELECT
    event_year,
    event_month,
    SUM(CASE WHEN event_type = 'WatchEvent'
             THEN event_count ELSE 0 END)                            AS monthly_stars,
    COUNT(DISTINCT CASE WHEN event_type = 'WatchEvent'
                        THEN repo_id END)                            AS repos_starred,
    ROUND(SUM(CASE WHEN event_type = 'WatchEvent'
                   THEN event_count ELSE 0 END) /
          NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'WatchEvent'
                                     THEN repo_id END), 0), 2)       AS avg_stars_per_starred_repo
FROM events_part
GROUP BY event_year, event_month
ORDER BY event_year, event_month;

SELECT * FROM q2_results;
