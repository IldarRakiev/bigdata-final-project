-- ============================================================
-- q2 — Monthly events trend (Jan 2023 – Jun 2024)
-- ============================================================
-- Aggregates total events + unique active repos per month.
-- Uses partition columns directly for fast scan.
-- ============================================================

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q2_results;

CREATE EXTERNAL TABLE q2_results (
    event_year    INT,
    event_month   INT,
    total_events  BIGINT,
    active_repos  BIGINT,
    active_actors BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q2_results';

INSERT OVERWRITE TABLE q2_results
SELECT
    event_year,
    event_month,
    SUM(event_count)         AS total_events,
    COUNT(DISTINCT repo_id)  AS active_repos,
    SUM(unique_actors)       AS active_actors
FROM events_part
GROUP BY event_year, event_month
ORDER BY event_year, event_month;

SELECT * FROM q2_results;
