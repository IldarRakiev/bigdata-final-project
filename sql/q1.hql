-- ============================================================
-- q1 — Top 10 programming languages by total event activity
-- ============================================================
-- Joins events_part with repositories_buck (bucket-aligned on
-- repo_id), groups by language, sums event_count.
-- ============================================================

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results (
    language     STRING,
    repo_count   BIGINT,
    total_events BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q1_results';

INSERT OVERWRITE TABLE q1_results
SELECT
    r.language,
    COUNT(DISTINCT r.repo_id) AS repo_count,
    SUM(e.event_count)        AS total_events
FROM events_part e
JOIN repositories_buck r ON e.repo_id = r.repo_id
WHERE r.language IS NOT NULL
GROUP BY r.language
ORDER BY total_events DESC
LIMIT 10;

SELECT * FROM q1_results;
