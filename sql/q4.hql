-- ============================================================
-- q4 — Top-20 repositories by unique actors (most "viral")
-- ============================================================
-- Sums unique_actors across all event types per repo, joins to
-- repositories_buck to recover repo_name + language.
-- ============================================================

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q4_results;

CREATE EXTERNAL TABLE q4_results (
    repo_id       BIGINT,
    repo_name     STRING,
    language      STRING,
    total_events  BIGINT,
    total_actors  BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q4_results';

INSERT OVERWRITE TABLE q4_results
SELECT
    r.repo_id,
    r.repo_name,
    r.language,
    SUM(e.event_count)    AS total_events,
    SUM(e.unique_actors)  AS total_actors
FROM events_part e
JOIN repositories_buck r ON e.repo_id = r.repo_id
GROUP BY r.repo_id, r.repo_name, r.language
ORDER BY total_actors DESC
LIMIT 20;

SELECT * FROM q4_results;
