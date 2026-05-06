-- ============================================================
-- q3 — Distribution of event types
-- ============================================================
-- Share of each event type in the total activity:
--   WatchEvent, ForkEvent, PullRequestEvent,
--   PushEvent, IssuesEvent, CreateEvent.
-- ============================================================

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q3_results;

CREATE EXTERNAL TABLE q3_results (
    event_type    STRING,
    total_events  BIGINT,
    pct_of_total  DOUBLE,
    active_repos  BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q3_results';

INSERT OVERWRITE TABLE q3_results
SELECT
    event_type,
    SUM(event_count)                                     AS total_events,
    ROUND(100.0 * SUM(event_count) /
          SUM(SUM(event_count)) OVER (), 2)              AS pct_of_total,
    COUNT(DISTINCT repo_id)                              AS active_repos
FROM events_part
GROUP BY event_type
ORDER BY total_events DESC;

SELECT * FROM q3_results;
