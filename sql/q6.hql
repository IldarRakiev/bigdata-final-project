-- ============================================================
-- q6 — Push/Watch ratio per language (top 15 by activity)
-- ============================================================
-- Push  = developer activity (real work).
-- Watch = audience interest (popularity).
-- High push/watch  → working repos used by devs.
-- Low push/watch   → trendy/showcase repos with passive audience.
-- ============================================================

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q6_results;

CREATE EXTERNAL TABLE q6_results (
    language        STRING,
    push_events     BIGINT,
    watch_events    BIGINT,
    push_watch_ratio DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q6_results';

INSERT OVERWRITE TABLE q6_results
SELECT
    r.language,
    SUM(CASE WHEN e.event_type = 'PushEvent'  THEN e.event_count ELSE 0 END) AS push_events,
    SUM(CASE WHEN e.event_type = 'WatchEvent' THEN e.event_count ELSE 0 END) AS watch_events,
    ROUND(
        SUM(CASE WHEN e.event_type = 'PushEvent'  THEN e.event_count ELSE 0 END) /
        NULLIF(SUM(CASE WHEN e.event_type = 'WatchEvent' THEN e.event_count ELSE 0 END), 0),
        4
    ) AS push_watch_ratio
FROM events_part e
JOIN repositories_buck r ON e.repo_id = r.repo_id
WHERE r.language IS NOT NULL
GROUP BY r.language
HAVING SUM(CASE WHEN e.event_type = 'WatchEvent' THEN e.event_count ELSE 0 END) > 0
ORDER BY (push_events + watch_events) DESC
LIMIT 15;

SELECT * FROM q6_results;
