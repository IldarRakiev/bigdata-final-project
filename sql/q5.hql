-- ============================================================
-- q5 — Engagement: new repos (born in window) vs older repos
-- ============================================================
-- "New" = first_seen_at falls within the data window
--         (2023-01-01 .. 2024-06-30).
-- "Old" = first_seen_at predates the window (or NULL).
-- Compares activity per repo across the two cohorts.
-- ============================================================

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q5_results;

CREATE EXTERNAL TABLE q5_results (
    cohort           STRING,
    repo_count       BIGINT,
    total_events     BIGINT,
    avg_events_repo  DOUBLE,
    avg_actors_repo  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q5_results';

INSERT OVERWRITE TABLE q5_results
SELECT
    cohort,
    COUNT(*)                                AS repo_count,
    SUM(total_events)                       AS total_events,
    ROUND(AVG(total_events), 2)             AS avg_events_repo,
    ROUND(AVG(total_actors), 2)             AS avg_actors_repo
FROM (
    SELECT
        r.repo_id,
        CASE
            WHEN r.first_seen_at IS NULL THEN 'old_or_unknown'
            WHEN CAST(r.first_seen_at AS DATE) >= DATE '2023-01-01'
              THEN 'new_in_window'
            ELSE 'old_or_unknown'
        END AS cohort,
        SUM(e.event_count)   AS total_events,
        SUM(e.unique_actors) AS total_actors
    FROM events_part e
    JOIN repositories_buck r ON e.repo_id = r.repo_id
    GROUP BY r.repo_id, r.first_seen_at
) t
GROUP BY cohort;

SELECT * FROM q5_results;
