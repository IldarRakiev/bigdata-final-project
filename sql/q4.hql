-- ============================================================
-- q4 — Success rate by pre-T activity level
-- ============================================================
-- For each bucket of pre-T total event volume, how many repos are
-- there and what fraction of them ends up labelled as "success"
-- in the post-T window? Answers the practical question: how active
-- does a repo have to be in 2023 before it has a real chance of
-- breaking out in H1 2024?
--
-- Replaces the old threshold-sensitivity matrix — that one ran into
-- type-coercion issues in Hive 3 (CROSS JOIN with inline UNION ALL
-- of literals collapsed stars_min into 0/1 no matter what we tried).
-- This version is a single straightforward GROUP BY, no joins, no
-- inline tables, nothing for Hive to coerce.
-- ============================================================

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q4_results;

CREATE EXTERNAL TABLE q4_results (
    activity_bucket   STRING,
    repo_count        BIGINT,
    success_count     BIGINT,
    success_rate_pct  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q4_results';

INSERT OVERWRITE TABLE q4_results
SELECT
    activity_bucket,
    COUNT(*)                                                     AS repo_count,
    SUM(is_success)                                              AS success_count,
    ROUND(100.0 * SUM(is_success) / COUNT(*), 5)                 AS success_rate_pct
FROM (
    SELECT
        repo_id,
        CASE
            WHEN pre_total >= 1000 THEN 'Q5_>=1000_events'
            WHEN pre_total >=  100 THEN 'Q4_100-999_events'
            WHEN pre_total >=   30 THEN 'Q3_30-99_events'
            WHEN pre_total >=   10 THEN 'Q2_10-29_events'
            ELSE                       'Q1_<10_events'
        END AS activity_bucket,
        CASE WHEN post_watches >= 10
              AND post_watches >= 2.0 * pre_watches
              AND pre_watches  > 0
             THEN 1 ELSE 0 END AS is_success
    FROM (
        SELECT
            repo_id,
            SUM(CASE WHEN event_year = 2023
                     THEN event_count ELSE 0 END) AS pre_total,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'WatchEvent'
                     THEN event_count ELSE 0 END) AS pre_watches,
            SUM(CASE WHEN event_year = 2024 AND event_type = 'WatchEvent'
                     THEN event_count ELSE 0 END) AS post_watches
        FROM events_part
        GROUP BY repo_id
    ) per
) classified
GROUP BY activity_bucket
ORDER BY activity_bucket;

SELECT * FROM q4_results;
