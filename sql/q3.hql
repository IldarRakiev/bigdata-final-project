-- q3 — Pre-T vs Post-T star-growth distribution

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q3_results;

CREATE EXTERNAL TABLE q3_results (
    growth_bucket  STRING,
    repo_count     BIGINT,
    pct_of_total   DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q3_results';

INSERT OVERWRITE TABLE q3_results
SELECT
    growth_bucket,
    COUNT(*)                                                         AS repo_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4)               AS pct_of_total
FROM (
    SELECT
        repo_id,
        CASE
            WHEN pre_watches  = 0 AND post_watches = 0 THEN '00_no_stars'
            WHEN pre_watches  = 0 AND post_watches > 0 THEN '01_new_in_post'
            WHEN post_watches = 0                      THEN '02_decay_to_zero'
            WHEN post_watches < pre_watches            THEN '03_decline'
            WHEN post_watches < 2 * pre_watches        THEN '04_growth_lt_2x'
            WHEN post_watches < 3 * pre_watches        THEN '05_growth_2x_3x'
            WHEN post_watches < 5 * pre_watches        THEN '06_growth_3x_5x'
            ELSE                                            '07_growth_gt_5x'
        END AS growth_bucket
    FROM (
        SELECT
            repo_id,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'WatchEvent'
                     THEN event_count ELSE 0 END) AS pre_watches,
            SUM(CASE WHEN event_year = 2024 AND event_type = 'WatchEvent'
                     THEN event_count ELSE 0 END) AS post_watches
        FROM events_part
        GROUP BY repo_id
    ) per_repo
) bucketed
GROUP BY growth_bucket
ORDER BY growth_bucket;

SELECT * FROM q3_results;
