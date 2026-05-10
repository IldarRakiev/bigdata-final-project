-- q5 — Behavioral profile: success vs non-success cohorts

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q5_results;

CREATE EXTERNAL TABLE q5_results (
    is_success         INT,
    repo_count         BIGINT,
    avg_pre_events     DOUBLE,
    avg_pre_stars      DOUBLE,
    avg_push_share     DOUBLE,
    avg_watch_share    DOUBLE,
    avg_pr_share       DOUBLE,
    avg_issues_share   DOUBLE,
    avg_fork_share     DOUBLE,
    avg_create_share   DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q5_results';

INSERT OVERWRITE TABLE q5_results
SELECT
    is_success,
    COUNT(*)                       AS repo_count,
    ROUND(AVG(pre_total),    2)    AS avg_pre_events,
    ROUND(AVG(pre_watches),  2)    AS avg_pre_stars,
    ROUND(AVG(push_share),   4)    AS avg_push_share,
    ROUND(AVG(watch_share),  4)    AS avg_watch_share,
    ROUND(AVG(pr_share),     4)    AS avg_pr_share,
    ROUND(AVG(issues_share), 4)    AS avg_issues_share,
    ROUND(AVG(fork_share),   4)    AS avg_fork_share,
    ROUND(AVG(create_share), 4)    AS avg_create_share
FROM (
    SELECT
        repo_id,
        pre_total,
        pre_watches,
        pre_pushes  / pre_total AS push_share,
        pre_watches / pre_total AS watch_share,
        pre_prs     / pre_total AS pr_share,
        pre_issues  / pre_total AS issues_share,
        pre_forks   / pre_total AS fork_share,
        pre_creates / pre_total AS create_share,
        CASE WHEN post_watches >= 500
              AND post_watches >= 3.0 * pre_watches
              AND pre_watches > 0
             THEN 1 ELSE 0 END   AS is_success
    FROM (
        SELECT
            repo_id,
            SUM(CASE WHEN event_year = 2023 THEN event_count ELSE 0 END)                                                  AS pre_total,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'PushEvent'        THEN event_count ELSE 0 END)              AS pre_pushes,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'WatchEvent'       THEN event_count ELSE 0 END)              AS pre_watches,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'PullRequestEvent' THEN event_count ELSE 0 END)              AS pre_prs,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'IssuesEvent'      THEN event_count ELSE 0 END)              AS pre_issues,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'ForkEvent'        THEN event_count ELSE 0 END)              AS pre_forks,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'CreateEvent'      THEN event_count ELSE 0 END)              AS pre_creates,
            SUM(CASE WHEN event_year = 2024 AND event_type = 'WatchEvent'       THEN event_count ELSE 0 END)              AS post_watches
        FROM events_part
        GROUP BY repo_id
    ) agg
    WHERE pre_total > 0
) profiled
GROUP BY is_success
ORDER BY is_success;

SELECT * FROM q5_results;
