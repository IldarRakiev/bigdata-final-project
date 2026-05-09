cd ~/bigdata-final-project

cat > sql/q4.hql <<'SQL'
USE team28_projectdb;
SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results (
                                     stars_min INT, growth_min DOUBLE, total_repos BIGINT,
                                     success_count BIGINT, success_pct DOUBLE
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'project/hive/warehouse/q4_results';

INSERT OVERWRITE TABLE q4_results
SELECT 100, 2.0, COUNT(*),
       SUM(CASE WHEN post_w >= 100 AND pre_w > 0 AND post_w >= 2.0*pre_w THEN 1 ELSE 0 END),
       ROUND(100.0*SUM(CASE WHEN post_w >= 100 AND pre_w > 0 AND post_w >= 2.0*pre_w THEN 1 ELSE 0 END)/COUNT(*),5)
FROM (SELECT repo_id,
             SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
             SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
      FROM events_part GROUP BY repo_id) p
UNION ALL
SELECT 100, 3.0, COUNT(*),
       SUM(CASE WHEN post_w >= 100 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END),
       ROUND(100.0*SUM(CASE WHEN post_w >= 100 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END)/COUNT(*),5)
FROM (SELECT repo_id,
             SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
             SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
      FROM events_part GROUP BY repo_id) p
UNION ALL
SELECT 250, 2.0, COUNT(*),
       SUM(CASE WHEN post_w >= 250 AND pre_w > 0 AND post_w >= 2.0*pre_w THEN 1 ELSE 0 END),
       ROUND(100.0*SUM(CASE WHEN post_w >= 250 AND pre_w > 0 AND post_w >= 2.0*pre_w THEN 1 ELSE 0 END)/COUNT(*),5)
FROM (SELECT repo_id,
             SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
             SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
      FROM events_part GROUP BY repo_id) p
UNION ALL
SELECT 250, 3.0, COUNT(*),
       SUM(CASE WHEN post_w >= 250 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END),
       ROUND(100.0*SUM(CASE WHEN post_w >= 250 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END)/COUNT(*),5)
FROM (SELECT repo_id,
             SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
             SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
      FROM events_part GROUP BY repo_id) p
UNION ALL
SELECT 500, 2.0, COUNT(*),
       SUM(CASE WHEN post_w >= 500 AND pre_w > 0 AND post_w >= 2.0*pre_w THEN 1 ELSE 0 END),
       ROUND(100.0*SUM(CASE WHEN post_w >= 500 AND pre_w > 0 AND post_w >= 2.0*pre_w THEN 1 ELSE 0 END)/COUNT(*),5)
FROM (SELECT repo_id,
             SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
             SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
      FROM events_part GROUP BY repo_id) p
UNION ALL
SELECT 500, 3.0, COUNT(*),
       SUM(CASE WHEN post_w >= 500 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END),
       ROUND(100.0*SUM(CASE WHEN post_w >= 500 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END)/COUNT(*),5)
FROM (SELECT repo_id,
             SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
             SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
      FROM events_part GROUP BY repo_id) p
UNION ALL
SELECT 1000, 3.0, COUNT(*),
       SUM(CASE WHEN post_w >= 1000 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END),
       ROUND(100.0*SUM(CASE WHEN post_w >= 1000 AND pre_w > 0 AND post_w >= 3.0*pre_w THEN 1 ELSE 0 END)/COUNT(*),5)
FROM (SELECT repo_id,
             SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
             SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
      FROM events_part GROUP BY repo_id) p;

SELECT * FROM q4_results ORDER BY stars_min, growth_min;
SQL