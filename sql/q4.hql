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

DROP TABLE IF EXISTS q4_thresholds_tmp;
CREATE TABLE q4_thresholds_tmp (stars_min INT, growth_min DOUBLE)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'data/q4_thresholds.csv' INTO TABLE q4_thresholds_tmp;

INSERT OVERWRITE TABLE q4_results
SELECT th.stars_min, th.growth_min, COUNT(*),
       SUM(CASE WHEN per.post_w >= th.stars_min AND per.pre_w > 0
           AND per.post_w >= th.growth_min * per.pre_w THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN per.post_w >= th.stars_min AND per.pre_w > 0
           AND per.post_w >= th.growth_min * per.pre_w THEN 1 ELSE 0 END) / COUNT(*), 5)
FROM (
         SELECT repo_id,
                SUM(CASE WHEN event_year=2023 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS pre_w,
                SUM(CASE WHEN event_year=2024 AND event_type='WatchEvent' THEN event_count ELSE 0 END) AS post_w
         FROM events_part GROUP BY repo_id
     ) per
         CROSS JOIN q4_thresholds_tmp th
GROUP BY th.stars_min, th.growth_min
ORDER BY th.stars_min, th.growth_min;

DROP TABLE q4_thresholds_tmp;
SELECT * FROM q4_results;