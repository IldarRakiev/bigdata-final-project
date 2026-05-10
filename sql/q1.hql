-- q1 — Language ecosystems where breakout repos emerge

USE team28_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results (
    language          STRING,
    total_repos       BIGINT,
    pre_t_stars       BIGINT,
    candidate_repos   BIGINT,
    pct_candidates    DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q1_results';

INSERT OVERWRITE TABLE q1_results
SELECT
    r.language,
    COUNT(DISTINCT r.repo_id)                                            AS total_repos,
    SUM(CASE WHEN e.event_year = 2023 AND e.event_type = 'WatchEvent'
             THEN e.event_count ELSE 0 END)                              AS pre_t_stars,
    COUNT(DISTINCT CASE WHEN agg.pre_watches >= 10 THEN r.repo_id END)   AS candidate_repos,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN agg.pre_watches >= 10
                                      THEN r.repo_id END)
          / NULLIF(COUNT(DISTINCT r.repo_id), 0), 3)                     AS pct_candidates
FROM repositories_buck r
LEFT JOIN events_part e ON r.repo_id = e.repo_id
LEFT JOIN (
    SELECT repo_id,
           SUM(CASE WHEN event_year = 2023 AND event_type = 'WatchEvent'
                    THEN event_count ELSE 0 END) AS pre_watches
    FROM events_part
    GROUP BY repo_id
) agg ON r.repo_id = agg.repo_id
WHERE r.language IS NOT NULL
GROUP BY r.language
ORDER BY pre_t_stars DESC
LIMIT 15;

SELECT * FROM q1_results;
