-- ============================================================
-- Stage 2 — Hive: storage preparation
-- Run via: beeline -f sql/db.hql
--
-- Builds the analytical layer:
--   * raw external tables over Sqoop AVRO output (transient)
--   * events_part   — partitioned by (event_year, event_month) +
--                     bucketed by repo_id (32 buckets), AVRO+Snappy
--   * repositories_buck — bucketed by repo_id (16 buckets), AVRO+Snappy
--   * raw externals dropped after copy (per Stage 2 checklist)
-- ============================================================

SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

DROP DATABASE IF EXISTS team28_projectdb CASCADE;
CREATE DATABASE team28_projectdb LOCATION 'project/hive/warehouse';
USE team28_projectdb;

-- ----------------------------------------------------------------
-- 1) Raw external tables over Sqoop AVRO output (transient layer).
--    These will be dropped at the end of this script.
-- ----------------------------------------------------------------

CREATE EXTERNAL TABLE repositories_raw
STORED AS AVRO
LOCATION 'project/warehouse/repositories'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/repositories.avsc');

CREATE EXTERNAL TABLE events_raw
STORED AS AVRO
LOCATION 'project/warehouse/events'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/events.avsc');

-- Sanity check: confirm Sqoop-inferred column types.
DESCRIBE FORMATTED repositories_raw;
DESCRIBE FORMATTED events_raw;

-- ----------------------------------------------------------------
-- Note on date types:
--   Sqoop exports PostgreSQL DATE/TIMESTAMP as AVRO long (epoch
--   milliseconds), so events_raw.event_date arrives as BIGINT and
--   repositories_raw.first_seen_at arrives as STRING-of-millis.
--   Hence the from_unixtime(... / 1000) conversion below.
-- ----------------------------------------------------------------

-- ----------------------------------------------------------------
-- 2) Bucketed dimension table: repositories_buck
--    Bucketed by repo_id for efficient joins with events_part.
-- ----------------------------------------------------------------

CREATE EXTERNAL TABLE repositories_buck (
    repo_id       BIGINT,
    repo_name     STRING,
    first_seen_at TIMESTAMP,
    language      STRING
)
CLUSTERED BY (repo_id) INTO 16 BUCKETS
STORED AS AVRO
LOCATION 'project/hive/warehouse/repositories_buck'
TBLPROPERTIES ('avro.output.codec'='snappy');

INSERT OVERWRITE TABLE repositories_buck
SELECT
    repo_id,
    repo_name,
    CAST(from_unixtime(CAST(first_seen_at AS BIGINT) / 1000) AS TIMESTAMP) AS first_seen_at,
    language
FROM repositories_raw;

-- ----------------------------------------------------------------
-- 3) Partitioned + bucketed fact table: events_part
--    Partitioned by (event_year, event_month) — 18 partitions for
--    18 months of data, fits typical date-range filters.
--    Bucketed by repo_id to align with repositories_buck for joins.
-- ----------------------------------------------------------------

CREATE EXTERNAL TABLE events_part (
    event_type    STRING,
    repo_id       BIGINT,
    event_date    DATE,
    event_count   INT,
    unique_actors INT
)
PARTITIONED BY (event_year INT, event_month INT)
CLUSTERED BY (repo_id) INTO 32 BUCKETS
STORED AS AVRO
LOCATION 'project/hive/warehouse/events_part'
TBLPROPERTIES ('avro.output.codec'='snappy');

INSERT OVERWRITE TABLE events_part PARTITION (event_year, event_month)
SELECT
    event_type,
    repo_id,
    event_date,
    event_count,
    unique_actors,
    YEAR(event_date)  AS event_year,
    MONTH(event_date) AS event_month
FROM (
    SELECT
        event_type,
        repo_id,
        CAST(from_unixtime(CAST(event_date AS BIGINT) / 1000) AS DATE) AS event_date,
        event_count,
        unique_actors
    FROM events_raw
    WHERE event_date IS NOT NULL
) t
WHERE event_date IS NOT NULL;

-- ----------------------------------------------------------------
-- 4) Drop unpartitioned/unbucketed raw tables (Stage 2 checklist).
--    External — only Hive metadata is removed, HDFS files stay.
-- ----------------------------------------------------------------

DROP TABLE repositories_raw;
DROP TABLE events_raw;

-- ----------------------------------------------------------------
-- 5) Validation
-- ----------------------------------------------------------------

SHOW TABLES;
SHOW PARTITIONS events_part;

SELECT 'repositories_buck' AS table_name, COUNT(*) AS row_count
FROM repositories_buck;

SELECT 'events_part' AS table_name, COUNT(*) AS row_count
FROM events_part;
