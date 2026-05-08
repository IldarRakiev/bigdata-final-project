-- ============================================================
-- Stage 2 — Hive: storage preparation
-- Run via: beeline -f sql/db.hql
--
-- Builds the analytical layer:
--   * raw external tables over Sqoop AVRO output (transient)
--   * events_part         — partitioned by (event_year, event_month), AVRO+Snappy
--   * repositories_buck   — bucketed by repo_id (16 buckets),           AVRO+Snappy
--   * raw externals dropped after copy (per Stage 2 checklist)
-- ============================================================

-- Execution engine & dynamic partitioning
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.enforce.bucketing=true;

-- IMPORTANT: do NOT set hive.exec.compress.output / avro.output.codec here.
-- On this cluster those session-wide flags interact badly with
-- hive.optimize.sort.dynamic.partition + AvroSerDe: the reducer silently
-- drops the partition keys and writes every row into (event_year=NULL,
-- event_month=NULL), which then fails to register — the table ends up
-- with 0 rows / 0 partitions despite the INSERT "succeeding".
-- Per-table Snappy is already declared via TBLPROPERTIES('avro.output.codec'='snappy')
-- on both repositories_buck and events_part below, which is sufficient.

-- Tez resources.
-- Default 512 MB containers OOM on the 44M-row INSERT with dynamic
-- partitioning. 4 GB containers + 3.2 GB JVM + sort.dynamic.partition
-- (streams one partition at a time through the reducer, so the mapper
-- never buffers 18 open writers at once) is what actually fits on the
-- team cluster (3 executors × 1 GB quota per node -> shared pool).
SET tez.am.resource.memory.mb=2048;
SET hive.tez.container.size=4096;
SET hive.tez.java.opts=-Xmx3200m;
SET tez.task.resource.memory.mb=4096;
SET hive.optimize.sort.dynamic.partition=true;

SET hive.auto.convert.join=true;


DROP DATABASE IF EXISTS team28_projectdb CASCADE;
CREATE DATABASE team28_projectdb LOCATION 'project/hive/warehouse';
USE team28_projectdb;

-- ----------------------------------------------------------------
-- 1) Raw external tables over Sqoop AVRO output (transient layer).
--    event_date / first_seen_at arrive as BIGINT epoch-millis
--    because Sqoop maps JDBC DATE/TIMESTAMP -> long in AVRO.
-- ----------------------------------------------------------------
CREATE EXTERNAL TABLE repositories_raw
    STORED AS AVRO
    LOCATION 'project/warehouse/repositories'
    TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/repositories.avsc');

CREATE EXTERNAL TABLE events_raw
    STORED AS AVRO
    LOCATION 'project/warehouse/events'
    TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/events.avsc');

DESCRIBE FORMATTED repositories_raw;
DESCRIBE FORMATTED events_raw;

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
    CAST(from_unixtime(CAST(first_seen_at AS BIGINT) DIV 1000) AS TIMESTAMP) AS first_seen_at,
    language
FROM repositories_raw;

-- ----------------------------------------------------------------
-- 3) Partitioned fact table: events_part
--
--    NOTE: We intentionally do NOT bucket events_part. On our small
--    cluster, combining dynamic partitioning over (year, month) with
--    CLUSTERED BY(repo_id) INTO 32 BUCKETS for 44M rows caused an
--    unrecoverable shuffle/stall in Tez. Partitioning alone is
--    sufficient for Stage 2 and gives good pruning on the YYYY-MM
--    filters used in EDA/ML.
--
--    The INSERT below was simplified after empirical tuning:
--      * single-level SELECT (no wrapping subquery) — one mapper pass
--      * to_date() instead of CAST(... AS DATE)     — fewer UDF hops
--      * year/month predicate lifted into WHERE     — partition pruning
--        applied before the group-by-partition redistribution
-- ----------------------------------------------------------------
CREATE EXTERNAL TABLE events_part (
                                      event_type    STRING,
                                      repo_id       BIGINT,
                                      event_date    DATE,
                                      event_count   INT,
                                      unique_actors INT
)
    PARTITIONED BY (event_year INT, event_month INT)
    STORED AS AVRO
    LOCATION 'project/hive/warehouse/events_part'
    TBLPROPERTIES ('avro.output.codec'='snappy');

INSERT OVERWRITE TABLE events_part PARTITION (event_year, event_month)
SELECT
    event_type,
    repo_id,
    to_date(from_unixtime(CAST(event_date AS BIGINT) DIV 1000))        AS event_date,
    event_count,
    unique_actors,
    YEAR (to_date(from_unixtime(CAST(event_date AS BIGINT) DIV 1000))) AS event_year,
    MONTH(to_date(from_unixtime(CAST(event_date AS BIGINT) DIV 1000))) AS event_month
FROM events_raw
WHERE event_date IS NOT NULL
  AND YEAR(to_date(from_unixtime(CAST(event_date AS BIGINT) DIV 1000))) BETWEEN 2023 AND 2024;

-- ----------------------------------------------------------------
-- 4) Drop transient raw tables (external — HDFS files stay intact).
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