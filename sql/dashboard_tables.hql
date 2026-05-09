-- ============================================================
-- Stage 4 — Hive external tables over Stage 3 outputs
-- ============================================================
-- Spark writes the evaluation comparison and per-model predictions
-- as CSV directories on HDFS (`project/output/...`). Superset connects
-- to Hive, so to expose those CSVs as queryable datasets in the
-- dashboard we register external tables on top of them. Drop+create
-- for idempotency — tables are pure metadata, the CSV files stay.
--
-- Run via: beeline -f sql/dashboard_tables.hql
-- ============================================================

USE team28_projectdb;

-- Comparison dataframe (one row per trained model)
DROP TABLE IF EXISTS stage3_metrics;
CREATE EXTERNAL TABLE stage3_metrics (
    model  STRING,
    AUROC  DOUBLE,
    AUPR   DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/stage3_metrics.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Per-model test-set predictions (label, prediction)
DROP TABLE IF EXISTS rf_predictions;
CREATE EXTERNAL TABLE rf_predictions (
    label       DOUBLE,
    prediction  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/rf_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS svm_predictions;
CREATE EXTERNAL TABLE svm_predictions (
    label       DOUBLE,
    prediction  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/svm_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS nb_predictions;
CREATE EXTERNAL TABLE nb_predictions (
    label       DOUBLE,
    prediction  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/nb_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Smoke checks
SHOW TABLES;
SELECT 'stage3_metrics'  AS t, COUNT(*) AS rows FROM stage3_metrics
UNION ALL
SELECT 'rf_predictions',     COUNT(*)          FROM rf_predictions
UNION ALL
SELECT 'svm_predictions',    COUNT(*)          FROM svm_predictions
UNION ALL
SELECT 'nb_predictions',     COUNT(*)          FROM nb_predictions;
