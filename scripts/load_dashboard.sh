#!/bin/bash
# Stage 3 → Hive: load model results for the Superset dashboard.
# Idempotent — re-runnable.
set -euo pipefail

cd "$(dirname "$0")/.."

PASS_FILE="secrets/.hive.pass"
HIVE_URL="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001"
STAGING="project/dashboard_csv"

echo "[1/2] Uploading CSVs to HDFS..."
hdfs dfs -mkdir -p "$STAGING"
hdfs dfs -put -f output/stage3_metrics.csv  "$STAGING/"
hdfs dfs -put -f output/rf_predictions.csv  "$STAGING/"
hdfs dfs -put -f output/svm_predictions.csv "$STAGING/"
hdfs dfs -put -f output/nb_predictions.csv  "$STAGING/"

echo "[2/2] Loading into Hive..."
beeline -u "$HIVE_URL" -n team28 -p "$(cat $PASS_FILE)" <<'SQL'
USE team28_projectdb;

DROP TABLE IF EXISTS model_evaluation;
CREATE TABLE model_evaluation (model STRING, auroc DOUBLE, aupr DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');
LOAD DATA INPATH '/user/team28/project/dashboard_csv/stage3_metrics.csv' INTO TABLE model_evaluation;

DROP TABLE IF EXISTS rf_predictions;
CREATE TABLE rf_predictions (label DOUBLE, prediction DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');
LOAD DATA INPATH '/user/team28/project/dashboard_csv/rf_predictions.csv' INTO TABLE rf_predictions;

DROP TABLE IF EXISTS svm_predictions;
CREATE TABLE svm_predictions (label DOUBLE, prediction DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');
LOAD DATA INPATH '/user/team28/project/dashboard_csv/svm_predictions.csv' INTO TABLE svm_predictions;

DROP TABLE IF EXISTS nb_predictions;
CREATE TABLE nb_predictions (label DOUBLE, prediction DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');
LOAD DATA INPATH '/user/team28/project/dashboard_csv/nb_predictions.csv' INTO TABLE nb_predictions;

SELECT * FROM model_evaluation;
SQL

echo "Done."