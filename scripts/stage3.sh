#!/bin/bash
set -euo pipefail

echo "============================================"
echo "Stage 3: Spark ML — train & evaluate models"
echo "============================================"

# Activate venv if it exists (for pylint, local deps).
if [ -d "venv" ]; then
    source venv/bin/activate
fi

mkdir -p data models output

# ---- 1. Run the ML pipeline on YARN ------------------------------
echo ""
echo "[1/2] Running scripts/model.py on YARN..."
spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf "spark.sql.catalogImplementation=hive" \
    scripts/model.py

# ---- 2. Fetch HDFS artifacts into the repo -----------------------
echo ""
echo "[2/2] Fetching artifacts from HDFS..."

# Train / test JSON.
rm -f data/train.json data/test.json
hdfs dfs -cat project/data/train/part-*.json > data/train.json
hdfs dfs -cat project/data/test/part-*.json  > data/test.json

# Models.
rm -rf models/model1 models/model2
hdfs dfs -get project/models/model1 models/model1
hdfs dfs -get project/models/model2 models/model2

# Predictions (single CSV each, with header).
rm -f output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv
hdfs dfs -cat project/output/model1_predictions.csv/part-*.csv \
    > output/model1_predictions.csv
hdfs dfs -cat project/output/model2_predictions.csv/part-*.csv \
    > output/model2_predictions.csv
hdfs dfs -cat project/output/evaluation.csv/part-*.csv \
    > output/evaluation.csv

echo ""
echo "============================================"
echo "Stage 3 complete!"
echo "  Models:        models/model1, models/model2"
echo "  Data splits:   data/train.json, data/test.json"
echo "  Predictions:   output/model{1,2}_predictions.csv"
echo "  Comparison:    output/evaluation.csv"
echo "============================================"