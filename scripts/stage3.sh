#!/bin/bash
set -euo pipefail

echo "============================================"
echo "Stage 3: Spark ML on YARN"
echo "============================================"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

mkdir -p data output models

# Defaults aligned with model.py and the project rubric.
# pre-2024 watches are sparse, so STARS=10/GROWTH=2x gives ~1.7k positives
# out of ~2.1M repos (≈0.08%), which is a workable class ratio for AUPR.
SUCCESS_STARS_MIN="${SUCCESS_STARS_MIN:-10}"
SUCCESS_GROWTH_MIN="${SUCCESS_GROWTH_MIN:-2.0}"
MIN_PRE_EVENTS="${MIN_PRE_EVENTS:-5}"
CV_FOLDS="${CV_FOLDS:-3}"

HDFS_MODELS_DIR="${HDFS_MODELS_DIR:-project/models}"
HDFS_DATA_DIR="${HDFS_DATA_DIR:-project/data}"
HDFS_OUTPUT_DIR="${HDFS_OUTPUT_DIR:-project/output}"

# ---- 1. Run Spark job on YARN ----
echo ""
echo "[1/3] Submitting Spark job..."
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 4 \
    --executor-memory 4G \
    --executor-cores 2 \
    --driver-memory 2G \
    --packages org.apache.spark:spark-avro_2.12:3.2.4 \
    --conf spark.sql.warehouse.dir=project/hive/warehouse \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.hadoop.hive.metastore.uris=thrift://hadoop-02.uni.innopolis.ru:9883 \
    scripts/model.py \
        --success-stars-min "$SUCCESS_STARS_MIN" \
        --success-growth-min "$SUCCESS_GROWTH_MIN" \
        --min-pre-events "$MIN_PRE_EVENTS" \
        --cv-folds "$CV_FOLDS" \
        --output-dir output \
        --models-dir "$HDFS_MODELS_DIR" \
        --data-dir "$HDFS_DATA_DIR" \
        --predictions-dir "$HDFS_OUTPUT_DIR" \
        "$@" \
    2>&1 | tee output/stage3.log

# ---- 2. Mirror trained models from HDFS to local models/ ----
echo ""
echo "[2/3] Copying trained models from HDFS to local models/..."
for name in rf svm nb; do
    if hdfs dfs -test -d "$HDFS_MODELS_DIR/$name" 2>/dev/null; then
        rm -rf "models/$name"
        hdfs dfs -copyToLocal "$HDFS_MODELS_DIR/$name" "models/$name"
        echo "  models/$name <- $HDFS_MODELS_DIR/$name"
    else
        echo "  skipped: $HDFS_MODELS_DIR/$name not on HDFS (model not trained this run)"
    fi
done

# ---- 3. Mirror data splits + per-model predictions + evaluation ----
# Spark `coalesce(1).write` produces a *directory* containing one
# part-* file. `getmerge` flattens the directory into a single local
# file, which is what the Stage 3 checklist expects.
echo ""
echo "[3/3] Copying data splits, predictions, and evaluation from HDFS..."

merge_from_hdfs() {
    local src="$1"
    local dst="$2"
    if hdfs dfs -test -d "$src" 2>/dev/null; then
        rm -f "$dst"
        hdfs dfs -getmerge "$src" "$dst"
        echo "  $dst <- $src"
    else
        echo "  WARN: $src not found on HDFS"
    fi
}

merge_from_hdfs "$HDFS_DATA_DIR/train"                  data/train.json
merge_from_hdfs "$HDFS_DATA_DIR/test"                   data/test.json
merge_from_hdfs "$HDFS_OUTPUT_DIR/rf_predictions.csv"   output/rf_predictions.csv
merge_from_hdfs "$HDFS_OUTPUT_DIR/svm_predictions.csv"  output/svm_predictions.csv
merge_from_hdfs "$HDFS_OUTPUT_DIR/nb_predictions.csv"   output/nb_predictions.csv
merge_from_hdfs "$HDFS_OUTPUT_DIR/evaluation.csv"       output/evaluation.csv

echo ""
echo "============================================"
echo "Stage 3 complete!"
echo "  Train/test splits: data/train.json, data/test.json"
echo "  Trained models:    models/{rf,svm,nb}/"
echo "  Predictions:       output/{rf,svm,nb}_predictions.csv"
echo "  Evaluation:        output/evaluation.csv"
echo "  Sample features:   output/stage3_sample_features.csv"
echo "  Sample prediction: output/stage3_sample_prediction.csv"
echo "  Driver log:        output/stage3.log"
echo "============================================"