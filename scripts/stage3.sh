#!/bin/bash
set -euo pipefail

echo "Stage 3: Spark ML on YARN"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

mkdir -p output models

SUCCESS_STARS_MIN="${SUCCESS_STARS_MIN:-500}"
SUCCESS_GROWTH_MIN="${SUCCESS_GROWTH_MIN:-3.0}"
MIN_PRE_EVENTS="${MIN_PRE_EVENTS:-5}"
CV_FOLDS="${CV_FOLDS:-4}"
HDFS_MODELS_DIR="${HDFS_MODELS_DIR:-project/models}"

# ---- Run Spark job on YARN ----
echo ""
echo "[1/2] Submitting Spark job..."
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
        "$@" \
    2>&1 | tee output/stage3.log

# ---- Mirror trained models from HDFS to local models/ ----
echo ""
echo "[2/2] Copying trained models from HDFS to local models/..."
for name in rf svm nb; do
    if hdfs dfs -test -d "$HDFS_MODELS_DIR/$name" 2>/dev/null; then
        rm -rf "models/$name"
        hdfs dfs -copyToLocal "$HDFS_MODELS_DIR/$name" "models/$name"
        echo "  models/$name <- $HDFS_MODELS_DIR/$name"
    else
        echo "  skipped: $HDFS_MODELS_DIR/$name not on HDFS (model not trained this run)"
    fi
done

echo ""
echo "============================================"
echo "Stage 3 complete!"
echo "  Metrics:           output/stage3_metrics.csv"
echo "  Sample features:   output/stage3_sample_features.csv"
echo "  Sample prediction: output/stage3_sample_prediction.csv"
echo "  Trained models:    models/{rf,svm,nb}/"
echo "  Driver log:        output/stage3.log"
echo "============================================"