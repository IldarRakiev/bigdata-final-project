"""
Stage 3 — Predictive Data Analytics with Spark ML.

Task: Regression — predict log(total_events + 1) for each repository
based on its language, age, first-seen time (cyclical encoded), and
aggregated event-type counts.

Pipeline:
    1. Read repositories from Hive, events from raw AVRO on HDFS.
    2. Build a feature table (one row per repo).
    3. Feature engineering: cyclical sin/cos for month, one-hot for language.
    4. Train/test split (saved to HDFS as JSON).
    5. Train two models (Linear Regression, GBT Regressor).
    6. Tune hyper-parameters via CrossValidator + ParamGrid.
    7. Save best models, predictions, and evaluation results to HDFS.

Run:
    spark-submit --master yarn scripts/model.py
"""
import math

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

TEAM = "team28"
WAREHOUSE = "project/hive/warehouse"
DB_NAME = f"{TEAM}_projectdb"
EVENTS_AVRO_PATH = "project/warehouse/events"

EVENT_TYPES = [
    "PushEvent",
    "WatchEvent",
    "ForkEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "CreateEvent",
]

# End of data window (June 30, 2024) — used to compute repo age.
WINDOW_END = "2024-06-30"


def build_spark():
    """Create a Spark session connected to Hive Metastore on YARN."""
    return (
        SparkSession.builder.appName(f"{TEAM} - Stage 3 - Spark ML")
        .master("yarn")
        .config(
            "hive.metastore.uris",
            "thrift://hadoop-02.uni.innopolis.ru:9883",
        )
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.avro.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )


def read_tables(spark):
    """Read data sources.

    - repositories_buck from Hive (works).
    - events from raw AVRO on HDFS (Hive events_part is empty / broken).
    """
    repos = spark.read.table(f"{DB_NAME}.repositories_buck")

    events = (
        spark.read.format("avro")
        .load(EVENTS_AVRO_PATH)
        .select(
            "event_type",
            "repo_id",
            "event_date",
            "event_count",
            "unique_actors",
        )
    )
    return repos, events


def normalise_first_seen(col):
    """Cast first_seen_at to timestamp.

    Sqoop can export it either as STRING (ISO) or as BIGINT epoch millis,
    depending on the source type. Handle both.
    """
    as_str = F.col(col).cast("string")
    # If the value is purely digits and 12-14 chars long → epoch millis.
    numeric_ts = F.when(
        as_str.rlike("^[0-9]{12,14}$"),
        (as_str.cast("double") / 1000.0).cast(T.TimestampType()),
    )
    # Otherwise try to parse as string timestamp.
    string_ts = F.to_timestamp(as_str)
    return F.coalesce(numeric_ts, string_ts)


def build_feature_table(repos, events):
    """Aggregate events by repo and join with repositories metadata."""
    # Per-repo totals by event type (pivot).
    per_type = (
        events.groupBy("repo_id")
        .pivot("event_type", EVENT_TYPES)
        .agg(F.sum("event_count"))
        .na.fill(0)
    )
    for ev in EVENT_TYPES:
        per_type = per_type.withColumnRenamed(ev, f"{ev}_cnt")

    # Totals across all event types.
    # event_date is stored as epoch-millis BIGINT; derive YYYY-MM for active_months.
    events_with_month = events.withColumn(
        "event_month_key",
        F.date_format(
            F.to_timestamp((F.col("event_date").cast("double") / 1000.0)),
            "yyyy-MM",
        ),
    )
    totals = events_with_month.groupBy("repo_id").agg(
        F.sum("event_count").alias("total_events"),
        F.sum("unique_actors").alias("total_actors"),
        F.countDistinct("event_month_key").alias("active_months"),
    )

    features = totals.join(per_type, "repo_id", "inner").join(
        repos, "repo_id", "inner"
    )

    # Replace missing language with a sentinel category (keeps the row).
    features = features.withColumn(
        "language", F.coalesce(F.col("language"), F.lit("Unknown"))
    )

    # first_seen_at is epoch-millis stored as STRING.
    features = features.withColumn(
        "first_seen_ts",
        F.to_timestamp(F.col("first_seen_at").cast("double") / 1000.0),
    )

    features = features.withColumn(
        "repo_age_days",
        F.datediff(F.lit(WINDOW_END), F.col("first_seen_ts")),
    ).withColumn(
        "first_seen_month", F.month("first_seen_ts")
    )

    two_pi = 2.0 * math.pi
    features = features.withColumn(
        "first_seen_month_sin",
        F.sin(F.col("first_seen_month") * F.lit(two_pi / 12.0)),
    ).withColumn(
        "first_seen_month_cos",
        F.cos(F.col("first_seen_month") * F.lit(two_pi / 12.0)),
    )

    features = features.withColumn(
        "label", F.log1p(F.col("total_events").cast("double"))
    )

    feature_cols = [
        "repo_id",
        "language",
        "repo_age_days",
        "first_seen_month_sin",
        "first_seen_month_cos",
        "total_actors",
        "active_months",
        *[f"{ev}_cnt" for ev in EVENT_TYPES],
        "label",
    ]
    # Replace remaining NULL numerics with 0 so no row is dropped.
    return features.select(*feature_cols).na.fill(0, subset=[
        "repo_age_days", "total_actors", "active_months",
        *[f"{ev}_cnt" for ev in EVENT_TYPES],
    ]).na.fill("Unknown", subset=["language"])


def build_preprocessing_pipeline():
    """Build the feature extraction pipeline."""
    indexer = StringIndexer(
        inputCol="language",
        outputCol="language_idx",
        handleInvalid="keep",
    )
    encoder = OneHotEncoder(
        inputCol="language_idx",
        outputCol="language_ohe",
    )
    numeric_cols = [
        "repo_age_days",
        "first_seen_month_sin",
        "first_seen_month_cos",
        "total_actors",
        "active_months",
        *[f"{ev}_cnt" for ev in EVENT_TYPES],
    ]
    assembler = VectorAssembler(
        inputCols=["language_ohe", *numeric_cols],
        outputCol="features",
        handleInvalid="skip",
    )
    return Pipeline(stages=[indexer, encoder, assembler])


def save_json(df, hdfs_path):
    """Save DataFrame as a single JSON file on HDFS."""
    (df.coalesce(1)
        .write.mode("overwrite")
        .format("json")
        .save(hdfs_path))


def save_csv(df, hdfs_path):
    """Save DataFrame as a single CSV (with header)."""
    (df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("sep", ",")
        .option("header", "true")
        .save(hdfs_path))


def train_evaluate(
    name, estimator, param_grid, train_df, test_df, evaluator_rmse, evaluator_r2
):
    """Fit via CrossValidator, evaluate best model, persist outputs."""
    print(f"\n=== {name}: grid search ===")
    cross_val = CrossValidator(
        estimator=estimator,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_rmse,
        numFolds=3,
        parallelism=2,
        seed=42,
    )
    cv_model = cross_val.fit(train_df)
    best_model = cv_model.bestModel

    predictions = best_model.transform(test_df)
    rmse = evaluator_rmse.evaluate(predictions)
    r2_score = evaluator_r2.evaluate(predictions)
    print(f"{name}: RMSE={rmse:.4f}  R2={r2_score:.4f}")

    best_model.write().overwrite().save(f"project/models/{name}")
    save_csv(
        predictions.select("label", "prediction"),
        f"project/output/{name}_predictions.csv",
    )
    return best_model, rmse, r2_score


def main():
    """End-to-end Stage 3 pipeline."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("Stage 3 — Spark ML")
    print("=" * 60)

    repos, events = read_tables(spark)
    print(f"repositories_buck rows: {repos.count():,}")
    print(f"events (raw AVRO) rows: {events.count():,}")

    features = build_feature_table(repos, events)
    n_features = features.count()
    print(f"feature rows (repos):   {n_features:,}")

    if n_features == 0:
        raise RuntimeError(
            "Empty feature set: check repositories_buck and events AVRO."
        )

    # Preprocessing pipeline.
    pre_pipeline = build_preprocessing_pipeline()
    pre_model = pre_pipeline.fit(features)
    transformed = pre_model.transform(features).select("features", "label")

    # 60 / 40 split.
    train_df, test_df = transformed.randomSplit([0.6, 0.4], seed=42)
    train_df = train_df.cache()
    test_df = test_df.cache()
    print(f"train rows: {train_df.count():,}")
    print(f"test  rows: {test_df.count():,}")

    save_json(train_df, "project/data/train")
    save_json(test_df, "project/data/test")

    evaluator_rmse = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2"
    )

    # --- Model 1: Linear Regression -----------------------------------
    lr = LinearRegression(featuresCol="features", labelCol="label")
    lr_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.01, 0.1, 1.0])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .build()
    )
    _, rmse1, r2_1 = train_evaluate(
        "model1", lr, lr_grid, train_df, test_df,
        evaluator_rmse, evaluator_r2,
    )

    # --- Model 2: GBT Regressor ---------------------------------------
    gbt = GBTRegressor(
        featuresCol="features", labelCol="label", seed=42, maxIter=40,
    )
    gbt_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [3, 5, 7])
        .addGrid(gbt.maxBins, [32, 64])
        .build()
    )
    _, rmse2, r2_2 = train_evaluate(
        "model2", gbt, gbt_grid, train_df, test_df,
        evaluator_rmse, evaluator_r2,
    )

    # --- Comparison ---------------------------------------------------
    comparison_rows = [
        ("LinearRegression", float(rmse1), float(r2_1)),
        ("GBTRegressor", float(rmse2), float(r2_2)),
    ]
    comparison_df = spark.createDataFrame(
        comparison_rows, ["model", "RMSE", "R2"]
    )
    comparison_df.show(truncate=False)
    save_csv(comparison_df, "project/output/evaluation.csv")

    print("Stage 3 finished successfully.")
    spark.stop()


if __name__ == "__main__":
    main()