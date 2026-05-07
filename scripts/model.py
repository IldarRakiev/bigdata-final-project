"""
Stage 3 — Spark ML on YARN.

Trains three binary classifiers (Random Forest, Linear SVC, Naive Bayes)
to predict whether a GitHub repository will become "high-potential" in
the 6 months after the temporal split point T = 2023-12-31.

Pseudo-label (per the team registration):
    success = 1
        when post_watches >= success_stars_min
         and post_watches >= success_growth_min * pre_watches
         and pre_watches  >  0
    success = 0  otherwise.

Where:
    pre_watches  = WatchEvent count in [2023-01-01, 2023-12-31]
    post_watches = WatchEvent count in [2024-01-01, 2024-06-30]

Reference: Borges & Valente, "What's in a GitHub Star?" (MSR 2018).

Run via scripts/stage3.sh, which spark-submits this module on YARN.
"""

import argparse
import csv
import os
import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, NaiveBayes, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import MinMaxScaler, StandardScaler, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SEED = 42
HIVE_DB = "team28_projectdb"


def parse_args():
    """Command-line knobs for thresholds and output paths."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--success-stars-min", type=int, default=500,
                        help="Minimum post-T WatchEvent count to qualify as success.")
    parser.add_argument("--success-growth-min", type=float, default=3.0,
                        help="Minimum post/pre WatchEvent ratio for success.")
    parser.add_argument("--min-pre-events", type=int, default=5,
                        help="Filter: drop repos with fewer pre-T events than this.")
    parser.add_argument("--cv-folds", type=int, default=4,
                        help="Number of cross-validation folds (2 < k < 5).")
    parser.add_argument("--train-frac", type=float, default=0.7,
                        help="Fraction of data assigned to the training set.")
    parser.add_argument("--output-dir", default="output",
                        help="Local directory for metrics CSV + sample prediction.")
    parser.add_argument("--models-dir", default="project/models",
                        help="HDFS directory where trained models are saved.")
    return parser.parse_args()


def build_dataset(spark, args):
    """Aggregate per-repo features (pre-T) and pseudo-label (post-T)."""
    df = spark.sql(f"""
        SELECT
            repo_id,
            SUM(CASE WHEN event_year = 2023 THEN event_count   ELSE 0 END) AS pre_total_events,
            SUM(CASE WHEN event_year = 2023 THEN unique_actors ELSE 0 END) AS pre_total_actors,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'PushEvent'        THEN event_count ELSE 0 END) AS pre_pushes,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'WatchEvent'       THEN event_count ELSE 0 END) AS pre_watches,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'PullRequestEvent' THEN event_count ELSE 0 END) AS pre_prs,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'IssuesEvent'      THEN event_count ELSE 0 END) AS pre_issues,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'ForkEvent'        THEN event_count ELSE 0 END) AS pre_forks,
            SUM(CASE WHEN event_year = 2023 AND event_type = 'CreateEvent'      THEN event_count ELSE 0 END) AS pre_creates,
            COUNT(DISTINCT CASE WHEN event_year = 2023 THEN event_date END)                                  AS pre_active_days,
            MAX(CASE WHEN event_year = 2023 THEN event_count ELSE 0 END)                                     AS pre_max_daily_events,
            SUM(CASE WHEN event_year = 2023 AND event_month BETWEEN 1 AND  6 AND event_type = 'WatchEvent'
                     THEN event_count ELSE 0 END) AS h1_watches,
            SUM(CASE WHEN event_year = 2023 AND event_month BETWEEN 7 AND 12 AND event_type = 'WatchEvent'
                     THEN event_count ELSE 0 END) AS h2_watches,
            SUM(CASE WHEN event_year = 2024 AND event_type = 'WatchEvent' THEN event_count ELSE 0 END)       AS post_watches
        FROM {HIVE_DB}.events_part
        GROUP BY repo_id
        HAVING pre_total_events >= {args.min_pre_events}
    """)

    df = (
        df
        .withColumn("push_share",   F.col("pre_pushes")  / F.col("pre_total_events"))
        .withColumn("watch_share",  F.col("pre_watches") / F.col("pre_total_events"))
        .withColumn("pr_share",     F.col("pre_prs")     / F.col("pre_total_events"))
        .withColumn("issues_share", F.col("pre_issues")  / F.col("pre_total_events"))
        .withColumn("fork_share",   F.col("pre_forks")   / F.col("pre_total_events"))
        .withColumn("create_share", F.col("pre_creates") / F.col("pre_total_events"))
        .withColumn("events_per_day",
                    F.col("pre_total_events") /
                    F.greatest(F.col("pre_active_days"), F.lit(1)))
        .withColumn("intra_growth",
                    F.col("h2_watches") /
                    F.greatest(F.col("h1_watches"), F.lit(1)))
        .withColumn(
            "label",
            (
                (F.col("post_watches") >= args.success_stars_min)
                & (F.col("post_watches") >= args.success_growth_min * F.col("pre_watches"))
                & (F.col("pre_watches") > 0)
            ).cast("double")
        )
    )

    feature_cols = [
        "pre_total_events", "pre_total_actors",
        "pre_pushes", "pre_watches", "pre_prs",
        "pre_issues", "pre_forks", "pre_creates",
        "pre_active_days", "pre_max_daily_events",
        "push_share", "watch_share", "pr_share",
        "issues_share", "fork_share", "create_share",
        "events_per_day", "intra_growth",
    ]

    return df.select(["repo_id", "label", *feature_cols]), feature_cols


def stratified_split(df, train_frac):
    """Split each class independently to preserve class ratio."""
    df_neg = df.filter(F.col("label") == 0.0)
    df_pos = df.filter(F.col("label") == 1.0)
    train_neg, test_neg = df_neg.randomSplit([train_frac, 1 - train_frac], seed=SEED)
    train_pos, test_pos = df_pos.randomSplit([train_frac, 1 - train_frac], seed=SEED)
    return train_neg.union(train_pos), test_neg.union(test_pos)


def build_pipelines(feature_cols):
    """Three binary classifiers, each with a 27-cell grid (3 hyperparameters × 3 values)."""
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

    # ---- Random Forest ----
    rf_scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
    rf = RandomForestClassifier(featuresCol="features", labelCol="label", seed=SEED)
    rf_pipeline = Pipeline(stages=[assembler, rf_scaler, rf])
    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100, 200])
        .addGrid(rf.maxDepth, [5, 10, 15])
        .addGrid(rf.maxBins, [32, 64, 128])
        .build()
    )

    # ---- Linear SVC ----
    svc_scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                                withMean=True, withStd=True)
    svc = LinearSVC(featuresCol="features", labelCol="label")
    svc_pipeline = Pipeline(stages=[assembler, svc_scaler, svc])
    svc_grid = (
        ParamGridBuilder()
        .addGrid(svc.regParam, [0.001, 0.01, 0.1])
        .addGrid(svc.maxIter, [50, 100, 200])
        .addGrid(svc.tol, [1e-6, 1e-4, 1e-2])
        .build()
    )

    # ---- Naive Bayes ----
    # Multinomial / Complement need non-negative features; MinMaxScaler ensures that.
    nb_scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
    nb = NaiveBayes(featuresCol="features", labelCol="label")
    nb_pipeline = Pipeline(stages=[assembler, nb_scaler, nb])
    nb_grid = (
        ParamGridBuilder()
        .addGrid(nb.smoothing, [0.5, 1.0, 2.0])
        .addGrid(nb.modelType, ["multinomial", "complement", "gaussian"])
        .addGrid(nb_scaler.max, [1.0, 5.0, 10.0])
        .build()
    )

    return [
        ("rf",  rf_pipeline,  rf_grid),
        ("svm", svc_pipeline, svc_grid),
        ("nb",  nb_pipeline,  nb_grid),
    ]


def evaluate(model, test_df, name):
    """Compute the two binary metrics required by the rubric."""
    preds = model.transform(test_df)
    auroc = BinaryClassificationEvaluator(
        metricName="areaUnderROC", labelCol="label"
    ).evaluate(preds)
    aupr = BinaryClassificationEvaluator(
        metricName="areaUnderPR", labelCol="label"
    ).evaluate(preds)
    return {"model": name, "auroc": round(auroc, 4), "aupr": round(aupr, 4)}


def write_csv(rows, fieldnames, path):
    """Local CSV writer for small driver-side outputs."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Stage3-EarlyDetection")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("Stage 3 — early detection ML on YARN")
    print(f"  Success thresholds: stars >= {args.success_stars_min}, "
          f"growth >= {args.success_growth_min}x")
    print(f"  Min pre-T events:   {args.min_pre_events}")
    print(f"  CV folds:           {args.cv_folds}")
    print("=" * 60)

    df, feature_cols = build_dataset(spark, args)
    df = df.cache()
    total = df.count()
    pos = df.filter(F.col("label") == 1.0).count()
    if total == 0:
        print("ERROR: empty dataset — check Hive tables / filter thresholds.",
              file=sys.stderr)
        sys.exit(1)
    print(f"  Repos in dataset:   {total:,}")
    print(f"  Positive class:     {pos:,} ({100.0 * pos / total:.4f}%)")

    train, test = stratified_split(df, args.train_frac)
    train_n, test_n = train.count(), test.count()
    print(f"  Train size:         {train_n:,}")
    print(f"  Test size:          {test_n:,}")

    pipelines = build_pipelines(feature_cols)
    evaluator = BinaryClassificationEvaluator(metricName="areaUnderPR", labelCol="label")

    metrics = []
    best_models = {}
    for name, pipeline, grid in pipelines:
        print(f"\n[Training {name.upper()}]  grid combinations: {len(grid)}")
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=args.cv_folds,
            parallelism=2,
            seed=SEED,
        )
        cv_model = cv.fit(train)
        best = cv_model.bestModel
        best_models[name] = best

        m = evaluate(best, test, name)
        print(f"  test AUROC: {m['auroc']}  |  AUPR: {m['aupr']}")
        metrics.append(m)

        model_path = f"{args.models_dir}/{name}"
        best.write().overwrite().save(model_path)
        print(f"  saved best model to (HDFS): {model_path}")

    metrics_path = os.path.join(args.output_dir, "stage3_metrics.csv")
    write_csv(metrics, ["model", "auroc", "aupr"], metrics_path)
    print(f"\nMetrics saved to {metrics_path}")

    # ---- Sample prediction on one specific instance ----
    sample_row = test.orderBy(F.col("label").desc()).limit(1).collect()
    if not sample_row:
        print("WARN: no test row available for sample prediction.")
    else:
        s = sample_row[0]
        print("\n[Sample prediction]")
        print(f"  repo_id={s['repo_id']}  true_label={int(s['label'])}")
        sample_df = test.filter(F.col("repo_id") == s["repo_id"]).limit(1)

        sample_rows = [{"feature": col, "value": s[col]} for col in feature_cols]
        write_csv(
            sample_rows,
            ["feature", "value"],
            os.path.join(args.output_dir, "stage3_sample_features.csv"),
        )

        prediction_rows = [{
            "repo_id": s["repo_id"],
            "true_label": int(s["label"]),
            "rf_prediction": None, "svm_prediction": None, "nb_prediction": None,
        }]
        for name, model in best_models.items():
            pred = model.transform(sample_df).select("prediction").first()["prediction"]
            print(f"  {name.upper()}: prediction = {pred}")
            prediction_rows[0][f"{name}_prediction"] = pred

        write_csv(
            prediction_rows,
            ["repo_id", "true_label", "rf_prediction", "svm_prediction", "nb_prediction"],
            os.path.join(args.output_dir, "stage3_sample_prediction.csv"),
        )

    spark.stop()
    print("\nStage 3 done.")


if __name__ == "__main__":
    main()
