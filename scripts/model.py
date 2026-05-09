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
    parser.add_argument("--success-stars-min", type=int, default=10,
                        help="Minimum post-T WatchEvent count to qualify as success.")
    parser.add_argument("--success-growth-min", type=float, default=2.0,
                        help="Minimum post/pre WatchEvent ratio for success.")
    parser.add_argument("--min-pre-events", type=int, default=5,
                        help="Filter: drop repos with fewer pre-T events than this.")
    parser.add_argument("--cv-folds", type=int, default=3,
                        help="Number of cross-validation folds (2 < k < 5).")
    parser.add_argument("--train-frac", type=float, default=0.7,
                        help="Fraction of data assigned to the training set.")
    parser.add_argument("--output-dir", default="output",
                        help="Local directory for metrics CSV + sample prediction.")
    parser.add_argument("--models-dir", default="project/models",
                        help="HDFS directory where trained models are saved.")
    parser.add_argument("--only", choices=["rf", "svm", "nb"], default=None,
                        help="Train only one model (default: all three).")
    parser.add_argument("--smoke", action="store_true",
                        help="Mini grid + 2 folds + only=svm. Full dataset.")
    parser.add_argument("--data-dir", default="project/data",
                        help="HDFS directory for train/test JSON splits.")
    parser.add_argument("--predictions-dir", default="project/output",
                        help="HDFS directory for per-model predictions and evaluation.")
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


def build_pipelines(feature_cols, smoke=False):
    """Three binary classifiers; minimal grids per the project rubric (>=2 hyperparams)."""
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

    # ---- Random Forest ----
    rf_scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
    rf = RandomForestClassifier(featuresCol="features", labelCol="label", seed=SEED)
    rf_pipeline = Pipeline(stages=[assembler, rf_scaler, rf])
    rf_grid = ParamGridBuilder()
    if smoke:
        rf_grid = rf_grid.addGrid(rf.numTrees, [20]).addGrid(rf.maxDepth, [5])
    else:
        rf_grid = (rf_grid.addGrid(rf.numTrees, [50, 150])
                          .addGrid(rf.maxDepth, [5, 10]))
    rf_grid = rf_grid.build()

    # ---- Linear SVC ----
    svc_scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                                withMean=True, withStd=True)
    svc = LinearSVC(featuresCol="features", labelCol="label", maxIter=50)
    svc_pipeline = Pipeline(stages=[assembler, svc_scaler, svc])
    svc_grid = ParamGridBuilder()
    if smoke:
        svc_grid = svc_grid.addGrid(svc.regParam, [0.01]).addGrid(svc.maxIter, [20])
    else:
        svc_grid = (svc_grid.addGrid(svc.regParam, [0.01, 0.1])
                            .addGrid(svc.maxIter, [50, 100]))
    svc_grid = svc_grid.build()

    # ---- Naive Bayes ----
    # Multinomial / Complement need non-negative features; MinMaxScaler ensures that.
    # Gaussian is intentionally excluded: our features are skewed counts, not normal.
    nb_scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
    nb = NaiveBayes(featuresCol="features", labelCol="label")
    nb_pipeline = Pipeline(stages=[assembler, nb_scaler, nb])
    nb_grid = ParamGridBuilder()
    if smoke:
        nb_grid = (nb_grid.addGrid(nb.smoothing, [1.0])
                          .addGrid(nb.modelType, ["multinomial"]))
    else:
        nb_grid = (nb_grid.addGrid(nb.smoothing, [0.5, 1.0])
                          .addGrid(nb.modelType, ["multinomial", "complement"]))
    nb_grid = nb_grid.build()

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
    if args.smoke:
        if args.only is None:
            args.only = "svm"
        if args.cv_folds > 2:
            args.cv_folds = 2

    spark = (
        SparkSession.builder
        .appName("Stage3-EarlyDetection")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"USE {HIVE_DB}")

    # Sanity guard
    n_events = spark.sql("SELECT COUNT(*) AS c FROM events_part").collect()[0]["c"]
    n_repos = spark.sql("SELECT COUNT(*) AS c FROM repositories_buck").collect()[0]["c"]
    print(f"  events_part rows:    {n_events:,}", flush=True)
    print(f"  repositories_buck:   {n_repos:,}", flush=True)
    if n_events == 0:
        print("FATAL: events_part is empty. Stage 2 (Hive INSERT) failed. "
              "Fix Stage 2 and rerun — Stage 3 aborted.", file=sys.stderr)
        spark.stop()
        sys.exit(2)

    print("=" * 60)
    print("Stage 3 — early detection ML on YARN")
    if args.smoke:
        print("  *** SMOKE MODE *** (mini grid, k=2, single model)")
    print(f"  Models trained:     {args.only or 'rf, svm, nb'}")
    print(f"  Success thresholds: stars >= {args.success_stars_min}, "
          f"growth >= {args.success_growth_min}x")
    print(f"  Min pre-T events:   {args.min_pre_events}")
    print(f"  CV folds:           {args.cv_folds}")
    print("=" * 60, flush=True)

    df, feature_cols = build_dataset(spark, args)
    df = df.cache()
    total = df.count()
    pos = df.filter(F.col("label") == 1.0).count()
    if total == 0:
        print("ERROR: empty dataset — check Hive tables / filter thresholds.",
              file=sys.stderr)
        sys.exit(1)
    print(f"  Repos in dataset:   {total:,}")
    print(f"  Positive class:     {pos:,} ({100.0 * pos / total:.4f}%)", flush=True)

    train, test = stratified_split(df, args.train_frac)
    train = train.cache()
    test = test.cache()
    train_n, test_n = train.count(), test.count()
    print(f"  Train size:         {train_n:,}")
    print(f"  Test size:          {test_n:,}", flush=True)

    # Persist the splits to HDFS so the grader can inspect them and so
    # downstream stages (Stage 4 dashboard, future re-training) read the
    # exact same split. The Stage 3 checklist requires JSON at
    # project/data/{train,test}; stage3.sh mirrors them to data/*.json.
    split_cols = ["label", *feature_cols]
    (train.select(*split_cols)
          .coalesce(1)
          .write.mode("overwrite")
          .json(f"{args.data_dir}/train"))
    (test.select(*split_cols)
         .coalesce(1)
         .write.mode("overwrite")
         .json(f"{args.data_dir}/test"))
    print(f"  Saved splits to HDFS: {args.data_dir}/{{train,test}}", flush=True)

    pipelines = build_pipelines(feature_cols, smoke=args.smoke)
    if args.only:
        pipelines = [p for p in pipelines if p[0] == args.only]

    evaluator = BinaryClassificationEvaluator(metricName="areaUnderPR", labelCol="label")
    metrics = []
    best_models = {}

    for name, pipeline, grid in pipelines:
        n_fits = len(grid) * args.cv_folds
        print(f"\n[Training {name.upper()}]  {len(grid)} cells × {args.cv_folds} folds "
              f"= {n_fits} fits", flush=True)
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
        print(f"  test AUROC: {m['auroc']}  |  AUPR: {m['aupr']}", flush=True)
        metrics.append(m)

        # Save best model to HDFS
        model_path = f"{args.models_dir}/{name}"
        best.write().overwrite().save(model_path)
        print(f"  saved best model to (HDFS): {model_path}", flush=True)

        # Save full test-set predictions per model (label + prediction).
        predictions_path = f"{args.predictions_dir}/{name}_predictions.csv"
        (best.transform(test)
             .select("label", "prediction")
             .coalesce(1)
             .write.mode("overwrite")
             .option("header", "true")
             .csv(predictions_path))
        print(f"  saved test predictions to (HDFS): {predictions_path}", flush=True)

    # Comparison dataframe — required artifact (project/output/evaluation.csv).
    eval_rows = [(m["model"], float(m["auroc"]), float(m["aupr"])) for m in metrics]
    eval_df = spark.createDataFrame(eval_rows, ["model", "AUROC", "AUPR"])
    eval_df.show(truncate=False)
    eval_hdfs = f"{args.predictions_dir}/evaluation.csv"
    (eval_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(eval_hdfs))
    print(f"Evaluation saved to (HDFS): {eval_hdfs}", flush=True)

    # And a local copy for direct inspection in the repo (header matches HDFS).
    eval_local = os.path.join(args.output_dir, "evaluation.csv")
    local_rows = [{"model": m["model"], "AUROC": m["auroc"], "AUPR": m["aupr"]}
                  for m in metrics]
    write_csv(local_rows, ["model", "AUROC", "AUPR"], eval_local)
    print(f"Evaluation saved locally: {eval_local}", flush=True)

    # ---- Sample prediction on one specific instance ----
    sample_row = test.orderBy(F.col("label").desc()).limit(1).collect()
    if not sample_row:
        print("WARN: no test row available for sample prediction.")
    else:
        s = sample_row[0]
        print("\n[Sample prediction]")
        print(f"  repo_id={s['repo_id']}  true_label={int(s['label'])}", flush=True)
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
            "rf_prediction": None,
            "svm_prediction": None,
            "nb_prediction": None,
        }]
        for name, model in best_models.items():
            pred = model.transform(sample_df).select("prediction").first()["prediction"]
            print(f"  {name.upper()}: prediction = {pred}", flush=True)
            prediction_rows[0][f"{name}_prediction"] = pred

        write_csv(
            prediction_rows,
            ["repo_id", "true_label", "rf_prediction", "svm_prediction", "nb_prediction"],
            os.path.join(args.output_dir, "stage3_sample_prediction.csv"),
        )

    spark.stop()
    print("\nStage 3 done.", flush=True)


if __name__ == "__main__":
    main()