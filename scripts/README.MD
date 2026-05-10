# scripts/

Pipeline scripts. `main.sh` (in the repo root) chains them together
end-to-end; each one also runs standalone.

- **`data_collection.sh`** - Stage 0a. Pulls three CSVs from BigQuery
  into `data/`. One-time setup: needs `gcloud auth login` and a GCS
  bucket via `$GCS_BUCKET`. Idempotent - skips files that already exist.
- **`preprocess.sh`** + **`preprocess.py`** - Stage 0b. Normalises the
  raw CSVs into `events_clean.csv` and `repositories.csv` (the two
  files Stage 1 actually loads).
- **`stage1.sh`** + **`build_projectdb.py`** - Stage 1. Creates and
  fills the PostgreSQL database `team28_projectdb` on `hadoop-04`,
  then imports both tables to HDFS as Snappy-compressed AVRO via
  Sqoop. Generated `*.avsc` and `*.java` go to `output/`.
- **`stage2.sh`** - Stage 2. Uploads AVRO schemas to HDFS, wipes the
  previous Hive warehouse to keep re-runs idempotent, runs
  `sql/db.hql` (creates partitioned `events_part` and bucketed
  `repositories_buck`), then runs the six EDA queries
  `sql/q1.hql..q6.hql` and exports their results to `output/q*.csv`.
- **`stage3.sh`** + **`model.py`** - Stage 3. `spark-submit --master
  yarn` runs `model.py`, which trains three binary classifiers (RF,
  Linear SVC, Naive Bayes) with 27-cell hyperparameter grids and
  4-fold cross-validation, evaluated by AUROC + AUPR. Trained models,
  predictions, evaluation table, and train/test JSON splits are all
  copied from HDFS back into the repo at the end.
- **`stage4.sh`** - Stage 4 stub. The dashboard is built manually in
  Superset; see `notebooks/superset_runbook.md`.
- **`postprocess.sh`** - stub.

## A few config notes

- Cluster passwords go in `secrets/.psql.pass` and `secrets/.hive.pass`
  (gitignored). Same cluster password for both services.
- `data_collection.sh` needs `$GCS_BUCKET` and a logged-in `gcloud`
  account - without them it errors out with instructions.
- Stage 3 thresholds are env-vars: `SUCCESS_STARS_MIN`,
  `SUCCESS_GROWTH_MIN`, `MIN_PRE_EVENTS`, `CV_FOLDS`. Defaults match
  the pseudo-label registered with the team submission (500 stars,
  3× growth). For a smoke run pass `--smoke --only svm` through
  `stage3.sh "$@"`.
