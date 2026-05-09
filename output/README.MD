# output/

Everything the pipeline produces — logs, CSV results, predictions,
evaluation, exported charts.

## From Stage 1 (PostgreSQL + Sqoop)

- `stage1.log` — driver log of the Stage 1 run.
- `events.avsc`, `repositories.avsc` — AVRO schemas Sqoop generated
  during `import-all-tables`. Stage 2 re-uploads these to HDFS.
- `events.java`, `repositories.java` — Sqoop-generated Java classes.
  Kept for traceability; nothing downstream consumes them.

## From Stage 2 (Hive + EDA)

`hive_results.txt` is one big concatenated beeline log: first the
`db.hql` setup output, then per-query execution logs for `q1..q6`.

The six EDA query results, one CSV each:

- `q1.csv` — top language ecosystems by pre-T WatchEvent volume.
  Columns: `language`, `total_repos`, `pre_t_stars`,
  `candidate_repos`, `pct_candidates`.
- `q2.csv` — monthly star (WatchEvent) dynamics across the data
  window. Columns: `event_year`, `event_month`, `monthly_stars`,
  `repos_starred`, `avg_stars_per_starred_repo`.
- `q3.csv` — pre-T vs post-T growth distribution.
  Columns: `growth_bucket`, `repo_count`, `pct_of_total`.
- `q4.csv` — sensitivity of the success label to threshold choice.
  Columns: `stars_min`, `growth_min`, `total_repos`,
  `success_count`, `success_pct`.
- `q5.csv` — behavioural profile of success vs non-success cohorts.
  Columns: `is_success`, `repo_count`, `avg_pre_events`,
  `avg_pre_stars`, plus six `avg_*_share` columns (push, watch, pr,
  issues, fork, create).
- `q6.csv` — top accelerating repos in Q4 2023.
  Columns: `repo_id`, `repo_name`, `language`, `q3_2023_stars`,
  `q4_2023_stars`, `abs_jump`, `acceleration`.

The six chart exports from Superset (the visualisations of the same
six queries) end up here too: `q1.jpg`, `q2.jpg`, ..., `q6.jpg`.

## From Stage 3 (Spark ML)

- `stage3.log` — full driver log: class balance, training progress,
  per-model metrics, sample prediction.
- `evaluation.csv` — comparison of the three classifiers.
  Columns: `model` (`rf`/`svm`/`nb`), `AUROC`, `AUPR`.
- `rf_predictions.csv`, `svm_predictions.csv`, `nb_predictions.csv` —
  per-model predictions on the held-out test set, one CSV per model.
  Columns: `label`, `prediction`.
- `stage3_sample_features.csv` — the feature vector of the single
  sample instance used for the prediction demo.
  Columns: `feature`, `value`.
- `stage3_sample_prediction.csv` — predictions of all three models on
  that one sample. Columns: `repo_id`, `true_label`, `rf_prediction`,
  `svm_prediction`, `nb_prediction`.

## From Stage 4 (Dashboard)

- `dashboard.jpg` — screenshot of the assembled Superset dashboard
  (data characteristics, the six EDA charts, model performance,
  sample prediction).
