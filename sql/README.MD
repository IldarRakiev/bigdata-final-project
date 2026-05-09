# sql/

All SQL and HiveQL the pipeline uses, grouped by stage.

## BigQuery extracts (Stage 0)

These are the source-of-truth queries that materialise our raw dataset.
`scripts/data_collection.sh` runs each one against
`githubarchive.month.*` and the public `bigquery-public-data.github_repos`.

- `bigquery_events.sql` — daily event counts per repo, 10% sample
  (`MOD(repo.id, 10) = 0`), Jan 2023 through Jun 2024. Six event types:
  WatchEvent, ForkEvent, PullRequestEvent, PushEvent, IssuesEvent,
  CreateEvent.
- `bigquery_repo_metadata.sql` — earliest CreateEvent per repo,
  treated as the "first seen on GitHub" timestamp.
- `bigquery_repo_languages.sql` — primary language per repo from the
  public github_repos dataset.

## PostgreSQL setup (Stage 1)

`scripts/build_projectdb.py` runs these against `team28_projectdb` on
`hadoop-04.uni.innopolis.ru`.

- `create_tables.sql` — DDL for the `repositories` and `events` tables
  with primary-key constraints.
- `import_data.sql` — `COPY FROM STDIN` commands consumed by
  `psycopg2.copy_expert`.
- `test_database.sql` — sanity checks (row counts, language top-10).

## Hive setup (Stage 2)

`scripts/stage2.sh` runs `db.hql` via beeline. It creates the Hive
database `team28_projectdb`, registers raw external tables over the
Sqoop AVRO output, then builds the analytical layer:

- `repositories_buck` — bucketed by `repo_id` (16 buckets).
- `events_part` — partitioned by `(event_year, event_month)`.

Date columns in `events_raw` and `repositories_raw` arrive as
epoch-millis BIGINTs (Sqoop maps PostgreSQL DATE/TIMESTAMP to AVRO
`long`), so they're reconstructed via `from_unixtime(... DIV 1000)`.
The `DIV` (integer division) part matters — Hive 3 rejects `/`
between numeric and `timestamp_seconds()` arguments.

## EDA queries (Stage 2, six insights)

Each `qX.hql` writes a result table `qX_results` into Hive at
`project/hive/warehouse/qX_results` and selects it back. Their
results are exported as `output/qX.csv` by `stage2.sh`.

- `q1.hql` — language ecosystems where breakout repos emerge.
- `q2.hql` — monthly WatchEvent dynamics across the data window.
- `q3.hql` — pre-T vs post-T growth-rate distribution. Drives the
  calibration of the 300%-growth threshold.
- `q4.hql` — sensitivity of the success label to the choice of
  `(stars_min, growth_min)`.
- `q5.hql` — behavioural profile of success vs non-success cohorts.
- `q6.hql` — acceleration anomalies in Q4 2023 (top-20 repos by
  Q4/Q3 WatchEvent ratio).

The six together support the project narrative — early detection of
high-potential repositories via temporal growth pseudo-labels.
