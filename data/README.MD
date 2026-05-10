# data/

Datasets live here, both raw and processed. The big files are gitignored
(several GB total) - they're regenerable from BigQuery.

After the full pipeline runs, you'll find:

**Raw BigQuery exports** (from `scripts/data_collection.sh`)

- `events_daily.csv` - daily event counts from GH Archive, 10% sample
  (`MOD(repo.id, 10) = 0`), Jan 2023 through Jun 2024. ~2.9 GB.
  Columns: `event_type`, `repo_id`, `repo_name`, `event_date`,
  `event_count`, `unique_actors`.
- `repo_metadata.csv` - earliest CreateEvent per repo, used as a
  proxy for "first seen on GitHub". ~680 MB.
  Columns: `repo_id`, `repo_name`, `repo_first_seen`.
- `repo_languages.csv` - primary language per repo, from the public
  github_repos dataset. ~110 MB.
  Columns: `repo_name`, `language`, `language_bytes`.

**Cleaned CSVs** (from `scripts/preprocess.py`, what Stage 1 actually loads)

- `events_clean.csv` - same events but without `repo_name`, since
  `repo_name` lives in the dimension table.
- `repositories.csv` - repo dimension. Columns: `repo_id`, `repo_name`,
  `first_seen_at`, `language`.

**Train/test splits** (Stage 3, mirrored from HDFS by `stage3.sh`)

- `train.json`, `test.json` - JSON-lines, one repo per line, with
  `repo_id`, `label`, and 18 numerical features. Stratified 70/30
  split by `label`.

To regenerate from scratch on the cluster:

```bash
export GCS_BUCKET=gs://team28-bigdata-staging
bash scripts/data_collection.sh
bash scripts/preprocess.sh
# train/test JSON only after Stage 3:
bash scripts/stage3.sh
```
