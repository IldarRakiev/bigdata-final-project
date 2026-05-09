#!/bin/bash
set -euo pipefail

# ============================================================
# Stage 0 — Data collection from Google BigQuery (GH Archive)
# ============================================================
# Materialises the three source CSVs that Stage 1 expects in data/:
#   data/events_daily.csv     (~2.9 GB)  — all-events 10% sample
#   data/repo_metadata.csv    (~680 MB)  — earliest CreateEvent per repo
#   data/repo_languages.csv   (~110 MB)  — primary language per repo
#
# The actual SQL is kept in sql/bigquery_*.sql so the source-of-truth
# stays in the repo. This script just runs each query, dumps the result
# table into a temporary GCS bucket as CSV shards, and concatenates
# them back to a single local file.
#
# Prerequisites (one-time setup, not automated here):
#   1. gcloud + bq + gsutil installed
#   2. gcloud auth login  (and a default project set)
#   3. GCS bucket for staging exports — pass via $GCS_BUCKET
#
# Idempotent: if the target CSV already exists, the corresponding query
# is skipped. Re-run safely after a partial failure.
# ============================================================

mkdir -p data

GCS_BUCKET="${GCS_BUCKET:-}"
if [ -z "$GCS_BUCKET" ]; then
    cat <<EOF >&2
ERROR: GCS_BUCKET env var is not set.

This script needs a Google Cloud Storage bucket to stage BigQuery
exports before downloading them locally. Example:

    export GCS_BUCKET=gs://team28-bigdata-staging
    bash scripts/data_collection.sh

If you don't have a bucket yet:
    gsutil mb -l US gs://team28-bigdata-staging

If you already have the CSVs locally (e.g. someone else ran the
queries via the BigQuery web UI), just place them in data/ — this
script will see them and skip the download.
EOF
    # Skip downloads if all three files are already in place.
    if [ -f data/events_daily.csv ] \
       && [ -f data/repo_metadata.csv ] \
       && [ -f data/repo_languages.csv ]; then
        echo "All three source CSVs already exist in data/ — nothing to do."
        ls -la data/*.csv
        exit 0
    fi
    exit 1
fi

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
BQ_DATASET="${BQ_DATASET:-gharchive_temp}"
BQ_LOCATION="${BQ_LOCATION:-US}"

echo "============================================"
echo "Stage 0: Data collection from BigQuery"
echo "  Project:  $PROJECT_ID"
echo "  Dataset:  $BQ_DATASET ($BQ_LOCATION)"
echo "  GCS:      $GCS_BUCKET"
echo "============================================"

# Create the staging dataset if it doesn't exist (idempotent).
bq --location="$BQ_LOCATION" mk --force --dataset \
    "$PROJECT_ID:$BQ_DATASET" >/dev/null

run_query_export() {
    local sql_file="$1"
    local table="$2"
    local local_file="$3"

    if [ -f "$local_file" ]; then
        size=$(wc -c < "$local_file")
        echo "  [skip] $local_file already exists ($size bytes)"
        return
    fi

    echo ""
    echo "  [run]  $sql_file"
    echo "         -> bq table: $BQ_DATASET.$table"

    bq query \
        --use_legacy_sql=false \
        --destination_table="$BQ_DATASET.$table" \
        --replace \
        --quiet \
        < "$sql_file"

    echo "         -> exporting to $GCS_BUCKET/$table-*.csv"
    bq extract \
        --destination_format=CSV \
        --print_header=true \
        --field_delimiter=, \
        "$BQ_DATASET.$table" \
        "$GCS_BUCKET/$table-*.csv"

    local tmp_dir
    tmp_dir=$(mktemp -d)
    echo "         -> downloading shards into $tmp_dir"
    gsutil -m cp "$GCS_BUCKET/$table-*.csv" "$tmp_dir/"

    # Concatenate shards keeping a single header (each shard has one).
    echo "         -> concatenating into $local_file"
    local first=true
    for shard in "$tmp_dir"/*.csv; do
        if $first; then
            cat "$shard" > "$local_file"
            first=false
        else
            tail -n +2 "$shard" >> "$local_file"
        fi
    done

    rm -rf "$tmp_dir"
    gsutil -m rm "$GCS_BUCKET/$table-*.csv" >/dev/null
    echo "  done:  $local_file ($(wc -c < "$local_file") bytes)"
}

run_query_export sql/bigquery_events.sql         events_daily_10pct  data/events_daily.csv
run_query_export sql/bigquery_repo_metadata.sql  repo_metadata_10pct data/repo_metadata.csv
run_query_export sql/bigquery_repo_languages.sql repo_languages      data/repo_languages.csv

echo ""
echo "============================================"
echo "Data collection complete!"
ls -la data/*.csv
echo "============================================"
