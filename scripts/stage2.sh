#!/bin/bash
set -euo pipefail

echo "============================================"
echo "Stage 2: Hive — storage preparation + EDA"
echo "============================================"

HIVE_HOST="${HIVE_HOST:-hadoop-03.uni.innopolis.ru}"
HIVE_PORT="${HIVE_PORT:-10001}"
HIVE_URL="jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}"
USER="team28"

if [ ! -f secrets/.hive.pass ]; then
    echo "ERROR: secrets/.hive.pass not found."
    echo "Create it with the cluster password (same as .psql.pass):"
    echo "  cp secrets/.psql.pass secrets/.hive.pass"
    exit 1
fi

password=$(head -n 1 secrets/.hive.pass)

mkdir -p output

# ---- 1. Upload AVRO schemas (from Stage 1) to HDFS ----
echo ""
echo "[1/3] Uploading AVRO schemas to HDFS..."
if ! ls output/*.avsc 1>/dev/null 2>&1; then
    echo "ERROR: No .avsc files in output/. Run Stage 1 first (Sqoop generates schemas)."
    exit 1
fi

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put -f output/*.avsc project/warehouse/avsc/
echo "  Schemas on HDFS:"
hdfs dfs -ls project/warehouse/avsc

# ---- 2. Build Hive database (partitioned + bucketed tables) ----
echo ""
echo "[2/3] Running sql/db.hql (database + partitioned/bucketed tables)..."
beeline -u "$HIVE_URL" \
    -n "$USER" \
    -p "$password" \
    --silent=false \
    -f sql/db.hql \
    2>&1 | tee output/hive_results.txt

# ---- 3. Run EDA queries q1..q6 and export each result as CSV ----
echo ""
echo "[3/3] Running EDA queries..."
for q in q1 q2 q3 q4 q5 q6; do
    echo ""
    echo "  --- $q.hql ---"
    beeline -u "$HIVE_URL" \
        -n "$USER" \
        -p "$password" \
        --silent=true \
        --outputformat=csv2 \
        --showHeader=true \
        --hiveconf hive.resultset.use.unique.column.names=false \
        -f "sql/$q.hql" \
        > "output/$q.csv"

    rows=$(($(wc -l < "output/$q.csv") - 1))
    echo "  $q done: output/$q.csv ($rows rows)"
done

echo ""
echo "============================================"
echo "Stage 2 complete!"
echo "  Hive DB:        team28_projectdb"
echo "  Tables:         repositories_buck (bucketed),"
echo "                  events_part (partitioned + bucketed)"
echo "  EDA results:    output/q1.csv .. output/q6.csv"
echo "  Setup log:      output/hive_results.txt"
echo "============================================"
