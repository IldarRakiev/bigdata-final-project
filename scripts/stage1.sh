#!/bin/bash
set -euo pipefail

echo "Stage 1: PostgreSQL + Sqoop"

# venv
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -r requirements.txt --quiet

password=$(head -n 1 secrets/.psql.pass)
JDBC_URL="jdbc:postgresql://hadoop-04.uni.innopolis.ru/team28_projectdb"
WAREHOUSE_DIR="project/warehouse"

# --- HDFS quota sanity check ---
echo ""
echo "[0/2] HDFS quota check..."
hdfs dfs -count -q -h /user/team28 || true
echo "  (raw CSVs are ~2.3 GB; with ×3 replication Sqoop needs ~7 GB free)"

# --- PostgreSQL ---
echo ""
echo "[1/2] Building PostgreSQL database..."
python3 scripts/build_projectdb.py

# --- Sqoop ---
echo ""
echo "[2/2] Importing data into HDFS via Sqoop..."
# skipTrash so we don't eat our own quota with every rerun
hadoop fs -rm -r -f -skipTrash "$WAREHOUSE_DIR" || true
# also flush Trash that piled up from previous runs
hdfs dfs -expunge || true

sqoop import-all-tables \
    --connect "$JDBC_URL" \
    --username team28 \
    --password "$password" \
    --compression-codec=snappy \
    --compress \
    --as-avrodatafile \
    --warehouse-dir="$WAREHOUSE_DIR" \
    --m 1

# Move generated schema/java next to the project (best-effort).
mv ./*.avsc output/ 2>/dev/null || true
mv ./*.java output/ 2>/dev/null || true

# Hard verification — fail if directories are empty.
echo ""
echo "Verifying HDFS data..."
for sub in repositories events; do
    echo "--- $sub ---"
    if ! hadoop fs -ls "$WAREHOUSE_DIR/$sub" 2>/dev/null | grep -q part-m; then
        echo "FATAL: Sqoop produced no part-m-* files in $WAREHOUSE_DIR/$sub" >&2
        hdfs dfs -count -q -h /user/team28 >&2 || true
        exit 3
    fi
    hadoop fs -ls -h "$WAREHOUSE_DIR/$sub"
done

echo ""
echo "============================================"
echo "Stage 1 complete!"
echo "  PostgreSQL: team28_projectdb (2 tables)"
echo "  HDFS:       /user/team28/$WAREHOUSE_DIR"
echo "============================================"
