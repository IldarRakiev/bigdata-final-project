#!/bin/bash
set -e

echo "============================================"
echo "Stage 1: PostgreSQL + Sqoop"
echo "============================================"

# Activate virtual environment
source venv/bin/activate

# ---- Configuration ----
password=$(head -n 1 secrets/.psql.pass)
JDBC_URL="jdbc:postgresql://hadoop-04.uni.innopolis.ru/team28_projectdb"
WAREHOUSE_DIR="project/warehouse"

# ---- Step 1: Build PostgreSQL database ----
echo ""
echo "[1/2] Building PostgreSQL database..."
python3 scripts/build_projectdb.py

# ---- Step 2: Import into HDFS via Sqoop ----
echo ""
echo "[2/2] Importing data into HDFS via Sqoop..."

# Clear target directory if it exists
hadoop fs -rm -r -f "$WAREHOUSE_DIR"

sqoop import-all-tables \
    --connect "$JDBC_URL" \
    --username team28 \
    --password "$password" \
    --compression-codec=snappy \
    --compress \
    --as-avrodatafile \
    --warehouse-dir="$WAREHOUSE_DIR" \
    --m 1

# Save generated AVRO schema and Java files for Stage 2
mv *.avsc output/ 2>/dev/null || true
mv *.java output/ 2>/dev/null || true

echo ""
echo "Verifying HDFS data..."
echo "--- repositories ---"
hadoop fs -ls "$WAREHOUSE_DIR/repositories"
echo "--- events ---"
hadoop fs -ls "$WAREHOUSE_DIR/events"

echo ""
echo "============================================"
echo "Stage 1 complete!"
echo "  PostgreSQL: team28_projectdb (2 tables)"
echo "  HDFS:       /user/team28/$WAREHOUSE_DIR"
echo "  AVRO schemas: output/*.avsc"
echo "============================================"
