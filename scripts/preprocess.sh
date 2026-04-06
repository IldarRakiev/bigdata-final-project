#!/bin/bash
set -e

echo "============================================"
echo "Pre-processing: Preparing raw CSVs for PostgreSQL"
echo "============================================"

# Create virtual environment if it does not exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt --quiet

python3 scripts/preprocess.py --data-dir data

echo "Pre-processing finished successfully."
