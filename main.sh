#!/bin/bash
# Pre-processing
echo "Running pre-processing"
bash scripts/preprocess.sh

# Run the big data pipeline
echo "Running Stage 1 of the pipeline - PostgreSQL + Sqoop"
bash scripts/stage1.sh

echo "Running Stage 2 of the pipeline - Hive + Spark SQL"
bash scripts/stage2.sh

# STAGE2_GUARD_V1
# If Stage 2 (Hive) failed, don't even try Stage 3 — it will only produce
# misleading "Table not found" errors. beeline returns non-zero on DAG failure,
# so $? captures it reliably.
if [[ $? -ne 0 ]]; then
    echo "FATAL: Stage 2 failed (beeline exit != 0). Aborting before Stage 3." >&2
    exit 2
fi


echo "Running Stage 3 of the pipeline - Spark ML"
bash scripts/stage3.sh

echo "Running Stage 4 of the pipeline - Streamlit"
bash scripts/stage4.sh

# Post-processing 
echo "Running post-processing!"
bash scripts/postprocess.sh


# Check the quality of the codes
echo "The quality of scripts in 'scripts/' folder\n"
echo "::============================================::"
pylint scripts


echo "Done testing the pipeline!"
