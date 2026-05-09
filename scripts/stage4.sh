#!/bin/bash
set -euo pipefail

# Stage 4 — Data presentation in Apache Superset.
#
# The dashboard itself is built through the Superset web UI (the
# course rubric explicitly says: "automate the tasks above except
# the tasks in Apache Superset"). This script does the part we *can*
# automate: checking that all the inputs the dashboard needs are
# actually present, and listing what's been exported back from the UI
# so we know how close we are to "done".
#
# Concrete steps for the manual part live in
# notebooks/superset_runbook.md.

echo "============================================"
echo "Stage 4: Apache Superset dashboard"
echo "============================================"

# ---- 1. Data prerequisites for the dashboard ----
echo ""
echo "[1/3] Checking prerequisites from Stages 2-3..."

required=(
    output/q1.csv
    output/q2.csv
    output/q3.csv
    output/q4.csv
    output/q5.csv
    output/q6.csv
    output/stage3_metrics.csv
    output/stage3_sample_features.csv
    output/stage3_sample_prediction.csv
)

missing=0
for f in "${required[@]}"; do
    if [ -f "$f" ]; then
        size=$(wc -c < "$f")
        printf "  ok      %-45s %10s bytes\n" "$f" "$size"
    else
        printf "  MISSING %-45s\n" "$f"
        missing=$((missing + 1))
    fi
done

if [ "$missing" -gt 0 ]; then
    echo ""
    echo "ERROR: $missing prerequisite file(s) missing. Run earlier stages first:"
    echo "  bash scripts/stage2.sh   # produces output/q*.csv"
    echo "  bash scripts/stage3.sh   # produces output/stage3_*.csv"
    exit 1
fi

# ---- 2. Charts exported from Superset (manual UI step) ----
echo ""
echo "[2/3] Charts exported from Superset..."

chart_jpgs=(
    output/q1.jpg
    output/q2.jpg
    output/q3.jpg
    output/q4.jpg
    output/q5.jpg
    output/q6.jpg
)
exported=0
for jpg in "${chart_jpgs[@]}"; do
    if [ -f "$jpg" ]; then
        printf "  ok       %s\n" "$jpg"
        exported=$((exported + 1))
    else
        printf "  pending  %s\n" "$jpg"
    fi
done
echo "  -> $exported / ${#chart_jpgs[@]} chart images present"

if [ -f output/dashboard.jpg ]; then
    echo "  ok       output/dashboard.jpg (full-dashboard screenshot)"
else
    echo "  pending  output/dashboard.jpg (full-dashboard screenshot)"
fi

if [ -f output/dashboard.json ]; then
    echo "  ok       output/dashboard.json (Superset export)"
else
    echo "  pending  output/dashboard.json (Superset export)"
fi

# ---- 3. What to do next, if anything is pending ----
if [ "$exported" -lt "${#chart_jpgs[@]}" ] || [ ! -f output/dashboard.jpg ]; then
    echo ""
    echo "[3/3] Manual steps still required:"
    echo "  1. Open Apache Superset on the cluster."
    echo "  2. Follow notebooks/superset_runbook.md to build:"
    echo "     - 6 EDA charts (q1..q6)"
    echo "     - 1 model-performance chart (from stage3_metrics.csv)"
    echo "     - 1 dashboard combining everything"
    echo "  3. Export each chart as image -> output/qX.jpg."
    echo "  4. Take a full-dashboard screenshot -> output/dashboard.jpg."
    echo "  5. Optional: export the dashboard JSON -> output/dashboard.json"
    echo "     (Dashboards -> ... -> Export to YAML/JSON)."
else
    echo ""
    echo "[3/3] All Superset artifacts present."
fi

echo ""
echo "============================================"
if [ -f output/dashboard.jpg ]; then
    dashboard_state="dashboard ok"
else
    dashboard_state="dashboard pending"
fi
echo "Stage 4 status: $exported / ${#chart_jpgs[@]} charts, $dashboard_state"
echo "============================================"
