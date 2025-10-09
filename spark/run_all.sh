#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}"
export SPARK_CHECKPOINT_ROOT=${SPARK_CHECKPOINT_ROOT:-/tmp/delta/checkpoints}

SCRIPTS=(
  ingest_stream.py
  silver_customers_orders.py
  gold_c360.py
  score_and_publish.py
)

pids=()

for script in "${SCRIPTS[@]}"; do
  echo "Launching $script"
  python3 "$SCRIPT_DIR/$script" &
  pids+=("$!")
done

terminate() {
  echo "Stopping Spark jobs"
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
    fi
  done
}

trap terminate SIGINT SIGTERM

for pid in "${pids[@]}"; do
  wait "$pid"
done
