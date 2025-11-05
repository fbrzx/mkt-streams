#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}"
export SPARK_CHECKPOINT_ROOT=${SPARK_CHECKPOINT_ROOT:-/tmp/delta/checkpoints}
PYTHON_BIN=${PYTHON_BIN:-python3}

if ! command -v java >/dev/null 2>&1; then
  cat <<'EOF' >&2
Error: Java runtime not found. Install a JDK (e.g. `brew install temurin@17`) and set JAVA_HOME.
EOF
  exit 1
fi

"$PYTHON_BIN" - <<'PY' || {
import sys

missing = []
for module in ("pyspark", "delta"):
    try:
        __import__(module)
    except ModuleNotFoundError:
        missing.append(module)

if missing:
    sys.stderr.write(
        "Error: Missing Python packages: {}\n"
        "Run `pip install -r spark/requirements.txt` in your active environment.\n".format(
            ", ".join(missing)
        )
    )
    sys.exit(1)
PY
  exit 1
}

echo "Priming Spark dependencies..."
"$PYTHON_BIN" -m spark.bootstrap_env

# Start base layer jobs (bronze and silver)
BASE_SCRIPTS=(
  ingest_stream.py
  silver_customers_orders.py
)

# Start gold layer job (depends on silver)
GOLD_SCRIPTS=(
  gold_c360.py
)

# Start publishing jobs (depend on gold)
PUBLISH_SCRIPTS=(
  publish_c360.py
  score_and_publish.py
)

pids=()

echo "=== Starting base layer jobs (bronze, silver) ==="
for script in "${BASE_SCRIPTS[@]}"; do
  echo "Launching $script"
  "$PYTHON_BIN" "$SCRIPT_DIR/$script" &
  pids+=("$!")
done

echo "Waiting 5 seconds for base layers to initialize..."
sleep 5

echo "=== Starting gold layer job ==="
for script in "${GOLD_SCRIPTS[@]}"; do
  echo "Launching $script"
  "$PYTHON_BIN" "$SCRIPT_DIR/$script" &
  pids+=("$!")
done

echo "Waiting 5 seconds for gold layer to initialize..."
sleep 5

echo "=== Starting publishing jobs ==="
for script in "${PUBLISH_SCRIPTS[@]}"; do
  echo "Launching $script"
  "$PYTHON_BIN" "$SCRIPT_DIR/$script" &
  pids+=("$!")
done

echo "All Spark jobs started."

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
