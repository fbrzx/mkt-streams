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

SCRIPTS=(
  ingest_stream.py
  silver_customers_orders.py
  gold_c360.py
  score_and_publish.py
)

pids=()

for script in "${SCRIPTS[@]}"; do
  echo "Launching $script"
  "$PYTHON_BIN" "$SCRIPT_DIR/$script" &
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
