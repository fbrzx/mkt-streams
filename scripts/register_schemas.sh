#!/usr/bin/env bash
set -euo pipefail

SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
SCHEMA_DIR=${SCHEMA_DIR:-$(cd "$(dirname "$0")/.." && pwd)/schemas}

if [[ ! -d "$SCHEMA_DIR" ]]; then
  echo "Schema directory not found: $SCHEMA_DIR" >&2
  exit 1
fi

for schema_file in "$SCHEMA_DIR"/*.avsc; do
  [[ -e "$schema_file" ]] || continue
  subject="$(basename "${schema_file%.avsc}")-value"
  payload=$(python3 - "$schema_file" <<'PY'
import json, sys, pathlib
schema_path = pathlib.Path(sys.argv[1])
text = schema_path.read_text()
print(json.dumps({"schema": text}))
PY
  )
  echo "Registering $subject via $SCHEMA_REGISTRY_URL"
  http_code=$(curl -s -o /tmp/register_response.json -w "%{http_code}" \
    -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "$payload" \
    "$SCHEMA_REGISTRY_URL/subjects/$subject/versions")

  case "$http_code" in
    200|201)
      schema_id=$(python3 - <<'PY'
import json
with open('/tmp/register_response.json') as f:
    data = json.load(f)
print(data.get('id', 'unknown'))
PY
      )
      echo " -> schema id: $schema_id"
      ;;
    409)
      echo " -> schema already registered"
      ;;
    *)
      echo "Registration failed for $subject (HTTP $http_code):" >&2
      cat /tmp/register_response.json >&2
      rm -f /tmp/register_response.json
      exit 1
      ;;
  esac
  rm -f /tmp/register_response.json
  echo "------------------------------"
done
