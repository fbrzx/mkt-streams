#!/usr/bin/env bash
set -euo pipefail

REST_PROXY_URL=${REST_PROXY_URL:-http://localhost:8082}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
TOPIC=${CUSTOMER_TOPIC:-dom.customer.profile.upsert.v1}
SUBJECT=${CUSTOMER_SUBJECT:-dom.customer.profile.upsert.v1-value}

echo "Fetching schema id for $SUBJECT"
schema_tmp=$(mktemp)
trap 'rm -f "$schema_tmp"' EXIT
http_code=$(curl -s -o "$schema_tmp" -w "%{http_code}" \
  "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions/latest") || http_code="000"

if [[ "$http_code" != "200" ]]; then
  echo "Unable to resolve schema id for $SUBJECT (HTTP $http_code)" >&2
  if [[ -s "$schema_tmp" ]]; then
    cat "$schema_tmp" >&2
  fi
  exit 1
fi

schema_id=$(python3 - "$schema_tmp" <<'PY'
import json, sys, pathlib
path = pathlib.Path(sys.argv[1])
with path.open() as f:
    payload = json.load(f)
schema_id = payload.get('id')
if not schema_id:
    raise SystemExit("Schema id missing from response")
print(schema_id)
PY
)

rm -f "$schema_tmp"
trap - EXIT

read -r -d '' payload <<'JSON' || true
{
  "key_schema": "\"string\"",
  "value_schema_id": SCHEMA_ID,
  "records": [
    {
      "key": "CUST-1000",
      "value": {
        "customer_id": "CUST-1000",
        "email": {"string": "ada@example.com"},
        "first_name": {"string": "Ada"},
        "last_name": {"string": "Lovelace"},
        "created_at": 1704067200000,
        "lifecycle_stage": {"string": "prospect"},
        "is_active": true
      }
    },
    {
      "key": "CUST-1001",
      "value": {
        "customer_id": "CUST-1001",
        "email": {"string": "grace@example.com"},
        "first_name": {"string": "Grace"},
        "last_name": {"string": "Hopper"},
        "created_at": 1704153600000,
        "lifecycle_stage": {"string": "customer"},
        "is_active": true
      }
    },
    {
      "key": "CUST-1002",
      "value": {
        "customer_id": "CUST-1002",
        "email": {"string": "katherine@example.com"},
        "first_name": {"string": "Katherine"},
        "last_name": {"string": "Johnson"},
        "created_at": 1704240000000,
        "lifecycle_stage": {"string": "prospect"},
        "is_active": false
      }
    }
  ]
}
JSON

payload=${payload//SCHEMA_ID/$schema_id}

echo "Seeding $TOPIC via $REST_PROXY_URL"
response=$(curl -s -o /tmp/customer_seed_response.json -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  --data "$payload" \
  "$REST_PROXY_URL/topics/$TOPIC")

if [[ "$response" != "200" ]]; then
  echo "Seed request failed (HTTP $response):"
  cat /tmp/customer_seed_response.json
  exit 1
fi

cat /tmp/customer_seed_response.json
rm -f /tmp/customer_seed_response.json

echo "Customer profile records published to $TOPIC"
