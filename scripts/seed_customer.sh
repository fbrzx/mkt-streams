#!/usr/bin/env bash
set -euo pipefail

REST_PROXY_URL=${REST_PROXY_URL:-http://localhost:8082}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
TOPIC=${CUSTOMER_TOPIC:-dom.customer.profile.upsert.v1}
SUBJECT=${CUSTOMER_SUBJECT:-dom.customer.profile.upsert.v1-value}

echo "Fetching schema id for $SUBJECT"
schema_id=$(curl -s "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions/latest" | python3 - <<'PY'
import json, sys
payload = json.load(sys.stdin)
print(payload['id'])
PY
)

if [[ -z "$schema_id" ]]; then
  echo "Unable to resolve schema id for $SUBJECT" >&2
  exit 1
fi

read -r -d '' payload <<'JSON'
{
  "key_schema": "\"string\"",
  "value_schema_id": SCHEMA_ID,
  "records": [
    {
      "key": "CUST-1000",
      "value": {
        "customer_id": "CUST-1000",
        "email": "ada@example.com",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "created_at": 1704067200000,
        "lifecycle_stage": "prospect",
        "is_active": true
      }
    },
    {
      "key": "CUST-1001",
      "value": {
        "customer_id": "CUST-1001",
        "email": "grace@example.com",
        "first_name": "Grace",
        "last_name": "Hopper",
        "created_at": 1704153600000,
        "lifecycle_stage": "customer",
        "is_active": true
      }
    },
    {
      "key": "CUST-1002",
      "value": {
        "customer_id": "CUST-1002",
        "email": "katherine@example.com",
        "first_name": "Katherine",
        "last_name": "Johnson",
        "created_at": 1704240000000,
        "lifecycle_stage": "prospect",
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
