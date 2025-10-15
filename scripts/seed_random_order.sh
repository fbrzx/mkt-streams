#!/usr/bin/env bash
set -euo pipefail

REST_PROXY_URL=${REST_PROXY_URL:-http://localhost:8082}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
TOPIC=${ORDER_TOPIC:-dom.order.placed.v1}
SUBJECT=${ORDER_SUBJECT:-dom.order.placed.v1-value}

echo "Fetching schema id for $SUBJECT"
schema_tmp=$(mktemp)
metadata_tmp=$(mktemp)
response_tmp=$(mktemp)
cleanup() {
  rm -f "$schema_tmp" "$metadata_tmp" "$response_tmp"
}
trap cleanup EXIT

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

payload=$(SCHEMA_ID="$schema_id" METADATA_PATH="$metadata_tmp" python3 - <<'PY'
import json
import os
import random
import sys
import time

rng = random.SystemRandom()
schema_id = int(os.environ["SCHEMA_ID"])
metadata_path = os.environ["METADATA_PATH"]

order_id = f"ORD-{rng.randint(10_000, 99_999)}"
existing_customers = ["CUST-1000", "CUST-1001", "CUST-1002"]

if rng.random() < 0.5:
    customer_id = rng.choice(existing_customers)
    customer_origin = "existing"
else:
    customer_id = f"CUST-{rng.randint(10_000, 99_999)}"
    customer_origin = "new"

catalog = [
    ("SKU-ALPHA", 79.99),
    ("SKU-BETA", 25.00),
    ("SKU-GAMMA", 59.50),
    ("SKU-DELTA", 30.00),
    ("SKU-OMEGA", 120.00),
    ("SKU-PHI", 19.00)
]
item_count = rng.randint(1, min(3, len(catalog)))
items = []
for sku, price in rng.sample(catalog, k=item_count):
    quantity = rng.randint(1, 3)
    items.append({
        "sku": sku,
        "quantity": quantity,
        "price": round(price, 2)
    })

order_total = round(sum(item["quantity"] * item["price"] for item in items), 2)
channel = rng.choice(["web", "store", "mobile"])
placed_at = int(time.time() * 1000)

payload = {
    "key_schema": "\"string\"",
    "value_schema_id": schema_id,
    "records": [
        {
            "key": order_id,
            "value": {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_total": order_total,
                "items": items,
                "currency": "USD",
                "channel": {"string": channel},
                "placed_at": placed_at
            }
        }
    ]
}

with open(metadata_path, "w", encoding="utf-8") as fh:
    json.dump(
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "customer_origin": customer_origin,
            "order_total": order_total,
            "channel": channel,
            "placed_at": placed_at
        },
        fh
    )

print(json.dumps(payload))
PY
)

echo "Seeding $TOPIC via $REST_PROXY_URL"
response_code=$(curl -s -o "$response_tmp" -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  --data "$payload" \
  "$REST_PROXY_URL/topics/$TOPIC")

if [[ "$response_code" != "200" ]]; then
  echo "Seed request failed (HTTP $response_code):"
  cat "$response_tmp"
  exit 1
fi

cat "$response_tmp"

IFS='|' read -r order_id customer_id customer_origin order_total channel placed_at < <(python3 - "$metadata_tmp" <<'PY'
import json, sys, pathlib
with pathlib.Path(sys.argv[1]).open() as fh:
    meta = json.load(fh)
print(
    "|".join(
        str(meta[field]) for field in (
            "order_id",
            "customer_id",
            "customer_origin",
            "order_total",
            "channel",
            "placed_at"
        )
    )
)
PY
)

echo "Random order $order_id for $customer_origin customer $customer_id published to $TOPIC"
echo "Details: total=$order_total channel=$channel placed_at_ms=$placed_at"
