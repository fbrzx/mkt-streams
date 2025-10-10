# Marketing Streams Demo Script

Use this walkthrough to prove how events flow from Kafka → ksqlDB → Spark (bronze → silver → gold) → outbound scoring. Commands assume you are at the repository root.

---

## 0. Prepare the stack

```bash
make up
make schemas
make topics          # optional if topics already exist
make ksql
make spark           # leave this running in a separate terminal
```

Open Kafka UI at <http://localhost:8080>. For ksqlDB introspection:

```bash
docker compose exec ksqldb ksql
```

Inside the prompt:

```sql
SHOW STREAMS;
SHOW TABLES;
SELECT * FROM T_CUSTOMER_ORDER_SUMMARY EMIT CHANGES LIMIT 5;
```

Optionally seed the baseline dataset:

```bash
make seed
```

---

## 1. Publish a new order (Kafka event)

Send a fresh order for `CUST-1000` via the REST Proxy:

```bash
curl -s -X POST http://localhost:8082/topics/dom.order.placed.v1 \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  -d '{
    "key_schema": "\"string\"",
    "value_schema": {
      "type": "record",
      "name": "OrderPlaced",
      "namespace": "dom.order",
      "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "order_total", "type": "double"},
        {"name": "items", "type": {"type": "array","items": {
          "name": "OrderItem","type": "record","fields": [
            {"name": "sku","type":"string"},
            {"name": "quantity","type":"int"},
            {"name": "price","type":"double"}
          ]}}},
        {"name": "currency", "type": "string"},
        {"name": "channel", "type": "string"},
        {"name": "placed_at", "type": "long"}
      ]
    },
    "records": [
      {
        "key": "ORD-9004",
        "value": {
          "order_id": "ORD-9004",
          "customer_id": "CUST-1000",
          "order_total": 180.25,
          "items": [
            {"sku": "SKU-OMEGA", "quantity": 1, "price": 120.00},
            {"sku": "SKU-SIGMA", "quantity": 2, "price": 30.125}
          ],
          "currency": "USD",
          "channel": "web",
          "placed_at": 1705272000000
        }
      }
    ]
  }'
```

---

## 2. Bronze listener (Spark `ingest_stream.py`)

Check the bronze Delta table for the new order:

```bash
docker compose run --rm spark \
  spark-sql -e "SELECT order_id, order_total, ingested_at FROM delta.'./delta/bronze/order_placed' WHERE order_id = 'ORD-9004'"
```

Result shows the raw record plus the ingestion timestamp.

---

## 3. Silver listener (Spark `silver_customers_orders.py`)

Verify the curated silver table captured it:

```bash
docker compose run --rm spark \
  spark-sql -e "SELECT order_id, customer_id, order_total FROM delta.'./delta/silver/customers_orders' WHERE order_id = 'ORD-9004'"
```

You should see the order with customer context.

---

## 4. Gold listener (Spark `gold_c360.py`)

Inspect the customer 360 table to confirm lifetime metrics updated:

```bash
docker compose run --rm spark \
  spark-sql -e "SELECT customer_id, lifetime_value, order_count, last_order_ts FROM delta.'./delta/gold/c360' WHERE customer_id = 'CUST-1000'"
```

The new order increases `lifetime_value` and `order_count` and refreshes `last_order_ts`.

---

## 5. ksqlDB verification

Within the ksql prompt:

```sql
SELECT customer_id, lifetime_value, order_count
FROM T_CUSTOMER_ORDER_SUMMARY
WHERE customer_id = 'CUST-1000'
EMIT CHANGES LIMIT 1;
```

Values should match the gold Delta table, linking Kafka/ksql to the Spark outputs.

---

## 6. Propensity scoring listener (`score_and_publish.py`)

Check the outbound topic for an updated score:

```bash
docker compose exec redpanda rpk topic consume dom.propensity.score.v1 --num 1
```

Look for a message where `customer_id = CUST-1000` and the score reflects the latest order.

---

## 7. Show Delta checkpoints (optional visual)

```bash
ls delta/bronze
ls delta/silver
ls delta/gold
```

Point out the `_delta_log` directories—each streaming job writes transaction logs as it processes events, proving the listeners are active.
