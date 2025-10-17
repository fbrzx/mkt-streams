# Marketing Streams Demo Script

Use this walkthrough when narrating the end-to-end marketing pipeline. It covers the required `make` targets, optional seeders, and the ksqlDB query that ties customer profiles to their order metrics.

---

## Quickstart make flow

Spin up the entire sandbox from the repository root:

> **Prerequisite:** a Redis instance reachable at `REDIS_URL` (defaults to `redis://host.docker.internal:6379/0` inside the `webapp` container). Adjust the variable if your environment exposes Redis elsewhere.

1. `make up` – build the images, start every container (including the web UI on port 5000), register Avro schemas, create the required topics, and apply the ksqlDB topology.
2. `make spark` *(optional)* – launch the Spark Structured Streaming jobs if you want the Delta Lake layers and propensity scoring demo alongside the ksqlDB flow.

Common follow-ups:

- `make seed` – emit deterministic customer/order/activation fixtures.
- `make seed-random` – send an additional random order; new customers also trigger a profile event (`FORCE_NEW_CUSTOMER=1 make seed-random` guarantees the next run generates a brand-new profile). The web UI offers the same functionality with buttons.
- `make down` – stop the running containers while preserving their data.
- `make clear` – stop everything, remove Docker volumes, wipe local Delta/checkpoint folders, and delete the host Redpanda data directory (`~/.img-data/redpanda`) for a fresh start.

Helpful endpoints: Kafka UI at <http://localhost:8080> and the ksqlDB REST API at <http://localhost:8088>.

---

## Lightweight web app

Prefer a UI instead of bespoke `curl` commands? Head to <http://localhost:5000> after `make up` finishes. The Flask app lets you:

- View the live `T_CUSTOMER_CONSOLIDATED` table, including lifecycle stage, active status, order count, and lifetime value.
- Add a random order for any existing customer (one click per row).
- Create a brand-new customer profile and initial order in a single action.

Need to run the Flask server directly on your host instead of Docker? Install the requirements (`pip install -r webapp/requirements.txt`) and use `make webapp-local`.

By default the UI caches the consolidated snapshot in Redis for snappy refreshes. Override `REDIS_URL` (e.g. `export REDIS_URL=redis://localhost:6379/0 make webapp-local`) if you run Redis somewhere other than `host.docker.internal:6379`.
When running the UI outside Docker, also point it at your Kafka brokers (`export KAFKA_BOOTSTRAP_SERVERS=localhost:29092`).

---

## ksqlDB consolidated customer view

Run `make ksql` after pulling the latest repo to create `T_CUSTOMER_CONSOLIDATED`, a materialised table that joins summary metrics with profile attributes. Then issue a pull query (in the ksql CLI or UI) to fetch the current state:

```sql
SELECT customer_id,
       order_count,
       lifetime_value,
       last_order_ts,
       first_name,
       last_name,
       email,
       lifecycle_stage,
       is_active
FROM T_CUSTOMER_CONSOLIDATED
WHERE customer_id = 'CUST-1000';
```

Want a streaming view across every customer? Drop the `WHERE` clause and use `EMIT CHANGES LIMIT <n>` against `T_CUSTOMER_CONSOLIDATED` to watch updates flow (pick a limit that matches the number of customers you expect—otherwise the query keeps waiting).

---

## Streaming walkthrough

1. **Open the ksqlDB shell**

   ```bash
   docker compose exec ksqldb ksql
   ```

   Recommended warm-up commands inside the prompt:

   ```sql
   SHOW STREAMS;
   SHOW TABLES;
   ```

2. **Publish a fresh order**

   Either run `make seed-random` or send a handcrafted event via REST Proxy (example below). Keep the payload handy if you want a deterministic order ID during the demo.

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

3. **Show the bronze ingest stream**

   ```bash
   docker compose run --rm spark \
     spark-sql -e "SELECT order_id, order_total, ingested_at FROM delta.'./delta/bronze/order_placed' WHERE order_id = 'ORD-9004'"
   ```

   Narrate how Spark `ingest_stream.py` lands raw Kafka events in Delta and adds ingestion timestamps.

4. **Inspect the silver table**

   ```bash
   docker compose run --rm spark \
     spark-sql -e "SELECT order_id, customer_id, order_total FROM delta.'./delta/silver/customers_orders' WHERE order_id = 'ORD-9004'"
   ```

   Emphasise the curated schema and MERGE-based upserts in `silver_customers_orders.py`.

5. **Confirm the gold customer 360 metrics**

   ```bash
   docker compose run --rm spark \
     spark-sql -e "SELECT customer_id, lifetime_value, order_count, last_order_ts FROM delta.'./delta/gold/c360' WHERE customer_id = 'CUST-1000'"
   ```

   Highlight how the gold aggregation keeps lifetime value in sync with new orders.

6. **Reinforce the join in ksqlDB**

   In the ksql shell, run the consolidated query above with `WHERE customer_id = 'CUST-1000'` to match the gold table results.

7. **Show the outbound propensity score**

   ```bash
   docker compose exec redpanda rpk topic consume dom.propensity.score.v1 --num 1
   ```

   Point out the updated `score` field and timestamp generated by `score_and_publish.py`.

8. **Optional visual cues**

   ```bash
   ls delta/bronze
   ls delta/silver
   ls delta/gold
   ```

   Use the `_delta_log` directories to explain streaming checkpoints and transaction logs.

---

## Troubleshooting and reset tips

- If Schema Registry lookups fail, confirm the service is healthy (`docker compose ps`) and rerun `make schemas`.
- ksqlDB will refuse to recreate existing streams/tables—`make ksql` is idempotent, but delete resources via the ksql CLI if you need a clean slate.
- Spark jobs keep local checkpoints under `.checkpoints/` and Delta tables under `./delta/`. Remove them between demos if you want to restart from scratch.
