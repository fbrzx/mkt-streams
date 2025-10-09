# Marketing Engine Demo

This repository provisions a local, end-to-end marketing streaming stack on top of Redpanda, Confluent Schema Registry + REST Proxy, ksqlDB, Kafka UI, and local Spark Structured Streaming pipelines that materialize bronze/silver/gold Delta tables and publish a propensity score topic.

## Prerequisites

- Docker & Docker Compose plugin
- Python 3.11 for running Spark jobs (venv recommended)

## Quickstart

1. **Start the platform services**

   ```bash
   make up
   ```

   This launches Redpanda, Schema Registry, REST Proxy, ksqlDB, ksqlDB CLI container, and Kafka UI.

2. **Register Avro schemas**

   ```bash
   make schemas
   ```

   The script posts each `.avsc` under `schemas/` to the Schema Registry. It is safe to rerun at any time.

3. **Seed demo data**

   ```bash
   make seed
   ```

   Customer profiles, order events, and activation statuses are produced via the REST Proxy using the registered Avro schemas.

4. **Deploy the ksqlDB topology**

   ```bash
   make ksql
   ```

   The CLI pipes `ksql/streams.sql` into the ksqlDB server, creating the base streams, tables, and segment materialization stream.

5. **(Optional) Create a Python virtual environment for Spark**

   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   pip install -r spark/requirements.txt
   ```

6. **Run the Spark streaming jobs**

   ```bash
   make spark
   ```

   This launches the four pipelines found in `spark/`:

   - `ingest_stream.py`: consumes Kafka topics and writes Delta bronze tables.
   - `silver_customers_orders.py`: builds curated silver tables with Delta MERGEs.
   - `gold_c360.py`: aggregates a customer 360º gold view.
   - `score_and_publish.py`: computes propensity scores from gold and publishes `dom.propensity.score.v1` to Kafka.

   Each stream maintains checkpoints beneath `/tmp/delta/` and writes Delta tables under `./delta/`.

7. **Explore the data**

   - Browse topics and schemas at [Kafka UI](http://localhost:8080/).
   - Inspect ksqlDB streams/tables at [ksqlDB REST API](http://localhost:8088/).
   - Query Delta tables with the Spark SQL shell:

     ```bash
     spark-sql --packages io.delta:delta-spark_2.12:2.4.0 -e "SELECT * FROM delta.`$(pwd)/delta/gold/c360`"
     ```

8. **Shutdown**

   ```bash
   make down
   ```

   This stops all containers and removes volumes.

## Directory Structure

- `docker-compose.yml` – Service topology for the marketing engine stack.
- `schemas/` – Avro schema definitions for customer, order, activation, and segment topics.
- `scripts/` – Bash helpers for schema registration and data seeding via REST Proxy.
- `ksql/streams.sql` – ksqlDB statements for ingest, enrichment, and segment materialization.
- `spark/` – Structured Streaming jobs, requirements, and orchestration script.
- `delta/` – Local Delta Lake storage (created at runtime).

## Environment Variables

The following environment variables can override defaults when running scripts locally:

- `SCHEMA_REGISTRY_URL` – Base URL for Schema Registry (default `http://localhost:8081`).
- `REST_PROXY_URL` – Base URL for Kafka REST Proxy (default `http://localhost:8082`).
- `KAFKA_BOOTSTRAP_SERVERS` – Kafka bootstrap servers for Spark (default `localhost:9092`).
- `CUSTOMER_TOPIC`, `ORDER_TOPIC`, `ACTIVATION_TOPIC`, `PROPENSITY_TOPIC` – Topic overrides for ingestion and publishing.
- `SPARK_CHECKPOINT_ROOT` – Root directory for Spark checkpoints (default `/tmp/delta/checkpoints`).

## Notes

- Seed scripts are idempotent; rerunning them simply re-emits the same deterministic records keyed by business IDs.
- Delta checkpoints live under `/tmp/delta/*` to keep state separate from the repository.
- No external cloud dependencies are required; everything runs locally via Docker and Spark.
