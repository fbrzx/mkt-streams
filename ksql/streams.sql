-- ksqlDB topology for marketing engine demo
SET 'auto.offset.reset' = 'earliest';

DROP STREAM IF EXISTS S_SEGMENT_MATERIALIZED DELETE TOPIC;
DROP STREAM IF EXISTS S_SEGMENT_SOURCE;
DROP TABLE IF EXISTS T_SEGMENT_SOURCE DELETE TOPIC;
DROP TABLE IF EXISTS T_CUSTOMER_CONSOLIDATED DELETE TOPIC;
DROP TABLE IF EXISTS T_CUSTOMER_ORDER_SUMMARY DELETE TOPIC;
DROP STREAM IF EXISTS S_CUSTOMER_ORDER_ENRICHED DELETE TOPIC;
DROP STREAM IF EXISTS S_ACTIVATION_STATUS;
DROP STREAM IF EXISTS S_ORDER_PLACED;
DROP TABLE IF EXISTS T_CUSTOMER_PROFILE;
DROP STREAM IF EXISTS S_CUSTOMER_PROFILE_SOURCE;

CREATE STREAM S_CUSTOMER_PROFILE_SOURCE (
  customer_id STRING KEY,
  email STRING,
  first_name STRING,
  last_name STRING,
  created_at BIGINT,
  lifecycle_stage STRING,
  is_active BOOLEAN
) WITH (
  KAFKA_TOPIC='dom.customer.profile.upsert.v1',
  VALUE_FORMAT='AVRO'
);

CREATE TABLE T_CUSTOMER_PROFILE AS
  SELECT
    customer_id,
    LATEST_BY_OFFSET(email) AS email,
    LATEST_BY_OFFSET(first_name) AS first_name,
    LATEST_BY_OFFSET(last_name) AS last_name,
    LATEST_BY_OFFSET(created_at) AS created_at,
    LATEST_BY_OFFSET(lifecycle_stage) AS lifecycle_stage,
    LATEST_BY_OFFSET(is_active) AS is_active
  FROM S_CUSTOMER_PROFILE_SOURCE
  GROUP BY customer_id
  EMIT CHANGES;

CREATE STREAM IF NOT EXISTS S_ORDER_PLACED (
  order_id STRING KEY,
  customer_id STRING,
  order_total DOUBLE,
  items ARRAY<STRUCT<sku STRING, quantity INTEGER, price DOUBLE>>,
  currency STRING,
  channel STRING,
  placed_at BIGINT
) WITH (
  KAFKA_TOPIC='dom.order.placed.v1',
  VALUE_FORMAT='AVRO'
);

CREATE STREAM IF NOT EXISTS S_ACTIVATION_STATUS (
  activation_id STRING KEY,
  order_id STRING,
  customer_id STRING,
  channel STRING,
  status STRING,
  delivered_at BIGINT,
  attempts INTEGER
) WITH (
  KAFKA_TOPIC='dom.activation.delivery.status.v1',
  VALUE_FORMAT='AVRO'
);

CREATE STREAM S_CUSTOMER_ORDER_ENRICHED WITH (
  KAFKA_TOPIC='dom.customer.order.enriched.v1',
  VALUE_FORMAT='JSON'
) AS
  SELECT
    o.order_id AS order_id,
    o.customer_id AS customer_id,
    o.order_total AS order_total,
    o.channel AS order_channel,
    o.placed_at AS order_placed_at,
    p.email AS customer_email,
    p.lifecycle_stage AS lifecycle_stage
  FROM S_ORDER_PLACED o
  LEFT JOIN T_CUSTOMER_PROFILE p
    ON o.customer_id = p.customer_id
  EMIT CHANGES;

CREATE TABLE T_CUSTOMER_ORDER_SUMMARY AS
  SELECT
    customer_id,
    SUM(order_total) AS lifetime_value,
    COUNT(*) AS order_count,
    MAX(order_placed_at) AS last_order_ts
  FROM S_CUSTOMER_ORDER_ENRICHED
  GROUP BY customer_id
  EMIT CHANGES;

CREATE TABLE T_CUSTOMER_CONSOLIDATED WITH (
  KAFKA_TOPIC='dom.customer.consolidated.v1',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
) AS
  SELECT
    s.customer_id AS customer_id,
    s.order_count AS order_count,
    s.lifetime_value AS lifetime_value,
    s.last_order_ts AS last_order_ts,
    p.first_name AS first_name,
    p.last_name AS last_name,
    p.email AS email,
    p.lifecycle_stage AS lifecycle_stage,
    p.is_active AS is_active
  FROM T_CUSTOMER_ORDER_SUMMARY s
  LEFT JOIN T_CUSTOMER_PROFILE p
    ON s.customer_id = p.customer_id
  EMIT CHANGES;

CREATE TABLE T_SEGMENT_SOURCE WITH (
  KAFKA_TOPIC='dom.segment.source.v1',
  VALUE_FORMAT='JSON'
) AS
  SELECT
    s.customer_id AS customer_id,
    s.lifetime_value AS lifetime_value,
    s.order_count AS order_count,
    s.last_order_ts AS last_order_ts,
    p.lifecycle_stage AS lifecycle_stage
  FROM T_CUSTOMER_ORDER_SUMMARY s
  LEFT JOIN T_CUSTOMER_PROFILE p
    ON s.customer_id = p.customer_id
  EMIT CHANGES;

CREATE STREAM IF NOT EXISTS S_SEGMENT_SOURCE (
  customer_id STRING KEY,
  lifetime_value DOUBLE,
  order_count BIGINT,
  last_order_ts BIGINT,
  lifecycle_stage STRING
) WITH (
  KAFKA_TOPIC='dom.segment.source.v1',
  VALUE_FORMAT='JSON'
);

CREATE STREAM IF NOT EXISTS S_SEGMENT_MATERIALIZED WITH (
  KAFKA_TOPIC='dom.segment.materialized.v1',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='dom.segment.SegmentMaterialized',
  PARTITIONS=1
) AS
  SELECT
    customer_id,
    CASE
      WHEN lifetime_value >= 200 THEN 'vip'
      WHEN order_count >= 2 THEN 'growth'
      WHEN lifecycle_stage IS NULL THEN 'unknown'
      ELSE lifecycle_stage
    END AS segment,
    CAST(ROUND(lifetime_value / 200.0 + order_count * 0.1, 2) AS DOUBLE) AS score,
    UNIX_TIMESTAMP()*1000 AS materialized_at
  FROM S_SEGMENT_SOURCE
  EMIT CHANGES;
