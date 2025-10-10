#!/usr/bin/env python3
import io
import json
import os
from typing import Dict

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common import (
    bronze_path,
    checkpoint_path,
    kafka_bootstrap_servers,
    load_avro_schema,
    topic_config,
    wait_for_delta,
    wait_for_topic,
)


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("BronzeIngest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    builder = configure_spark_with_delta_pip(builder)

    existing_packages = builder._options.get("spark.jars.packages")
    extras = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
    packages = ",".join(filter(None, [existing_packages, *extras]))
    builder = builder.config("spark.jars.packages", packages)

    return builder.getOrCreate()


def avro_struct(schema: Dict) -> StructType:
    fields = []
    for field in schema["fields"]:
        name = field["name"]
        field_type = field["type"]
        if isinstance(field_type, list):
            non_null = [t for t in field_type if t != "null"]
            field_type = non_null[0] if len(non_null) == 1 else non_null
        if isinstance(field_type, dict) and field_type.get("logicalType") == "timestamp-millis":
            field_type = "long"
        fields.append(StructField(name, map_avro_type(field_type), True))
    return StructType(fields)


def map_avro_type(avro_type):
    if isinstance(avro_type, dict):
        if avro_type.get("type") == "array":
            return ArrayType(map_avro_type(avro_type["items"]))
        if avro_type.get("type") == "record":
            return avro_struct(avro_type)
    if avro_type == "string":
        return StringType()
    if avro_type in ("double", "float"):
        return DoubleType()
    if avro_type == "int":
        return IntegerType()
    if avro_type == "long":
        return LongType()
    if avro_type == "boolean":
        return BooleanType()
    raise ValueError(f"Unsupported Avro type: {avro_type}")


def decode_with_fastavro(schema: Dict):
    from fastavro import schemaless_reader

    def decode(avro_bytes: bytes):
        if avro_bytes is None:
            return None
        with io.BytesIO(avro_bytes[5:]) as buffer:
            record = schemaless_reader(buffer, schema)
        return json.dumps(record)

    return decode


def start_stream(spark: SparkSession, topic: str, schema_name: str, output_dir: str, checkpoint: str):
    schema_dict = load_avro_schema(schema_name)
    struct_schema = avro_struct(schema_dict)
    decode = decode_with_fastavro(schema_dict)

    spark.udf.register(f"decode_{schema_name}", decode, StringType())

    wait_for_topic(topic)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers())
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    json_df = kafka_df.selectExpr("CAST(key AS STRING) AS message_key", "value", "timestamp")
    parsed_df = json_df.select(
        F.col("message_key"),
        F.from_json(F.call_udf(f"decode_{schema_name}", F.col("value")), struct_schema).alias("payload"),
        F.col("timestamp").alias("ingested_at"),
    )

    flattened_df = parsed_df.select("message_key", "payload.*", "ingested_at")

    query = (
        flattened_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start(output_dir)
    )

    return query


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    streams = [
        (
            topic_config("dom.customer.profile.upsert.v1", "CUSTOMER_TOPIC"),
            "dom.customer.profile.upsert.v1",
            bronze_path("customer_profile"),
            checkpoint_path("bronze", "customer_profile"),
        ),
        (
            topic_config("dom.order.placed.v1", "ORDER_TOPIC"),
            "dom.order.placed.v1",
            bronze_path("order_placed"),
            checkpoint_path("bronze", "order_placed"),
        ),
        (
            topic_config("dom.activation.delivery.status.v1", "ACTIVATION_TOPIC"),
            "dom.activation.delivery.status.v1",
            bronze_path("activation_status"),
            checkpoint_path("bronze", "activation_status"),
        ),
    ]

    queries = []
    for topic, schema_name, output_dir, checkpoint in streams:
        os.makedirs(output_dir, exist_ok=True)
        query = start_stream(spark, topic, schema_name, output_dir, checkpoint)
        queries.append(query)
        print(f"Started bronze ingest for {topic} -> {output_dir}")

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
