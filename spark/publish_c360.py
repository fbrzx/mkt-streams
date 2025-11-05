#!/usr/bin/env python3
import time
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import checkpoint_path, gold_path, kafka_bootstrap_servers, topic_config, wait_for_delta, wait_for_topic


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("PublishC360")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    builder = configure_spark_with_delta_pip(builder)
    existing_packages = builder._options.get("spark.jars.packages")
    extras = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
    packages = ",".join(filter(None, [existing_packages, *extras]))
    builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Waiting for gold C360 table to be created by gold_c360.py...", flush=True)
    wait_for_delta(gold_path("c360"), timeout=300)  # Increased timeout to 5 minutes

    # Additional wait to ensure there's some data in the table
    print("Waiting for initial data in gold C360 table...", flush=True)
    time.sleep(10)

    gold_stream = spark.readStream.format("delta").load(gold_path("c360"))

    # Transform to match the format expected by the webapp
    # Convert timestamp to milliseconds for last_order_ts
    output = gold_stream.select(
        F.col("customer_id").cast("string").alias("key"),
        F.to_json(
            F.struct(
                F.col("customer_id"),
                F.col("email"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("lifecycle_stage"),
                F.col("is_active"),
                F.col("lifetime_value"),
                F.col("order_count"),
                (F.col("last_order_at") * 1000).cast("bigint").alias("last_order_ts"),
                F.col("delivered_messages"),
                F.col("failed_messages"),
            )
        ).alias("value"),
    )

    topic = topic_config("dom.customer.gold.v1", "GOLD_C360_TOPIC")

    wait_for_topic(topic)

    query = (
        output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers())
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_path("gold", "publish_c360"))
        .outputMode("update")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
