#!/usr/bin/env python3
import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import bronze_path, checkpoint_path, silver_path, wait_for_delta


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("SilverCustomersOrders")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def upsert_batch(df, batch_id: int, target_path: str, key_columns):
    if df.rdd.isEmpty():
        return
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, target_path):
        df.write.format("delta").mode("overwrite").save(target_path)
    else:
        target = DeltaTable.forPath(spark, target_path)
        condition = " AND ".join([f"t.{col} = s.{col}" for col in key_columns])
        (
            target.alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def start_customer_stream(spark: SparkSession):
    wait_for_delta(bronze_path("customer_profile"))
    source = spark.readStream.format("delta").load(bronze_path("customer_profile"))
    curated = (
        source.select(
            F.col("message_key").alias("customer_id"),
            F.col("email"),
            F.col("first_name"),
            F.col("last_name"),
            F.to_timestamp(F.col("created_at") / F.lit(1000)).alias("created_at"),
            F.col("lifecycle_stage"),
            F.col("is_active"),
            F.col("ingested_at"),
        )
    )

    target_path = silver_path("customers")
    os.makedirs(target_path, exist_ok=True)

    return (
        curated.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_path("silver", "customers"))
        .foreachBatch(lambda df, batch_id: upsert_batch(df, batch_id, target_path, ["customer_id"]))
        .start()
    )


def start_order_stream(spark: SparkSession):
    wait_for_delta(bronze_path("order_placed"))
    source = spark.readStream.format("delta").load(bronze_path("order_placed"))
    curated = (
        source.select(
            F.col("message_key").alias("order_id"),
            F.col("customer_id"),
            F.col("order_total"),
            F.col("currency"),
            F.col("channel"),
            F.to_timestamp(F.col("placed_at") / F.lit(1000)).alias("placed_at"),
            F.col("items"),
            F.col("ingested_at"),
        )
    )

    target_path = silver_path("orders")
    os.makedirs(target_path, exist_ok=True)

    return (
        curated.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_path("silver", "orders"))
        .foreachBatch(lambda df, batch_id: upsert_batch(df, batch_id, target_path, ["order_id"]))
        .start()
    )


def start_activation_stream(spark: SparkSession):
    wait_for_delta(bronze_path("activation_status"))
    source = spark.readStream.format("delta").load(bronze_path("activation_status"))
    curated = (
        source.select(
            F.col("message_key").alias("activation_id"),
            F.col("order_id"),
            F.col("customer_id"),
            F.col("channel"),
            F.col("status"),
            F.to_timestamp(F.col("delivered_at") / F.lit(1000)).alias("delivered_at"),
            F.col("attempts"),
            F.col("ingested_at"),
        )
    )

    target_path = silver_path("activations")
    os.makedirs(target_path, exist_ok=True)

    return (
        curated.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_path("silver", "activations"))
        .foreachBatch(lambda df, batch_id: upsert_batch(df, batch_id, target_path, ["activation_id"]))
        .start()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        start_customer_stream(spark),
        start_order_stream(spark),
        start_activation_stream(spark),
    ]

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
