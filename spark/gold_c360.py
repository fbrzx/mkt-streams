#!/usr/bin/env python3
import os
import threading
import time

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import checkpoint_path, gold_path, silver_path, wait_for_delta


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("GoldC360")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def upsert(df, target_path: str, key_columns):
    spark = df.sparkSession
    if df.rdd.isEmpty():
        return
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


def process_batch(trigger_batch, batch_id: int, target_path: str):
    """Process a batch triggered by either orders or activations changes."""
    if trigger_batch.rdd.isEmpty():
        return

    spark = trigger_batch.sparkSession

    # Always read the latest full state from all tables
    customers_df = spark.read.format("delta").load(silver_path("customers"))
    orders_df = spark.read.format("delta").load(silver_path("orders"))

    # Check if activations table exists
    from delta.tables import DeltaTable
    activations_path = silver_path("activations")
    has_activations = DeltaTable.isDeltaTable(spark, activations_path)

    # Compute order metrics from full orders table
    order_metrics = (
        orders_df.groupBy("customer_id")
        .agg(
            F.sum("order_total").alias("lifetime_value"),
            F.countDistinct("order_id").alias("order_count"),
            F.max("placed_at").alias("last_order_at"),
        )
    )

    # Build C360 with or without activation metrics
    if has_activations:
        activations_df = spark.read.format("delta").load(activations_path)
        activation_metrics = (
            activations_df.groupBy("customer_id")
            .agg(
                F.count(F.when(F.col("status") == "delivered", True)).alias("delivered_messages"),
                F.count(F.when(F.col("status") != "delivered", True)).alias("failed_messages"),
            )
        )

        c360 = (
            order_metrics.alias("o")
            .join(customers_df.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
            .join(activation_metrics.alias("a"), F.col("o.customer_id") == F.col("a.customer_id"), "left")
            .select(
                F.col("o.customer_id").alias("customer_id"),
                F.col("c.email"),
                F.col("c.first_name"),
                F.col("c.last_name"),
                F.col("c.lifecycle_stage"),
                F.col("c.is_active"),
                F.col("o.lifetime_value"),
                F.col("o.order_count"),
                F.col("o.last_order_at"),
                F.coalesce(F.col("a.delivered_messages"), F.lit(0)).alias("delivered_messages"),
                F.coalesce(F.col("a.failed_messages"), F.lit(0)).alias("failed_messages"),
                F.current_timestamp().alias("updated_at"),
            )
        )
    else:
        # No activations yet, set engagement metrics to 0
        c360 = (
            order_metrics.alias("o")
            .join(customers_df.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
            .select(
                F.col("o.customer_id").alias("customer_id"),
                F.col("c.email"),
                F.col("c.first_name"),
                F.col("c.last_name"),
                F.col("c.lifecycle_stage"),
                F.col("c.is_active"),
                F.col("o.lifetime_value"),
                F.col("o.order_count"),
                F.col("o.last_order_at"),
                F.lit(0).alias("delivered_messages"),
                F.lit(0).alias("failed_messages"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    upsert(c360, target_path, ["customer_id"])


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    wait_for_delta(silver_path("orders"))
    wait_for_delta(silver_path("customers"))

    # Check if activations table exists
    from delta.tables import DeltaTable
    activations_path = silver_path("activations")
    has_activations_table = DeltaTable.isDeltaTable(spark, activations_path)

    # Read streams
    orders_stream = spark.readStream.format("delta").load(silver_path("orders"))

    target_path = gold_path("c360")
    os.makedirs(target_path, exist_ok=True)

    # Start orders stream
    orders_query = (
        orders_stream.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_path("gold", "c360_orders"))
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, target_path))
        .start()
    )

    activation_query_holder = {"query": None}

    def start_activation_trigger():
        """Spin up activation-triggered stream when the Delta table becomes available."""
        while True:
            try:
                wait_for_delta(activations_path, timeout=120)
            except TimeoutError:
                print("Still waiting for silver activations table...", flush=True)
                continue
            try:
                print("Starting activation stream trigger for C360", flush=True)
                activations_stream = spark.readStream.format("delta").load(activations_path)
                activation_query = (
                    activations_stream.writeStream.outputMode("update")
                    .option("checkpointLocation", checkpoint_path("gold", "c360_activations"))
                    .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, target_path))
                    .start()
                )
                activation_query_holder["query"] = activation_query
                activation_query.awaitTermination()
                if activation_query.exception() is not None:
                    print(
                        "Activation stream terminated with error; restarting in 10 seconds.",
                        flush=True,
                    )
                    time.sleep(10)
                    continue
                break
            except Exception as exc:  # noqa: BLE001 - best-effort retry with logging
                print(f"Activation stream failed to start ({exc}); retrying in 10 seconds.", flush=True)
                time.sleep(10)

    if has_activations_table:
        print("Activations table found; starting activation-triggered stream.", flush=True)
    else:
        print("Activations table not found yet; will monitor and start activation trigger when available.", flush=True)

    activation_trigger_thread = threading.Thread(
        target=start_activation_trigger, name="ActivationTrigger", daemon=True
    )
    activation_trigger_thread.start()

    orders_query.awaitTermination()


if __name__ == "__main__":
    main()
