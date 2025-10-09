#!/usr/bin/env python3
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import checkpoint_path, gold_path, silver_path


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("GoldC360")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


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


def process_batch(orders_batch, batch_id: int, target_path: str):
    if orders_batch.rdd.isEmpty():
        return

    spark = orders_batch.sparkSession
    customers_df = spark.read.format("delta").load(silver_path("customers"))
    activations_df = spark.read.format("delta").load(silver_path("activations"))

    order_metrics = (
        orders_batch.groupBy("customer_id")
        .agg(
            F.sum("order_total").alias("lifetime_value"),
            F.countDistinct("order_id").alias("order_count"),
            F.max("placed_at").alias("last_order_at"),
        )
    )

    activation_metrics = (
        activations_df.groupBy("customer_id")
        .agg(
            F.count(F.when(F.col("status") == "DELIVERED", True)).alias("delivered_messages"),
            F.count(F.when(F.col("status") != "DELIVERED", True)).alias("failed_messages"),
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

    upsert(c360, target_path, ["customer_id"])


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    orders_stream = spark.readStream.format("delta").load(silver_path("orders"))

    target_path = gold_path("c360")
    os.makedirs(target_path, exist_ok=True)

    query = (
        orders_stream.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_path("gold", "c360"))
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, target_path))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
