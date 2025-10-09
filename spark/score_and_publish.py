#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import checkpoint_path, gold_path, kafka_bootstrap_servers, topic_config


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ScoreAndPublish")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    gold_stream = spark.readStream.format("delta").load(gold_path("c360"))

    propensity = (
        gold_stream.select(
            F.col("customer_id").alias("customer_id"),
            F.col("email"),
            F.col("lifecycle_stage"),
            F.col("lifetime_value"),
            F.col("order_count"),
            F.col("delivered_messages"),
            F.col("failed_messages"),
            (
                F.col("lifetime_value") / F.lit(150.0)
                + F.col("order_count") * F.lit(0.75)
                + (F.col("delivered_messages") - F.col("failed_messages")) * F.lit(0.25)
            ).alias("raw_score"),
            F.current_timestamp().alias("scored_at"),
        )
        .withColumn("propensity", 1 / (1 + F.exp(-F.col("raw_score"))))
        .drop("raw_score")
    )

    output = propensity.select(
        F.col("customer_id").cast("string").alias("key"),
        F.to_json(
            F.struct(
                F.col("customer_id"),
                F.col("email"),
                F.col("lifecycle_stage"),
                F.round(F.col("propensity"), 4).alias("propensity"),
                F.col("scored_at"),
            )
        ).alias("value"),
    )

    topic = topic_config("dom.propensity.score.v1", "PROPENSITY_TOPIC")

    query = (
        output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers())
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_path("gold", "propensity"))
        .outputMode("update")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
