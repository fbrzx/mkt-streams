#!/usr/bin/env python3
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


EXTRA_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.commons:commons-pool2:2.11.1",
]


def main() -> None:
    builder = (
        SparkSession.builder.appName("SparkBootstrap")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    builder = configure_spark_with_delta_pip(builder)

    existing_packages = builder._options.get("spark.jars.packages")
    package_list = ",".join(filter(None, [existing_packages, *EXTRA_PACKAGES]))
    builder = builder.config("spark.jars.packages", package_list)

    spark = builder.getOrCreate()
    try:
        # Force the session to initialize and download configured artifacts.
        spark.range(1).collect()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
