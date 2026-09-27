"""Explicit Spark/Delta configuration; local filesystem or single-writer S3."""

import os
from pathlib import Path
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark(name="orderstream"):
    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"]
    builder = (
        SparkSession.builder.appName(name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "3"))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .config("spark.jars.ivy", str(Path(".runtime/ivy").resolve()))
        .config("spark.ui.enabled", "false")
    )
    if os.getenv("TABLE_ROOT", "").startswith("s3a://"):
        packages += ["org.apache.hadoop:hadoop-aws:3.3.4"]
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        if endpoint := os.getenv("S3_ENDPOINT"):
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.connection.ssl.enabled",
                    str(endpoint.startswith("https://")).lower(),
                )
            )
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
