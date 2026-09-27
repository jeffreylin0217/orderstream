"""Two checkpointed queries: Kafka ingestion, then finalized delivery windows."""

import argparse
import hashlib
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from streaming.session import create_spark
from streaming.transforms import parse_events, delivery_windows


def paths():
    return (
        os.getenv("TABLE_ROOT", str(Path("data/tables").resolve())).rstrip("/"),
        os.getenv("CHECKPOINT_ROOT", str(Path("data/checkpoints").resolve())).rstrip("/"),
    )


def kafka_source(spark, topic=None):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", topic or os.getenv("KAFKA_TOPIC", "order-events"))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000"))
        .load()
    )


def clean_columns(parsed):
    return parsed.drop("raw_value", "kafka_key", "is_valid", "rejection_reason")


def initialize(spark, root):
    empty_raw = spark.createDataFrame(
        [],
        "value binary, key binary, topic string, partition int, offset long, timestamp timestamp",
    )
    parsed = parse_events(empty_raw)
    frames = {
        "received_events": parsed,
        "rejected_events": parsed,
        "clean_events": clean_columns(parsed),
    }
    for name, frame in frames.items():
        if not DeltaTable.isDeltaTable(spark, f"{root}/{name}"):
            frame.write.format("delta").mode("errorifexists").save(f"{root}/{name}")


def write_batch(batch, batch_id, root, app_id):
    batch.persist()
    try:
        # A retry after a later write fails reuses the same transaction IDs.
        for name, frame in (
            ("received_events", batch),
            ("rejected_events", batch.filter(~F.col("is_valid"))),
        ):
            (
                frame.write.format("delta")
                .mode("append")
                .option("txnAppId", app_id + ":" + name)
                .option("txnVersion", batch_id)
                .save(f"{root}/{name}")
            )
        valid = clean_columns(batch.filter("is_valid")).dropDuplicates(["event_id"])
        (
            DeltaTable.forPath(batch.sparkSession, f"{root}/clean_events")
            .alias("target")
            .merge(valid.alias("source"), "target.event_id = source.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    finally:
        batch.unpersist()


def start_ingestion(spark, root, checkpoints, topic=None, available_now=False):
    app_id = hashlib.sha256(checkpoints.encode()).hexdigest()
    writer = (
        parse_events(kafka_source(spark, topic))
        .writeStream.queryName("order_ingestion")
        .option("checkpointLocation", f"{checkpoints}/ingestion")
        .foreachBatch(lambda batch, epoch: write_batch(batch, epoch, root, app_id))
    )
    return writer.trigger(
        **({"availableNow": True} if available_now else {"processingTime": "5 seconds"})
    ).start()


def start_metrics(spark, root, checkpoints, available_now=False):
    events = (
        spark.readStream.format("delta")
        .option("withEventTimeOrder", "true")
        .load(f"{root}/clean_events")
    )
    writer = (
        delivery_windows(events)
        .writeStream.format("delta")
        .queryName("delivery_windows")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoints}/metrics")
    )
    return writer.trigger(
        **({"availableNow": True} if available_now else {"processingTime": "5 seconds"})
    ).start(f"{root}/delivery_windows")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--available-now", action="store_true", help="Drain current backlog and exit"
    )
    args = parser.parse_args()
    spark = create_spark()
    root, checkpoints = paths()
    initialize(spark, root)
    ingestion = start_ingestion(spark, root, checkpoints, available_now=args.available_now)
    metrics = None
    try:
        if args.available_now:
            ingestion.awaitTermination()
            metrics = start_metrics(spark, root, checkpoints, available_now=True)
            metrics.awaitTermination()
        else:
            metrics = start_metrics(spark, root, checkpoints)
            spark.streams.awaitAnyTermination()
    finally:
        ingestion.stop()
        if metrics:
            metrics.stop()
        spark.stop()


if __name__ == "__main__":
    main()
