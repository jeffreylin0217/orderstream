"""Parse the wire contract and calculate a single event-time window."""

from functools import reduce
from pyspark.sql import functions as F, types as T
from producer.events import EVENT_TYPES

EVENT_SCHEMA = T.StructType(
    [
        T.StructField(name, T.StringType())
        for name in (
            "event_id",
            "event_type",
            "event_timestamp",
            "order_id",
            "customer_id",
            "product_id",
            "status",
            "currency",
            "source",
        )
    ]
    + [
        T.StructField(name, T.LongType())
        for name in ("quantity", "unit_price", "order_total", "schema_version")
    ]
    + [T.StructField("_corrupt_record", T.StringType())]
)


def parse_events(raw):
    parsed = (
        raw.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("key").cast("string").alias("kafka_key"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        .select("*", "event.*")
        .drop("event")
    )
    parsed = parsed.withColumn("event_time", F.to_timestamp("event_timestamp"))
    missing = reduce(
        lambda a, b: a | b,
        [
            F.col(name).isNull() | (F.length(F.trim(F.col(name))) == 0)
            for name in ("event_id", "order_id", "customer_id", "product_id", "source")
        ],
    )
    valid = (
        F.col("_corrupt_record").isNull()
        & ~missing
        & F.col("event_type").isin(*EVENT_TYPES)
        & (F.col("status") == F.col("event_type"))
        & (F.col("currency") == "USD")
        & (F.col("schema_version") == 1)
        & F.col("quantity").between(1, 1000)
        & F.col("unit_price").between(1, 100000000)
        & (F.col("order_total") == F.col("quantity") * F.col("unit_price"))
        & F.col("event_time").isNotNull()
        & F.col("event_timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T.*(Z|[+-]\d{2}:\d{2})$")
        & (F.col("kafka_key") == F.col("order_id"))
    )
    return (
        parsed.withColumn("is_valid", F.coalesce(valid, F.lit(False)))
        .withColumn("rejection_reason", F.when(~F.col("is_valid"), F.lit("invalid_contract")))
        .drop("_corrupt_record")
    )


def delivery_windows(events):
    # All valid event types advance event time; only deliveries contribute money/counts.
    return (
        events.withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "5 minutes"))
        .agg(
            F.sum(F.when(F.col("event_type") == "delivered", 1).otherwise(0)).alias(
                "delivered_orders"
            ),
            F.sum(
                F.when(F.col("event_type") == "delivered", F.col("order_total")).otherwise(0)
            ).alias("revenue_cents"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "delivered_orders",
            "revenue_cents",
        )
    )
