"""Real Kafka + Spark + Delta: all four reliability scenarios, no mocked engines."""

import json
import random
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import functions as F

from producer.events import lifecycle
from streaming.pipeline import initialize, start_ingestion, start_metrics

pytestmark = pytest.mark.integration


def event(minute, kind="delivered"):
    e = lifecycle(random.Random(42), datetime(2025, 1, 1, tzinfo=timezone.utc))[0]
    return dict(
        e, event_type=kind, status=kind, event_timestamp=f"2025-01-01T00:{minute:02d}:00+00:00"
    )


def drain(query):
    assert query.awaitTermination(180), "Streaming query did not finish within 180 seconds"
    return query.recentProgress


def test_kafka_delta_recovery_and_watermark(spark, tmp_path):
    topic = "test-order-events-" + uuid4().hex[:12]
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
    admin.create_topics([NewTopic(topic, 3, 1)])
    producer = KafkaProducer(bootstrap_servers="localhost:9092", acks="all")
    root, checkpoints = str(tmp_path / "tables"), str(tmp_path / "checkpoints")
    initialize(spark, root)

    def send(e):
        producer.send(topic, key=e["order_id"].encode(), value=json.dumps(e).encode()).get(15)

    def ingest():
        return drain(start_ingestion(spark, root, checkpoints, topic, available_now=True))

    def metrics():
        return drain(start_metrics(spark, root, checkpoints, available_now=True))

    def table(name):
        return spark.read.format("delta").load(f"{root}/{name}")

    try:
        first = event(1)
        send(first)
        send(first)
        producer.send(topic, key=b"bad", value=b"{broken").get(15)
        ingest()
        metrics()
        assert table("clean_events").count() == 1
        assert table("received_events").count() == 3
        assert table("rejected_events").count() == 1

        # Ingestion is stopped. Kafka still accepts records; tables stay unchanged.
        send(first)
        send(event(8, "order_created"))
        assert table("received_events").count() == 3
        progress = ingest()
        assert sum(p["numInputRows"] for p in progress) > 0
        assert table("received_events").count() == 5
        assert table("clean_events").count() == 2
        metrics()

        # Out-of-order but still inside the watermark allowance.
        send(event(2))
        ingest()
        metrics()
        send(event(20, "order_created"))
        ingest()
        metrics()
        result = (
            table("delivery_windows")
            .filter("window_start = timestamp '2025-01-01 00:00:00'")
            .first()
        )
        assert result.delivered_orders == 2
        assert result.revenue_cents == 2 * first["order_total"]

        # Older than the established watermark: retained clean, excluded from finalized window.
        send(event(3))
        ingest()
        progress = metrics()
        assert (
            sum(
                op.get("numRowsDroppedByWatermark", 0)
                for p in progress
                for op in p.get("stateOperators", [])
            )
            >= 1
        )
        assert table("clean_events").filter(F.col("event_type") == "delivered").count() == 3
        result = (
            table("delivery_windows")
            .filter("window_start = timestamp '2025-01-01 00:00:00'")
            .first()
        )
        assert result.delivered_orders == 2
        received = table("received_events").count()
        ingest()
        metrics()
        assert table("received_events").count() == received == 8
        assert table("clean_events").count() == 5
    finally:
        for query in spark.streams.active:
            query.stop()
        producer.close()
        admin.delete_topics([topic])
        admin.close()
