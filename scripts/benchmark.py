"""Reproducible finite Kafka backlog benchmark, with verified Delta output counts."""

import argparse
import json
import os
import platform
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import functions as F

from producer.events import lifecycle
from streaming.pipeline import initialize, start_ingestion, start_metrics
from streaming.session import create_spark


def wait(query):
    if not query.awaitTermination(900):
        query.stop()
        raise TimeoutError("Benchmark query exceeded 15 minutes")
    return query.recentProgress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="benchmarks/local.json")
    args = parser.parse_args()
    if args.orders <= 0:
        parser.error("--orders must be positive")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "benchmark-order-events-" + uuid4().hex[:12]
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(topic, 3, 1)])
    producer = KafkaProducer(
        bootstrap_servers=bootstrap, acks="all", enable_idempotence=True, linger_ms=10
    )
    spark = create_spark("orderstream-benchmark")
    base = Path("data/benchmarks") / topic
    root, checkpoints = str(base.resolve() / "tables"), str(base.resolve() / "checkpoints")
    initialize(spark, root)
    rng = random.Random(args.seed)
    expected_clean = duplicates = rejected = delivered = revenue = 0
    errors = []
    timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)

    def send(e):
        producer.send(topic, key=e["order_id"].encode(), value=json.dumps(e).encode()).add_errback(
            errors.append
        )

    try:
        started = time.perf_counter()
        for index in range(args.orders):
            for e in lifecycle(rng, timestamp):
                send(e)
                expected_clean += 1
                if e["event_type"] == "delivered":
                    delivered += 1
                    revenue += e["order_total"]
            if index % 100 == 0:
                send(e)
                duplicates += 1
            if index % 200 == 0:
                producer.send(topic, key=b"malformed", value=b"{broken").add_errback(errors.append)
                rejected += 1
        # A valid boundary event advances event time so the first five-minute window closes.
        boundary = lifecycle(rng, timestamp + timedelta(minutes=20))[0]
        send(boundary)
        expected_clean += 1
        producer.flush(120)
        if errors:
            raise RuntimeError(str(errors[0]))
        generated_at = time.perf_counter()
        producer.close()
        ingestion_progress = wait(start_ingestion(spark, root, checkpoints, topic, True))
        ingested_at = time.perf_counter()
        metrics_progress = wait(start_metrics(spark, root, checkpoints, True))
        completed_at = time.perf_counter()

        def table(name):
            return spark.read.format("delta").load(f"{root}/{name}")

        clean_count = table("clean_events").count()
        rejected_count = table("rejected_events").count()
        received_count = table("received_events").count()
        totals = (
            table("delivery_windows").agg(F.sum("delivered_orders"), F.sum("revenue_cents")).first()
        )
        assert clean_count == expected_clean
        assert rejected_count == rejected
        assert received_count == expected_clean + duplicates + rejected
        assert tuple(totals) == (delivered, revenue)
        assert table("clean_events").select("event_id").distinct().count() == clean_count
        duration = completed_at - started
        result = {
            "measured_at_utc": datetime.now(timezone.utc).isoformat(),
            "workload": "finite backlog; generation followed by ingestion and window finalization",
            "orders": args.orders,
            "seed": args.seed,
            "boundary_events": 1,
            "generated_events": received_count,
            "processed_unique_events": clean_count,
            "rejected_events": rejected_count,
            "duplicate_events": duplicates,
            "delivered_orders": delivered,
            "revenue_cents": revenue,
            "generation_seconds": round(generated_at - started, 3),
            "ingestion_seconds": round(ingested_at - generated_at, 3),
            "window_seconds": round(completed_at - ingested_at, 3),
            "duration_seconds": round(duration, 3),
            "input_events_per_second": round(received_count / duration, 2),
            "kafka_partitions": 3,
            "spark_master": spark.sparkContext.master,
            "spark_shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
            "max_offsets_per_trigger": int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000")),
            "storage": "local filesystem Delta",
            "python_version": platform.python_version(),
            "platform": platform.system(),
            "architecture": platform.machine(),
            "spark_version": spark.version,
            "delta_version": "3.3.2",
            "kafka_version": "3.9.1",
            "ingestion_microbatches": len(ingestion_progress),
            "metrics_microbatches": len(metrics_progress),
            "assertions_passed": True,
            "timing_excludes": "runtime startup, empty table initialization and post-run verification",
        }
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    finally:
        for query in spark.streams.active:
            query.stop()
        producer.close()
        spark.stop()
        admin.delete_topics([topic])
        admin.close()


if __name__ == "__main__":
    main()
