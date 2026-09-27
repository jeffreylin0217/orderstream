"""Publish interleaved lifecycles, preserving each order's event order."""

import argparse
import heapq
import json
import logging
import random
import time
from datetime import datetime

from kafka import KafkaProducer

from producer.events import lifecycle


def publish(bootstrap: str, topic: str, orders: int, rate: float, seed: int, realtime=True):
    if orders < 0 or rate < 0:
        raise ValueError("orders and rate must be nonnegative; zero means unbounded/unthrottled")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        enable_idempotence=True,
        key_serializer=lambda key: key.encode(),
        value_serializer=lambda event: json.dumps(event).encode(),
        linger_ms=10,
    )
    rng = random.Random(seed)
    sent = 0
    errors = []
    pending = []
    created = 0
    next_order = time.monotonic()
    started = time.monotonic()
    try:
        while orders == 0 or created < orders or pending:
            now = time.monotonic()
            if (orders == 0 or created < orders) and now >= next_order:
                for event in lifecycle(rng):
                    due = datetime.fromisoformat(event["event_timestamp"]).timestamp()
                    heapq.heappush(pending, (due, event["event_id"], event))
                created += 1
                # Rate is new orders per second; event rate depends on lifecycle length.
                next_order = now + (1 / rate if rate else 0)
            while pending and (not realtime or pending[0][0] <= time.time()):
                _, _, event = heapq.heappop(pending)
                producer.send(topic, key=event["order_id"], value=event).add_errback(errors.append)
                sent += 1
            if realtime:
                time.sleep(0.005)
        producer.flush(timeout=120)
    finally:
        producer.close(timeout=120)
    if errors:
        raise RuntimeError(f"Kafka delivery failed: {errors[0]}")
    result = {"orders": created, "events": sent, "seconds": time.monotonic() - started}
    logging.info("producer completed: %s", result)
    return result


def main():
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=0)
    parser.add_argument("--rate", type=float, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--mode", choices=["development", "benchmark"], default="development")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    print(
        json.dumps(
            publish(
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                os.getenv("KAFKA_TOPIC", "order-events"),
                args.orders,
                args.rate,
                args.seed,
                args.mode == "development",
            )
        )
    )


if __name__ == "__main__":
    main()
