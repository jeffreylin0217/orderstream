"""Read a bounded sample without committing offsets or competing with Spark."""

import argparse
import json
from kafka import KafkaConsumer

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--topic", default="order-events")
parser.add_argument("--count", type=int, default=10)
args = parser.parse_args()
consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers="localhost:9092",
    group_id=None,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
)
seen = 0
try:
    for record in consumer:
        event = json.loads(record.value)
        assert record.key.decode() == event["order_id"]
        print(record.partition, record.offset, event["event_type"])
        seen += 1
        if seen >= args.count:
            break
finally:
    consumer.close()
assert seen == args.count, f"Only received {seen} records"
