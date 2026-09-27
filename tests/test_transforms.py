import json
import random
from datetime import datetime, timezone

import pytest
from producer.events import lifecycle
from streaming.transforms import parse_events, delivery_windows

pytestmark = pytest.mark.integration


def raw_frame(spark, payloads):
    return spark.createDataFrame(
        [
            (payload, key, "test", 0, i, datetime.now(timezone.utc))
            for i, (payload, key) in enumerate(payloads)
        ],
        "value string, key string, topic string, partition int, offset long, timestamp timestamp",
    )


def test_parse_and_reject(spark):
    event = lifecycle(random.Random(1))[0]
    valid = (json.dumps(event), event["order_id"])
    bad = [
        ("{bad json", "x"),
        ("{}", "x"),
        (json.dumps(dict(event, quantity=-1)), event["order_id"]),
        (json.dumps(dict(event, event_timestamp="yesterday")), event["order_id"]),
        (json.dumps(event), "wrong-order"),
    ]
    rows = parse_events(raw_frame(spark, [valid] + bad)).collect()
    assert rows[0].is_valid
    assert all(not row.is_valid and row.rejection_reason for row in rows[1:])
    assert rows[0].event_time is not None


def test_delivery_totals(spark):
    events = lifecycle(random.Random(42), datetime(2025, 1, 1, tzinfo=timezone.utc))
    events[-1]["event_type"] = events[-1]["status"] = "delivered"
    parsed = parse_events(raw_frame(spark, [(json.dumps(e), e["order_id"]) for e in events]))
    row = delivery_windows(parsed).first()
    assert row.delivered_orders == 1
    assert row.revenue_cents == events[-1]["order_total"]
    assert (row.window_end - row.window_start).total_seconds() == 300
