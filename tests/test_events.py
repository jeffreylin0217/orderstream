import json
import random
from pathlib import Path

import jsonschema
import pytest

from producer.events import lifecycle

SCHEMA = json.loads(Path("schemas/order-event.json").read_text())


def test_generated_lifecycles():
    rng = random.Random(42)
    for _ in range(100):
        events = lifecycle(rng)
        assert events[0]["event_type"] == "order_created"
        assert events[-1]["event_type"] in {"delivered", "cancelled"}
        assert len({e["order_id"] for e in events}) == 1
        for event in events:
            jsonschema.validate(event, SCHEMA, format_checker=jsonschema.FormatChecker())
            assert event["order_total"] == event["unit_price"] * event["quantity"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("quantity", -1),
        ("event_id", None),
        ("event_timestamp", "yesterday"),
        ("event_type", "unknown"),
    ],
)
def test_invalid_schema(field, value):
    event = lifecycle(random.Random(42))[0]
    event[field] = value
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(event, SCHEMA, format_checker=jsonschema.FormatChecker())
