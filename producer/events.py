"""Generate one-item orders with fixed-point monetary amounts in USD cents."""

import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4

EVENT_TYPES = (
    "order_created",
    "payment_authorized",
    "payment_failed",
    "order_ready",
    "shipment_dispatched",
    "delivered",
    "cancelled",
)


def lifecycle(rng: random.Random, start: datetime | None = None) -> list[dict]:
    start = start or datetime.now(timezone.utc)
    quantity = rng.randint(1, 4)
    price = rng.randint(500, 20000)
    common = {
        "order_id": str(uuid4()),
        "customer_id": f"customer-{rng.randint(1, 10000)}",
        "product_id": f"product-{rng.randint(1, 100)}",
        "quantity": quantity,
        "unit_price": price,
        "order_total": quantity * price,
        "currency": "USD",
        "source": "synthetic-fulfillment",
        "schema_version": 1,
    }
    stages = ["order_created"]
    if rng.random() < 0.08:
        stages += ["payment_failed", "cancelled"]
    elif rng.random() < 0.05:
        stages += ["payment_authorized", "cancelled"]
    else:
        stages += ["payment_authorized", "order_ready", "shipment_dispatched", "delivered"]
    # A compressed lifecycle lasts 4–40 seconds; useful for local demonstrations.
    spacing = rng.randint(1, 10)
    return [
        dict(
            common,
            event_id=str(uuid4()),
            event_type=stage,
            status=stage,
            event_timestamp=(start + timedelta(seconds=i * spacing)).isoformat(),
        )
        for i, stage in enumerate(stages)
    ]
