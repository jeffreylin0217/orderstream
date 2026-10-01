# OrderStream

OrderStream is an event-driven data engineering project built around a simulated e-commerce order lifecycle.

## Current architecture

```text
Synthetic order producer
        |
        v
      Kafka
        |
        v
Spark Structured Streaming
        |
        v
    Delta Lake
        |
        v
Airflow daily reconciliation
```

## Current implementation

- Generates synthetic order lifecycle events using a defined JSON event contract.
- Publishes events to Kafka using `order_id` as the message key.
- Processes Kafka events with Spark Structured Streaming.
- Validates incoming records and routes malformed events separately.
- Deduplicates events by `event_id` before writing clean data to Delta Lake.
- Computes event-time delivery metrics using 5-minute windows and a 10-minute watermark.
- Uses Spark checkpoints and Kafka offsets for restart and backlog recovery.
- Runs a daily Airflow reconciliation workflow to inspect, validate, and refresh delivery aggregates.

## Testing

The project includes unit and integration tests covering event generation, parsing, deduplication, late-event handling, recovery behavior, and daily reconciliation.
