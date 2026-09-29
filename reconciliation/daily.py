"""Read fixed Delta snapshots, validate a UTC date, refresh one daily aggregate."""

import argparse
import json
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from reconciliation.checks import validate_report
from streaming.pipeline import paths
from streaming.session import create_spark


def inspect_day(spark, root, day):
    day = date.fromisoformat(day).isoformat()
    versions = {
        name: DeltaTable.forPath(spark, f"{root}/{name}").history(1).first().version
        for name in ("received_events", "clean_events", "rejected_events")
    }

    def read(name):
        return (
            spark.read.format("delta").option("versionAsOf", versions[name]).load(f"{root}/{name}")
        )

    received = read("received_events")
    valid = received.filter(F.col("is_valid") & (F.to_date("event_time") == day))
    clean = read("clean_events").filter(F.to_date("event_time") == day)
    expected_ids = valid.select("event_id").distinct()
    clean_ids = clean.select("event_id").distinct()
    received_day = received.filter(F.to_date("kafka_timestamp") == day)
    rejected_day = read("rejected_events").filter(F.to_date("kafka_timestamp") == day)
    expected_count, clean_count = expected_ids.count(), clean.count()
    return {
        "day": day,
        "versions": versions,
        "expected_events": expected_count,
        "clean_events": clean_count,
        "duplicate_events": valid.count() - expected_count,
        "clean_duplicates": clean_count - clean_ids.count(),
        "missing_events": expected_ids.join(clean_ids, "event_id", "left_anti").count(),
        "unexpected_events": clean_ids.join(expected_ids, "event_id", "left_anti").count(),
        "received_on_day": received_day.count(),
        "rejected_on_day": rejected_day.count(),
        "expected_rejected_on_day": received_day.filter(~F.col("is_valid")).count(),
    }


def refresh_day(spark, root, report):
    day = date.fromisoformat(report["day"]).isoformat()
    clean = (
        spark.read.format("delta")
        .option("versionAsOf", report["versions"]["clean_events"])
        .load(f"{root}/clean_events")
        .filter(F.to_date("event_time") == day)
    )
    aggregate = (
        clean.agg(
            F.sum(F.when(F.col("event_type") == "delivered", 1).otherwise(0)).alias(
                "delivered_orders"
            ),
            F.sum(
                F.when(F.col("event_type") == "delivered", F.col("order_total")).otherwise(0)
            ).alias("revenue_cents"),
        )
        .withColumn("day", F.lit(day).cast("date"))
        .withColumn("clean_version", F.lit(report["versions"]["clean_events"]))
    )
    destination = f"{root}/daily_deliveries"
    if not DeltaTable.isDeltaTable(spark, destination):
        aggregate.limit(0).write.format("delta").save(destination)
    (
        DeltaTable.forPath(spark, destination)
        .alias("target")
        .merge(aggregate.alias("source"), "target.day = source.day")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inspect", "validate", "refresh"])
    parser.add_argument("--day", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--max-rejected-fraction", type=float, default=0.01)
    args = parser.parse_args()
    report_path = Path(args.report)
    if args.action == "validate":
        report = json.loads(report_path.read_text())
        if report["day"] != date.fromisoformat(args.day).isoformat():
            raise ValueError("Report date does not match requested date")
        validate_report(report, args.max_rejected_fraction)
        print(json.dumps(report, indent=2))
        return
    spark = create_spark("orderstream-reconciliation")
    root, _ = paths()
    try:
        if args.action == "inspect":
            report = inspect_day(spark, root, args.day)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2) + "\n")
        else:
            report = json.loads(report_path.read_text())
            if report["day"] != date.fromisoformat(args.day).isoformat():
                raise ValueError("Report date does not match requested date")
            validate_report(report, args.max_rejected_fraction)
            refresh_day(spark, root, report)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
