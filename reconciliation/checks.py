"""Pure validation logic shared by the CLI and Airflow workflow."""


def validate_report(report, max_rejected_fraction=0.01):
    if not 0 <= max_rejected_fraction <= 1:
        raise ValueError("max_rejected_fraction must be between zero and one")
    if report["expected_events"] == 0:
        raise ValueError(
            "No valid events for the requested day; completeness cannot be established"
        )
    if report["missing_events"] or report["unexpected_events"]:
        raise ValueError("Received and cleaned event IDs do not reconcile")
    if report["clean_events"] != report["expected_events"] or report["clean_duplicates"]:
        raise ValueError("Clean event counts or uniqueness failed")
    denominator = report["received_on_day"]
    if denominator and report["rejected_on_day"] / denominator > max_rejected_fraction:
        raise ValueError("Rejected-event fraction exceeds configured threshold")
    if report["rejected_on_day"] != report["expected_rejected_on_day"]:
        raise ValueError("Rejected-event routing is incomplete")
    return report
