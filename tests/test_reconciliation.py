import pytest
from reconciliation.checks import validate_report


def report():
    return dict(
        expected_events=10,
        clean_events=10,
        clean_duplicates=0,
        missing_events=0,
        unexpected_events=0,
        received_on_day=12,
        rejected_on_day=0,
        expected_rejected_on_day=0,
        duplicate_events=2,
    )


def test_valid_reconciliation():
    assert validate_report(report())["duplicate_events"] == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("clean_duplicates", 1),
        ("missing_events", 1),
        ("unexpected_events", 1),
        ("clean_events", 9),
        ("expected_events", 0),
        ("rejected_on_day", 2),
        ("expected_rejected_on_day", 1),
    ],
)
def test_reconciliation_fails_clearly(field, value):
    counts = report()
    counts[field] = value
    with pytest.raises(ValueError):
        validate_report(counts)
