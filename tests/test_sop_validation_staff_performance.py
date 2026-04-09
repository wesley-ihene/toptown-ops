"""Tests for Phase 1 staff performance SOP validation."""

from packages.sop_validation.staff_performance import validate_staff_performance


def test_validate_staff_performance_accepts_consistent_payload() -> None:
    result = validate_staff_performance(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "total_staff_records": 2,
                "total_items_moved": 15,
            },
            "items": [
                {"staff_name": "John", "duty_status": "active", "items_moved": 10},
                {"staff_name": "Mary", "duty_status": "active", "items_moved": 5},
            ],
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_staff_performance_rejects_metric_mismatch() -> None:
    result = validate_staff_performance(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "total_staff_records": 3,
                "total_items_moved": 12,
            },
            "items": [
                {"staff_name": "John", "duty_status": "active", "items_moved": 10},
                {"staff_name": "Mary", "duty_status": "active", "items_moved": 5},
            ],
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["invalid_count_mismatch", "invalid_count_mismatch"]
