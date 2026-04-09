"""Tests for Phase 1 store monitoring SOP validation."""

from packages.sop_validation.store_monitoring import validate_store_monitoring


def test_validate_store_monitoring_accepts_consistent_payload() -> None:
    result = validate_store_monitoring(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "total_checks": 3,
                "issue_count": 1,
                "critical_count": 1,
            },
            "items": [
                {"check_name": "Opening", "status": "ok"},
                {"check_name": "Till", "status": "issue"},
                {"check_name": "CCTV", "status": "critical"},
            ],
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_store_monitoring_rejects_invalid_status_and_metric_mismatch() -> None:
    result = validate_store_monitoring(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "total_checks": 3,
                "issue_count": 0,
                "critical_count": 0,
            },
            "items": [
                {"check_name": "Opening", "status": "ok"},
                {"check_name": "Till", "status": "issue"},
                {"check_name": "CCTV", "status": "blocked"},
            ],
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["invalid_status", "invalid_count_mismatch"]
