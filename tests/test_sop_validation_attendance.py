"""Tests for Phase 1 attendance SOP validation."""

from packages.sop_validation.attendance import validate_attendance


def test_validate_attendance_accepts_matching_status_counts() -> None:
    result = validate_attendance(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "total_staff_listed": 3,
                "present_count": 1,
                "absent_count": 1,
                "off_count": 1,
                "leave_count": 0,
            },
            "items": [
                {"staff_name": "John", "status": "present"},
                {"staff_name": "Mary", "status": "absent"},
                {"staff_name": "Peter", "status": "off"},
            ],
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_attendance_rejects_invalid_status_and_count_mismatch() -> None:
    result = validate_attendance(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "total_staff_listed": 2,
                "present_count": 2,
                "absent_count": 0,
                "off_count": 0,
                "leave_count": 0,
            },
            "items": [
                {"staff_name": "John", "status": "present"},
                {"staff_name": "Mary", "status": "late"},
            ],
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["invalid_status", "invalid_count_mismatch"]
