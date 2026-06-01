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


def test_validate_attendance_groups_richer_statuses_into_metric_buckets() -> None:
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
    assert result.rejection_codes == ["invalid_count_mismatch", "invalid_count_mismatch"]
    assert all(rejection.code == "invalid_count_mismatch" for rejection in result.rejections)
    assert {rejection.field for rejection in result.rejections} == {
        "metrics.present_count",
        "metrics.absent_count",
    }


def test_validate_attendance_accepts_non_active_staff_excluded_from_active_total() -> None:
    result = validate_attendance(
        {
            "branch": "lae_5th_street",
            "report_date": "2026-05-22",
            "metrics": {
                "total_staff_listed": 18,
                "total_staff": 17,
                "present_count": 17,
                "absent_count": 0,
                "off_count": 0,
                "leave_count": 0,
                "non_active": 1,
            },
            "items": [
                *[{"staff_name": f"Staff {index}", "status": "present"} for index in range(1, 18)],
                {"staff_name": "Joyce Lovave", "status": "non_active"},
            ],
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []
