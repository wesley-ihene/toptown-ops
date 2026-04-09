"""Tests for Phase 1 supervisor control SOP validation."""

from packages.sop_validation.supervisor_control import validate_supervisor_control


def test_validate_supervisor_control_accepts_confirmed_resolved_items() -> None:
    result = validate_supervisor_control(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {"exception_count": 1},
            "items": [
                {
                    "exception_type": "STAFF_ISSUE",
                    "action_taken": "Resolved",
                    "supervisor_confirmed": "YES",
                }
            ],
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_supervisor_control_rejects_missing_confirmation_and_open_action() -> None:
    result = validate_supervisor_control(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {"exception_count": 1},
            "items": [
                {
                    "exception_type": "STAFF_ISSUE",
                    "action_taken": "Escalated",
                    "supervisor_confirmed": "NO",
                }
            ],
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["missing_confirmation", "unresolved_exception"]
