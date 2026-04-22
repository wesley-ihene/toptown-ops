"""Tests for conservative autonomous control decisions."""

from __future__ import annotations

from apps.autonomous_control_engine import generate_control_actions


def test_no_action_on_rejected_governance() -> None:
    result = generate_control_actions(
        structured_payload=_sales_payload(conversion_rate=0.2),
        governance_sidecar={"status": "rejected", "export_allowed": False},
    )

    assert result["status"] == "skipped"
    assert result["actions"] == []


def test_no_action_on_duplicate_or_conflict_statuses() -> None:
    duplicate = generate_control_actions(
        structured_payload=_sales_payload(conversion_rate=0.2),
        governance_sidecar={"status": "duplicate", "export_allowed": False},
    )
    conflict = generate_control_actions(
        structured_payload=_sales_payload(conversion_rate=0.2),
        governance_sidecar={"status": "conflict_blocked", "export_allowed": False},
    )

    assert duplicate["actions"] == []
    assert conflict["actions"] == []


def test_low_conversion_action_generated_when_threshold_breached() -> None:
    result = generate_control_actions(
        structured_payload=_sales_payload(conversion_rate=0.2),
        governance_sidecar={"status": "accepted", "export_allowed": True},
        source_paths=["records/structured/sales_income/waigani/2026-04-07.json"],
    )

    assert result["status"] == "generated"
    assert len(result["actions"]) == 1
    action = result["actions"][0]
    assert action["rule_code"] == "low_conversion_rate"
    assert action["priority"] == "high"
    assert action["assigned_to"] == "branch_supervisor"
    assert action["requires_ack"] is True
    assert action["evidence"]["conversion_rate"] == 0.2


def test_no_action_when_evidence_is_insufficient() -> None:
    payload = _sales_payload(conversion_rate=None)
    del payload["metrics"]["conversion_rate"]

    result = generate_control_actions(
        structured_payload=payload,
        governance_sidecar={"status": "accepted", "export_allowed": True},
    )

    assert result["status"] == "skipped"
    assert result["actions"] == []


def test_attendance_shortage_action_generated_from_stable_metrics() -> None:
    result = generate_control_actions(
        structured_payload={
            "branch": "waigani",
            "report_date": "2026-04-07",
            "signal_type": "hr_attendance",
            "metrics": {
                "total_staff_listed": 8,
                "absent_count": 3,
                "coverage_ratio": 0.5,
                "attendance_gap": 3,
            },
        },
        governance_sidecar={"status": "accepted_with_warning", "export_allowed": True},
    )

    assert result["status"] == "generated"
    assert result["actions"][0]["rule_code"] == "attendance_shortage"
    assert result["actions"][0]["evidence"]["absence_ratio"] == 0.375


def test_dedupe_key_is_stable() -> None:
    first = generate_control_actions(
        structured_payload=_sales_payload(conversion_rate=0.2),
        governance_sidecar={"status": "accepted", "export_allowed": True},
    )
    second = generate_control_actions(
        structured_payload=_sales_payload(conversion_rate=0.2),
        governance_sidecar={"status": "accepted", "export_allowed": True},
    )

    assert first["actions"][0]["dedupe_key"] == second["actions"][0]["dedupe_key"]
    assert first["actions"][0]["action_id"] == second["actions"][0]["action_id"]


def test_replay_is_suppressed_by_default() -> None:
    result = generate_control_actions(
        structured_payload=_sales_payload(conversion_rate=0.2),
        governance_sidecar={"status": "accepted", "export_allowed": True},
        replay=True,
    )

    assert result == {
        "status": "suppressed_replay",
        "reason": "replay_suppressed",
        "actions": [],
    }


def _sales_payload(*, conversion_rate: float | None) -> dict[str, object]:
    return {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "signal_type": "sales_income",
        "metrics": {
            "traffic": 10,
            "served": 2,
            "conversion_rate": conversion_rate,
        },
    }
