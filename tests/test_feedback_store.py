"""Tests for append-only operator feedback storage."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from packages.feedback_store import (
    build_action_feedback_state,
    list_feedback_for_date,
    read_action_feedback,
    record_action_feedback,
)
from packages.record_store import get_feedback_path


def test_acknowledged_feedback_write_is_deterministic(tmp_path: Path) -> None:
    action_path = _write_action(tmp_path, action_id="action-1")

    result = record_action_feedback(
        action_id="action-1",
        branch="waigani",
        report_date="2026-04-07",
        status="acknowledged",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T10:00:00Z",
        source_action_path=str(action_path),
        output_root=tmp_path,
    )

    feedback_path = get_feedback_path("2026-04-07", "waigani", "action-1", output_root=tmp_path)
    assert Path(result["path"]) == feedback_path
    payload = json.loads(feedback_path.read_text(encoding="utf-8"))
    assert payload["status"] == "acknowledged"
    assert payload["acknowledged_by"] == "Ops One"
    assert payload["acknowledged_at"] == "2026-04-07T10:00:00Z"
    assert payload["source_action_path"] == str(action_path)
    assert payload["history"][0]["feedback_id"] == "action_1_001"


def test_in_progress_and_resolved_writes_preserve_audit_trail(tmp_path: Path) -> None:
    action_path = _write_action(tmp_path, action_id="action-2")

    record_action_feedback(
        action_id="action-2",
        branch="waigani",
        report_date="2026-04-07",
        status="acknowledged",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T10:00:00Z",
        source_action_path=str(action_path),
        output_root=tmp_path,
    )
    record_action_feedback(
        action_id="action-2",
        branch="waigani",
        report_date="2026-04-07",
        status="in_progress",
        acknowledged_by="Ops Two",
        acknowledged_at="2026-04-07T11:00:00Z",
        resolution_note="Follow-up call in progress",
        source_action_path=str(action_path),
        output_root=tmp_path,
    )
    record_action_feedback(
        action_id="action-2",
        branch="waigani",
        report_date="2026-04-07",
        status="resolved",
        acknowledged_by="Ops Three",
        acknowledged_at="2026-04-07T12:00:00Z",
        resolution_note="Store team confirmed completion",
        evidence_paths=["records/evidence/waigani/photo-1.jpg"],
        source_action_path=str(action_path),
        output_root=tmp_path,
    )

    payload = read_action_feedback("2026-04-07", "waigani", "action-2", output_root=tmp_path)
    assert payload is not None
    assert payload["status"] == "resolved"
    assert payload["resolution_note"] == "Store team confirmed completion"
    assert payload["history"][0]["status"] == "acknowledged"
    assert payload["history"][1]["status"] == "in_progress"
    assert payload["history"][2]["status"] == "resolved"
    assert payload["history"][2]["evidence_paths"] == ["records/evidence/waigani/photo-1.jpg"]


def test_dismissed_write_and_idempotent_repeat_do_not_duplicate_history(tmp_path: Path) -> None:
    action_path = _write_action(tmp_path, action_id="action-3")

    first = record_action_feedback(
        action_id="action-3",
        branch="waigani",
        report_date="2026-04-07",
        status="dismissed",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T10:00:00Z",
        resolution_note="False positive after manual verification",
        source_action_path=str(action_path),
        output_root=tmp_path,
    )
    second = record_action_feedback(
        action_id="action-3",
        branch="waigani",
        report_date="2026-04-07",
        status="dismissed",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T10:30:00Z",
        resolution_note="False positive after manual verification",
        source_action_path=str(action_path),
        output_root=tmp_path,
    )

    assert first["transition_status"] == "recorded"
    assert second["transition_status"] == "unchanged"
    payload = read_action_feedback("2026-04-07", "waigani", "action-3", output_root=tmp_path)
    assert payload is not None
    assert payload["status"] == "dismissed"
    assert len(payload["history"]) == 1


def test_invalid_status_transition_is_rejected(tmp_path: Path) -> None:
    action_path = _write_action(tmp_path, action_id="action-4")

    record_action_feedback(
        action_id="action-4",
        branch="waigani",
        report_date="2026-04-07",
        status="resolved",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T10:00:00Z",
        source_action_path=str(action_path),
        output_root=tmp_path,
    )

    with pytest.raises(ValueError, match="invalid feedback status transition"):
        record_action_feedback(
            action_id="action-4",
            branch="waigani",
            report_date="2026-04-07",
            status="acknowledged",
            acknowledged_by="Ops Two",
            source_action_path=str(action_path),
            output_root=tmp_path,
        )


def test_linked_action_reference_fields_are_preserved_and_listed(tmp_path: Path) -> None:
    action_path = _write_action(tmp_path, action_id="action-5")
    review_path = tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales_income" / "action_5.json"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(
        json.dumps({"linked_action_id": "action-5", "linked_action_path": str(action_path)}, sort_keys=True),
        encoding="utf-8",
    )

    record_action_feedback(
        action_id="action-5",
        branch="waigani",
        report_date="2026-04-07",
        status="acknowledged",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T10:00:00Z",
        source_action_path=str(action_path),
        linked_review_queue_path=str(review_path),
        output_root=tmp_path,
    )

    feedback_items = list_feedback_for_date("2026-04-07", branch="waigani", output_root=tmp_path)
    assert len(feedback_items) == 1
    assert feedback_items[0]["linked_review_queue_path"] == str(review_path)


def test_build_action_feedback_state_derives_pending_and_stale_status(tmp_path: Path) -> None:
    _write_action(tmp_path, action_id="action-pending", expires_at="2026-04-06T23:59:59Z")
    resolved_action_path = _write_action(tmp_path, action_id="action-resolved", expires_at="2026-04-08T23:59:59Z")
    review_path = tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales_income" / "action_pending.json"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(
        json.dumps({"linked_action_id": "action-pending", "linked_action_path": "records/actions/example.json"}, sort_keys=True),
        encoding="utf-8",
    )
    record_action_feedback(
        action_id="action-resolved",
        branch="waigani",
        report_date="2026-04-07",
        status="resolved",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T12:00:00Z",
        source_action_path=str(resolved_action_path),
        output_root=tmp_path,
    )

    payload = build_action_feedback_state(
        "2026-04-07",
        branch="waigani",
        output_root=tmp_path,
        now_utc="2026-04-08T12:00:00Z",
    )

    assert payload["summary"]["pending_actions"] == 1
    assert payload["summary"]["actions_resolved"] == 1
    assert payload["summary"]["review_linked_actions"] == 1
    assert payload["summary"]["stale_pending_actions"] == 1
    assert payload["pending_actions"][0]["action_id"] == "action-pending"
    assert payload["pending_actions"][0]["linked_review_queue_path"] == str(review_path)


def _write_action(
    root: Path,
    *,
    action_id: str,
    expires_at: str = "2026-04-08T23:59:59Z",
) -> Path:
    path = root / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate" / f"{action_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "action_id": action_id,
                "action_type": "low_conversion_rate",
                "rule_code": "low_conversion_rate",
                "branch": "waigani",
                "report_date": "2026-04-07",
                "signal_type": "sales_income",
                "priority": "high",
                "severity": "warning",
                "assigned_to": "branch_supervisor",
                "requires_ack": True,
                "status": "pending",
                "expires_at": expires_at,
                "summary": "Review floor engagement.",
                "delivery_status": "pending_manual_dispatch",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path
