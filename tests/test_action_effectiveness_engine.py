"""Tests for deterministic action-effectiveness summaries."""

from __future__ import annotations

import json
from pathlib import Path

from apps.action_effectiveness_engine import analyze_action_effectiveness


def test_rates_are_computed_correctly_and_artifact_is_written(tmp_path: Path) -> None:
    _write_action(
        tmp_path,
        action_id="action-1",
        rule_code="low_conversion_rate",
        branch="waigani",
        priority="high",
        created_at="2026-04-07T09:00:00Z",
        expires_at="2026-04-08T23:59:59Z",
    )
    _write_feedback(
        tmp_path,
        action_id="action-1",
        branch="waigani",
        status="resolved",
        acknowledged_at="2026-04-07T10:00:00Z",
        history=[
            {"status": "acknowledged", "acknowledged_at": "2026-04-07T10:00:00Z"},
            {"status": "resolved", "acknowledged_at": "2026-04-07T11:00:00Z"},
        ],
    )
    _write_action(
        tmp_path,
        action_id="action-2",
        rule_code="low_conversion_rate",
        branch="waigani",
        priority="high",
        created_at="2026-04-07T09:30:00Z",
        expires_at="2026-04-08T23:59:59Z",
    )
    _write_feedback(
        tmp_path,
        action_id="action-2",
        branch="waigani",
        status="dismissed",
        acknowledged_at="2026-04-07T10:30:00Z",
        history=[
            {"status": "dismissed", "acknowledged_at": "2026-04-07T10:30:00Z"},
        ],
    )

    result = analyze_action_effectiveness(
        "2026-04-07",
        output_root=tmp_path,
        generated_at="2026-04-07T12:00:00Z",
    )

    summary = result["summary"]
    assert result["output_path"] == str(
        tmp_path / "records" / "learning" / "action_effectiveness" / "2026-04-07.json"
    )
    assert summary["artifact_type"] == "action_effectiveness"
    assert summary["summary"] == {
        "total_actions": 2,
        "acknowledged_actions": 2,
        "resolved_actions": 1,
        "dismissed_actions": 1,
        "stale_pending_actions": 0,
        "acknowledged_rate": 1.0,
        "resolved_rate": 0.5,
        "dismissed_rate": 0.5,
        "stale_pending_rate": 0.0,
        "average_response_delay": 3600.0,
        "response_delay_sample_count": 2,
    }

    persisted = json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))
    assert persisted["generated_at"] == "2026-04-07T12:00:00Z"
    assert persisted["analysis_window"] == {
        "start_date": "2026-04-01",
        "end_date": "2026-04-07",
        "window_days": 7,
    }


def test_grouping_by_rule_branch_and_priority_is_correct(tmp_path: Path) -> None:
    _write_action(tmp_path, action_id="a-1", rule_code="low_conversion_rate", branch="waigani", priority="high")
    _write_feedback(tmp_path, action_id="a-1", branch="waigani", status="resolved")
    _write_action(tmp_path, action_id="a-2", rule_code="attendance_shortage", branch="lae", priority="medium")
    _write_feedback(tmp_path, action_id="a-2", branch="lae", status="acknowledged")

    summary = analyze_action_effectiveness("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["action_effectiveness_by_rule"] == [
        {
            "rule_code": "attendance_shortage",
            "total_actions": 1,
            "acknowledged_actions": 1,
            "resolved_actions": 0,
            "dismissed_actions": 0,
            "stale_pending_actions": 0,
            "acknowledged_rate": 1.0,
            "resolved_rate": 0.0,
            "dismissed_rate": 0.0,
            "stale_pending_rate": 0.0,
            "average_response_delay": None,
            "response_delay_sample_count": 0,
            "branches": ["lae"],
            "priorities": ["medium"],
        },
        {
            "rule_code": "low_conversion_rate",
            "total_actions": 1,
            "acknowledged_actions": 1,
            "resolved_actions": 1,
            "dismissed_actions": 0,
            "stale_pending_actions": 0,
            "acknowledged_rate": 1.0,
            "resolved_rate": 1.0,
            "dismissed_rate": 0.0,
            "stale_pending_rate": 0.0,
            "average_response_delay": None,
            "response_delay_sample_count": 0,
            "branches": ["waigani"],
            "priorities": ["high"],
        },
    ]
    assert summary["action_response_by_branch"] == [
        {
            "branch": "lae",
            "total_actions": 1,
            "acknowledged_actions": 1,
            "resolved_actions": 0,
            "dismissed_actions": 0,
            "stale_pending_actions": 0,
            "acknowledged_rate": 1.0,
            "resolved_rate": 0.0,
            "dismissed_rate": 0.0,
            "stale_pending_rate": 0.0,
            "average_response_delay": None,
            "response_delay_sample_count": 0,
            "rule_counts": [{"rule_code": "attendance_shortage", "count": 1}],
            "priority_counts": [{"priority": "medium", "count": 1}],
        },
        {
            "branch": "waigani",
            "total_actions": 1,
            "acknowledged_actions": 1,
            "resolved_actions": 1,
            "dismissed_actions": 0,
            "stale_pending_actions": 0,
            "acknowledged_rate": 1.0,
            "resolved_rate": 1.0,
            "dismissed_rate": 0.0,
            "stale_pending_rate": 0.0,
            "average_response_delay": None,
            "response_delay_sample_count": 0,
            "rule_counts": [{"rule_code": "low_conversion_rate", "count": 1}],
            "priority_counts": [{"priority": "high", "count": 1}],
        },
    ]
    assert summary["action_effectiveness_by_priority"] == [
        {
            "priority": "high",
            "total_actions": 1,
            "acknowledged_actions": 1,
            "resolved_actions": 1,
            "dismissed_actions": 0,
            "stale_pending_actions": 0,
            "acknowledged_rate": 1.0,
            "resolved_rate": 1.0,
            "dismissed_rate": 0.0,
            "stale_pending_rate": 0.0,
            "average_response_delay": None,
            "response_delay_sample_count": 0,
        },
        {
            "priority": "medium",
            "total_actions": 1,
            "acknowledged_actions": 1,
            "resolved_actions": 0,
            "dismissed_actions": 0,
            "stale_pending_actions": 0,
            "acknowledged_rate": 1.0,
            "resolved_rate": 0.0,
            "dismissed_rate": 0.0,
            "stale_pending_rate": 0.0,
            "average_response_delay": None,
            "response_delay_sample_count": 0,
        },
    ]


def test_stale_action_detection_and_noisy_rule_candidates_are_reported(tmp_path: Path) -> None:
    _write_action(
        tmp_path,
        action_id="stale-1",
        rule_code="low_conversion_rate",
        branch="waigani",
        priority="high",
        expires_at="2026-04-06T23:59:59Z",
    )
    _write_action(
        tmp_path,
        action_id="dismissed-1",
        rule_code="low_conversion_rate",
        branch="waigani",
        priority="high",
        expires_at="2026-04-08T23:59:59Z",
    )
    _write_feedback(tmp_path, action_id="dismissed-1", branch="waigani", status="dismissed")

    summary = analyze_action_effectiveness("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["stale_actions_summary"] == {
        "count": 1,
        "by_rule": [{"rule_code": "low_conversion_rate", "count": 1}],
        "by_branch": [{"branch": "waigani", "count": 1}],
        "items": [
            {
                "action_id": "stale-1",
                "rule_code": "low_conversion_rate",
                "branch": "waigani",
                "priority": "high",
                "report_date": "2026-04-07",
                "expires_at": "2026-04-06T23:59:59Z",
                "effective_status": "pending",
            }
        ],
    }
    assert summary["noisy_rules_candidates"] == [
        {
            "rule_code": "low_conversion_rate",
            "total_actions": 2,
            "dismissed_rate": 0.5,
            "dismissed_actions": 1,
            "stale_pending_rate": 0.5,
            "stale_pending_actions": 1,
            "branches": ["waigani"],
            "priorities": ["high"],
        }
    ]


def test_missing_feedback_is_handled_safely(tmp_path: Path) -> None:
    _write_action(
        tmp_path,
        action_id="action-pending",
        rule_code="attendance_shortage",
        branch="lae",
        priority="medium",
        expires_at="2026-04-08T23:59:59Z",
    )

    summary = analyze_action_effectiveness("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["summary"] == {
        "total_actions": 1,
        "acknowledged_actions": 0,
        "resolved_actions": 0,
        "dismissed_actions": 0,
        "stale_pending_actions": 0,
        "acknowledged_rate": 0.0,
        "resolved_rate": 0.0,
        "dismissed_rate": 0.0,
        "stale_pending_rate": 0.0,
        "average_response_delay": None,
        "response_delay_sample_count": 0,
    }
    assert summary["action_effectiveness_by_rule"][0]["rule_code"] == "attendance_shortage"
    assert summary["action_effectiveness_by_rule"][0]["acknowledged_rate"] == 0.0


def test_invalid_feedback_files_are_ignored_safely(tmp_path: Path) -> None:
    _write_action(tmp_path, action_id="action-1", rule_code="low_conversion_rate", branch="waigani", priority="high")
    invalid = tmp_path / "records" / "feedback" / "2026-04-07" / "waigani" / "broken.json"
    invalid.parent.mkdir(parents=True, exist_ok=True)
    invalid.write_text("{not-json", encoding="utf-8")

    summary = analyze_action_effectiveness("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["total_actions"] == 1
    assert summary["summary"]["acknowledged_actions"] == 0
    assert summary["source_paths"] == [
        str(tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate" / "action-1.json")
    ]


def _write_action(
    root: Path,
    *,
    action_id: str,
    rule_code: str,
    branch: str,
    priority: str,
    created_at: str | None = None,
    expires_at: str = "2026-04-08T23:59:59Z",
) -> None:
    path = root / "records" / "actions" / "2026-04-07" / branch / rule_code / f"{action_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "action_id": action_id,
        "action_type": rule_code,
        "rule_code": rule_code,
        "branch": branch,
        "report_date": "2026-04-07",
        "priority": priority,
        "status": "pending",
        "expires_at": expires_at,
    }
    if created_at is not None:
        payload["created_at"] = created_at
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_feedback(
    root: Path,
    *,
    action_id: str,
    branch: str,
    status: str,
    acknowledged_at: str | None = None,
    history: list[dict[str, object]] | None = None,
) -> None:
    path = root / "records" / "feedback" / "2026-04-07" / branch / f"{action_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "action_id": action_id,
        "branch": branch,
        "report_date": "2026-04-07",
        "status": status,
        "acknowledged_at": acknowledged_at,
        "history": history or [],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
