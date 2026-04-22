"""Tests for lightweight daily observability artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from packages.observability import (
    load_daily_artifact,
    record_action_event,
    record_export_event,
    record_pre_ingestion_validation_event,
    refresh_feedback_summary,
)
from packages.provenance_store import write_provenance_record


def test_summary_artifact_is_generated_and_core_metrics_update(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    write_provenance_record(
        outcome="accepted",
        report_type="sales",
        branch="waigani",
        report_date="2026-04-07",
        raw_message_hash="accepted-hash",
        parser_used="sales_income_agent",
        parse_mode="strict",
        confidence=0.95,
        warnings=[],
        validation_outcome={"accepted": True, "status": "accepted"},
        acceptance_outcome={"decision": "accept", "status": "accept"},
        downstream_references={"structured_records": ["records/structured/sales_income/waigani/2026-04-07.json"]},
    )
    write_provenance_record(
        outcome="review",
        report_type="sales",
        branch="waigani",
        report_date="2026-04-07",
        raw_message_hash="review-hash",
        parser_used="fallback_extraction_agent",
        parse_mode="fallback",
        confidence=0.65,
        warnings=[{"code": "needs_review", "severity": "warning", "message": "review required"}],
        validation_outcome={"accepted": True, "status": "accepted"},
        acceptance_outcome={"decision": "review", "status": "review"},
        downstream_references={"review_queue_path": "records/review/2026_04_07/waigani/sales/review-hash.json"},
    )
    write_provenance_record(
        outcome="rejected",
        report_type="sales",
        branch="waigani",
        report_date="2026-04-07",
        raw_message_hash="rejected-hash",
        parser_used="fallback_extraction_agent",
        parse_mode="fallback",
        confidence=0.2,
        warnings=[{"code": "invalid_totals", "severity": "error", "message": "invalid"}],
        validation_outcome={"accepted": False, "status": "rejected"},
        acceptance_outcome={"decision": "reject", "status": "reject"},
        downstream_references={"rejected_meta_path": "records/rejected/whatsapp/sales/example.meta.json"},
    )
    record_export_event(
        report_date="2026-04-07",
        branch="waigani",
        success=True,
        manifest_summary={"written": 2, "failed": 0},
    )
    record_export_event(
        report_date="2026-04-07",
        branch="waigani",
        success=False,
        error="bridge unavailable",
    )

    summary_path = tmp_path / "records" / "observability" / "daily" / "2026_04_07" / "summary.json"
    assert summary_path.exists()

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["report_date"] == "2026-04-07"
    assert payload["summary"] == {
        "intake_volume": 3,
        "accept_count": 1,
        "review_count": 1,
        "reject_count": 1,
        "fallback_activation_count": 2,
        "fallback_activation_rate": 0.6667,
    }
    assert payload["agents"]["sales_income_agent"] == {
        "processed_count": 1,
        "failure_count": 0,
        "failure_rate": 0.0,
    }
    assert payload["agents"]["fallback_extraction_agent"] == {
        "processed_count": 2,
        "failure_count": 1,
        "failure_rate": 0.5,
    }
    assert payload["branches"]["waigani"]["processed_count"] == 3
    assert payload["branches"]["waigani"]["warning_record_count"] == 2
    assert payload["branches"]["waigani"]["low_confidence_count"] == 2
    assert payload["exports"]["success_count"] == 1
    assert payload["exports"]["failure_count"] == 1
    assert payload["exports"]["last_manifest_summary"] == {"written": 2, "failed": 0}
    assert payload["exports"]["last_error"] == "bridge unavailable"


def test_pre_ingestion_validation_artifact_records_counters_and_events(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    record_pre_ingestion_validation_event(
        report_date="2026-04-07",
        received_at="2026-04-07T12:00:00Z",
        message_id="wamid.accepted-1",
        payload_kind="text",
        result={
            "status": "accepted",
            "cleaned_text": "DAY-END SALES REPORT",
            "reasons": [],
            "warnings": [],
            "detected_risks": [],
            "suggested_report_family": "sales",
            "validator_version": "v1",
        },
        output_root=tmp_path,
    )
    record_pre_ingestion_validation_event(
        report_date="2026-04-07",
        received_at="2026-04-07T12:05:00Z",
        message_id="wamid.cleaned-1",
        payload_kind="text",
        result={
            "status": "cleaned",
            "cleaned_text": "DAY-END SALES REPORT\n\nBranch: Waigani",
            "reasons": [{"code": "collapsed_blank_lines", "message": "collapsed"}],
            "warnings": ["Multiple strong report headers were detected in one message."],
            "detected_risks": ["mixed_report_risk"],
            "suggested_report_family": None,
            "validator_version": "v1",
        },
        output_root=tmp_path,
    )
    record_pre_ingestion_validation_event(
        report_date="2026-04-07",
        received_at="2026-04-07T12:10:00Z",
        message_id="wamid.rejected-1",
        payload_kind="image",
        result={
            "status": "rejected",
            "cleaned_text": "",
            "reasons": [{"code": "unsupported_payload_kind", "message": "unsupported"}],
            "warnings": [],
            "detected_risks": [],
            "suggested_report_family": None,
            "validator_version": "v1",
        },
        output_root=tmp_path,
    )

    payload = load_daily_artifact("pre_ingestion_validation", "2026-04-07", output_root=tmp_path)

    assert payload is not None
    assert payload["summary"] == {
        "accepted": 1,
        "cleaned": 1,
        "rejected": 1,
        "mixed_report_risk": 1,
        "empty_input": 0,
        "unsupported_payload_kind": 1,
    }
    assert [event["message_id"] for event in payload["events"]] == [
        "wamid.accepted-1",
        "wamid.cleaned-1",
        "wamid.rejected-1",
    ]


def test_pre_ingestion_validation_repeated_writes_aggregate_safely(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    for message_id in ("wamid.reject-1", "wamid.reject-2"):
        record_pre_ingestion_validation_event(
            report_date="2026-04-07",
            received_at="2026-04-07T12:15:00Z",
            message_id=message_id,
            payload_kind="text",
            result={
                "status": "rejected",
                "cleaned_text": "",
                "reasons": [{"code": "empty_input", "message": "empty"}],
                "warnings": [],
                "detected_risks": [],
                "suggested_report_family": None,
                "validator_version": "v1",
            },
            output_root=tmp_path,
        )

    payload = load_daily_artifact("pre_ingestion_validation", "2026-04-07", output_root=tmp_path)

    assert payload is not None
    assert payload["summary"]["rejected"] == 2
    assert payload["summary"]["empty_input"] == 2
    assert len(payload["events"]) == 2


def test_autonomous_action_observability_records_counters_and_breakdowns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    record_action_event(
        report_date="2026-04-07",
        branch="waigani",
        signal_type="sales_income",
        outcome="generated",
        rule_code="low_conversion_rate",
        priority="high",
        action_id="action-1",
        dedupe_key="waigani:2026-04-07:low_conversion_rate:branch_conversion",
        output_root=tmp_path,
    )
    record_action_event(
        report_date="2026-04-07",
        branch="waigani",
        signal_type="sales_income",
        outcome="skipped",
        output_root=tmp_path,
    )
    record_action_event(
        report_date="2026-04-07",
        branch="waigani",
        signal_type="sales_income",
        outcome="suppressed_replay",
        output_root=tmp_path,
    )

    payload = load_daily_artifact("autonomous_actions", "2026-04-07", output_root=tmp_path)

    assert payload is not None
    assert payload["summary"] == {
        "actions_generated": 1,
        "actions_skipped": 1,
        "actions_suppressed_replay": 1,
        "actions_by_rule": {"low_conversion_rate": 1},
        "actions_by_priority": {"high": 1},
    }
    assert [event["outcome"] for event in payload["events"]] == ["generated", "skipped", "suppressed_replay"]


def test_feedback_summary_tracks_lifecycle_counters_and_stale_pending(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    _write_json(
        tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate" / "action-1.json",
        {
            "action_id": "action-1",
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
            "expires_at": "2026-04-06T23:59:59Z",
        },
    )
    _write_json(
        tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "attendance_shortage" / "action-2.json",
        {
            "action_id": "action-2",
            "action_type": "attendance_shortage",
            "rule_code": "attendance_shortage",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "signal_type": "hr_attendance",
            "priority": "high",
            "severity": "warning",
            "assigned_to": "branch_supervisor",
            "requires_ack": True,
            "status": "pending",
            "expires_at": "2026-04-08T23:59:59Z",
        },
    )
    _write_json(
        tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales_income" / "action_1.json",
        {
            "linked_action_id": "action-1",
            "linked_action_path": "records/actions/2026-04-07/waigani/low_conversion_rate/action-1.json",
        },
    )
    _write_json(
        tmp_path / "records" / "feedback" / "2026-04-07" / "waigani" / "action_1.json",
        {
            "feedback_id": "action_1_001",
            "action_id": "action-1",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "status": "acknowledged",
            "acknowledged_by": "Ops One",
            "acknowledged_at": "2026-04-07T10:00:00Z",
            "resolution_note": None,
            "evidence_paths": [],
            "source_action_path": "records/actions/2026-04-07/waigani/low_conversion_rate/action-1.json",
            "linked_review_queue_path": "records/review/2026_04_07/waigani/sales_income/action_1.json",
            "version": "v1",
            "history": [
                {
                    "feedback_id": "action_1_001",
                    "status": "acknowledged",
                    "acknowledged_by": "Ops One",
                    "acknowledged_at": "2026-04-07T10:00:00Z",
                    "resolution_note": None,
                    "evidence_paths": [],
                    "source_action_path": "records/actions/2026-04-07/waigani/low_conversion_rate/action-1.json",
                    "linked_review_queue_path": "records/review/2026_04_07/waigani/sales_income/action_1.json",
                }
            ],
        },
    )
    _write_json(
        tmp_path / "records" / "feedback" / "2026-04-07" / "waigani" / "action_2.json",
        {
            "feedback_id": "action_2_001",
            "action_id": "action-2",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "status": "resolved",
            "acknowledged_by": "Ops Two",
            "acknowledged_at": "2026-04-07T12:00:00Z",
            "resolution_note": "Completed",
            "evidence_paths": [],
            "source_action_path": "records/actions/2026-04-07/waigani/attendance_shortage/action-2.json",
            "linked_review_queue_path": None,
            "version": "v1",
            "history": [
                {
                    "feedback_id": "action_2_001",
                    "status": "resolved",
                    "acknowledged_by": "Ops Two",
                    "acknowledged_at": "2026-04-07T12:00:00Z",
                    "resolution_note": "Completed",
                    "evidence_paths": [],
                    "source_action_path": "records/actions/2026-04-07/waigani/attendance_shortage/action-2.json",
                    "linked_review_queue_path": None,
                }
            ],
        },
    )

    payload = refresh_feedback_summary(
        report_date="2026-04-07",
        branch="waigani",
        output_root=tmp_path,
        now_utc="2026-04-08T12:00:00Z",
    )

    assert payload["summary"]["actions_acknowledged"] == 1
    assert payload["summary"]["actions_resolved"] == 1
    assert payload["summary"]["review_linked_actions"] == 1
    assert payload["summary"]["stale_pending_actions"] == 1

    summary = _read_json(tmp_path / "records" / "observability" / "daily" / "2026_04_07" / "summary.json")
    assert summary["actions"] == {
        "actions_acknowledged": 1,
        "actions_in_progress": 0,
        "actions_resolved": 1,
        "actions_dismissed": 0,
        "review_linked_actions": 1,
        "stale_pending_actions": 1,
    }

    artifact = load_daily_artifact("feedback_summary", "2026-04-07", output_root=tmp_path)
    assert artifact is not None
    assert artifact["summary"]["feedback_records"] == 2


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "ACTIONS_DIR", records_dir / "actions")
    monkeypatch.setattr(record_paths, "FEEDBACK_DIR", records_dir / "feedback")
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "PROPOSALS_DIR", records_dir / "proposals")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
