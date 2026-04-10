"""Tests for lightweight daily observability artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from packages.observability import record_export_event
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


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "PROPOSALS_DIR", records_dir / "proposals")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
