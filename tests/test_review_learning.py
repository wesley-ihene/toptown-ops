"""Tests for review-gated learning proposals."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from packages.provenance_store import write_provenance_record


def test_repeated_fallback_pattern_produces_and_persists_proposal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    config_before = Path("config/report_policy.json").read_text(encoding="utf-8")
    rules_before = Path("apps/orchestrator_agent/policy_guard.py").read_text(encoding="utf-8")

    write_provenance_record(
        outcome="review",
        report_type="sales",
        branch="waigani",
        report_date="2026-04-07",
        raw_message_hash="hash-one",
        parser_used="fallback_extraction_agent",
        parse_mode="fallback",
        confidence=0.65,
        warnings=[{"code": "needs_review", "message": "review required"}],
        validation_outcome={"accepted": True, "status": "accepted"},
        acceptance_outcome={
            "decision": "review",
            "reason": "confidence_between_review_and_accept_thresholds",
        },
        downstream_references={"review_queue_path": "records/review/2026_04_07/waigani/sales/hash-one.json"},
    )
    write_provenance_record(
        outcome="review",
        report_type="sales",
        branch="waigani",
        report_date="2026-04-08",
        raw_message_hash="hash-two",
        parser_used="fallback_extraction_agent",
        parse_mode="fallback",
        confidence=0.67,
        warnings=[{"code": "needs_review", "message": "review required"}],
        validation_outcome={"accepted": True, "status": "accepted"},
        acceptance_outcome={
            "decision": "review",
            "reason": "confidence_between_review_and_accept_thresholds",
        },
        downstream_references={"review_queue_path": "records/review/2026_04_08/waigani/sales/hash-two.json"},
    )

    proposal_paths = sorted((tmp_path / "records" / "proposals").rglob("*.json"))
    assert len(proposal_paths) == 1

    proposal = json.loads(proposal_paths[0].read_text(encoding="utf-8"))
    assert proposal["proposal_type"] == "confidence_threshold_adjustment"
    assert proposal["status"] == "proposal_only"
    assert proposal["report_type"] == "sales"
    assert proposal["observation_count"] == 2
    assert proposal["pattern"]["parse_mode"] == "fallback"
    assert proposal["pattern"]["acceptance_decision"] == "review"
    assert proposal["recommendation"]["current_thresholds"]["auto_accept_min"] == 0.9

    assert Path("config/report_policy.json").read_text(encoding="utf-8") == config_before
    assert Path("apps/orchestrator_agent/policy_guard.py").read_text(encoding="utf-8") == rules_before


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
