"""Tests for supervisor approval, rejection, replay, and audit flow."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.approval_engine import worker


def test_approve_review_item_writes_structured_and_audit(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    review_path = _write_review_item(
        tmp_path,
        record_id="hash-approve",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-23",
            "resolution_status": "open",
            "candidate_payload": {
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-23",
                "status": "needs_review",
                "warnings": [],
                "metrics": {"gross_sales": 10},
            },
            "validation": {"accepted": True, "details": {}},
            "raw_paths": {"raw_meta_path": str(_write_raw_meta(tmp_path))},
        },
    )

    result = worker.process_supervisor_action(
        {
            "command_name": "approve",
            "record_id": "hash-approve",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-23.json"
    audit_paths = sorted((tmp_path / "records" / "audit" / "supervisor_actions").glob("*/*.json"))
    updated_review = _read_json(review_path)

    assert result["status"] == "completed"
    assert structured_path.exists()
    assert len(audit_paths) == 1
    assert updated_review["resolution_status"] == "accepted_after_review"
    assert updated_review["acceptance"]["decision"] == "accept"


def test_reject_review_item_writes_rejected_copy_and_audit(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    source_path = tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-23__waigani__sales.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("DAY-END SALES REPORT\nBranch: Waigani", encoding="utf-8")
    review_path = _write_review_item(
        tmp_path,
        record_id="hash-reject",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-23",
            "resolution_status": "open",
            "provenance": {"source_record_path": str(source_path)},
        },
    )

    result = worker.process_supervisor_action(
        {
            "command_name": "reject",
            "record_id": "hash-reject",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    rejected_paths = sorted((tmp_path / "records" / "rejected" / "whatsapp" / "sales").glob("*.txt"))
    updated_review = _read_json(review_path)

    assert result["status"] == "completed"
    assert len(rejected_paths) == 1
    assert updated_review["resolution_status"] == "rejected_after_review"
    assert updated_review["acceptance"]["decision"] == "reject"


def test_replay_review_item_invokes_safe_replay_and_audits(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    source_path = tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-23__waigani__sales.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("DAY-END SALES REPORT\nBranch: Waigani", encoding="utf-8")
    _write_review_item(
        tmp_path,
        record_id="hash-replay",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-23",
            "resolution_status": "open",
            "provenance": {"source_record_path": str(source_path)},
        },
    )
    captured = {}

    def fake_replay_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(worker.replay_records, "main", fake_replay_main)

    result = worker.process_supervisor_action(
        {
            "command_name": "replay",
            "record_id": "hash-replay",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "completed"
    assert captured["argv"] == [
        "--source",
        "raw",
        "--mode",
        "orchestrator",
        "--path",
        str(source_path),
    ]


def test_unauthorized_supervisor_is_denied_and_audited(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    _write_review_item(
        tmp_path,
        record_id="hash-auth",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-23",
            "resolution_status": "open",
        },
    )

    result = worker.process_supervisor_action(
        {
            "command_name": "approve",
            "record_id": "hash-auth",
            "sender_phone": "67570000001",
        },
        output_root=tmp_path,
    )

    audit_paths = sorted((tmp_path / "records" / "audit" / "supervisor_actions").glob("*/*.json"))

    assert result["status"] == "unauthorized"
    assert len(audit_paths) == 1
    assert _read_json(audit_paths[0])["authorized"] is False


def test_missing_review_item_fails_safely(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)

    result = worker.process_supervisor_action(
        {
            "command_name": "approve",
            "record_id": "missing-record",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "failed"
    assert result["reason"] == "review_item_not_found"


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")


def _patch_supervisors(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config" / "supervisors.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        '{"supervisors":[{"name":"Alice","sender_phone":"67570000000","branches":["waigani"]}]}',
        encoding="utf-8",
    )
    import apps.supervisor_auth.worker as supervisor_auth_worker

    monkeypatch.setattr(supervisor_auth_worker, "_CONFIG_PATH", config_path)
    supervisor_auth_worker._load_supervisors_cached.cache_clear()


def _write_review_item(tmp_path: Path, *, record_id: str, payload: dict[str, object]) -> Path:
    review_path = tmp_path / "records" / "review" / "2026_04_23" / "waigani" / "sales" / f"{record_id}.json"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return review_path


def _write_raw_meta(tmp_path: Path) -> Path:
    path = tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-23__waigani__sales.meta.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"message_id": "wamid.1", "raw_sha256": "raw-hash-1"}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
