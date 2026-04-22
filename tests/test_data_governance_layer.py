"""Focused tests for the governed structured-record layer."""

from __future__ import annotations

import json
from pathlib import Path

from apps.orchestrator_agent.worker import process_work_item
import packages.record_store.paths as record_paths
from packages.record_store.writer import write_governed_structured, write_json_file
from packages.signal_contracts.work_item import WorkItem
from scripts import export_colony_signals


def test_governance_rejects_duplicate_message_id(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)
    existing_raw_meta = (
        tmp_path
        / "records"
        / "raw"
        / "whatsapp"
        / "unknown"
        / "2026-04-07__waigani__seed.meta.json"
    )
    write_json_file(
        existing_raw_meta,
        {
            "message_id": "wamid.dup-1",
            "raw_sha256": "seed-a",
            "processing_status": "processed",
        },
    )

    result = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(status="accepted", gross_sales=1200.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {
                "message_id": "wamid.dup-1",
                "raw_sha256": "seed-b",
            },
        },
        root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    assert result.persisted is False
    assert result.governance.status == "duplicate"
    assert result.governance.reasons == ["duplicate_message_id"]
    assert not result.path.exists()


def test_governance_rejects_duplicate_raw_sha256(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)
    existing_raw_meta = (
        tmp_path
        / "records"
        / "raw"
        / "whatsapp"
        / "unknown"
        / "2026-04-07__waigani__seed.meta.json"
    )
    write_json_file(
        existing_raw_meta,
        {
            "message_id": "wamid.other",
            "raw_sha256": "same-raw",
            "processing_status": "processed",
        },
    )

    result = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(status="accepted", gross_sales=1200.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {
                "message_id": "wamid.new",
                "raw_sha256": "same-raw",
            },
        },
        root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    assert result.persisted is False
    assert result.governance.status == "duplicate"
    assert result.governance.reasons == ["duplicate_raw_sha256"]
    assert not result.path.exists()


def test_governance_detects_semantic_duplicate_and_conflict(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    first = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(status="accepted", gross_sales=1200.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {"message_id": "wamid.1", "raw_sha256": "raw-1"},
        },
        root=tmp_path,
        colony_root=colony_root,
    )
    assert first.persisted is True
    assert first.governance.status == "accepted"

    duplicate = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(status="accepted", gross_sales=1200.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {"message_id": "wamid.2", "raw_sha256": "raw-2"},
        },
        root=tmp_path,
        colony_root=colony_root,
    )
    assert duplicate.persisted is False
    assert duplicate.governance.status == "duplicate"
    assert duplicate.governance.reasons == ["duplicate_semantic"]

    conflict = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(status="accepted", gross_sales=1800.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {"message_id": "wamid.3", "raw_sha256": "raw-3"},
        },
        root=tmp_path,
        colony_root=colony_root,
    )
    assert conflict.persisted is False
    assert conflict.governance.status == "conflict_blocked"
    assert conflict.governance.reasons == ["conflicting_record_same_scope"]

    stored = json.loads(first.path.read_text(encoding="utf-8"))
    assert stored["metrics"]["gross_sales"] == 1200.0


def test_governance_blocks_export_for_needs_review_record(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    result = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(status="needs_review", gross_sales=1200.0, warnings=[{"code": "invalid_totals"}]),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {"message_id": "wamid.review", "raw_sha256": "raw-review"},
        },
        root=tmp_path,
        colony_root=colony_root,
    )

    assert result.persisted is True
    assert result.governance.status == "needs_review"
    manifest = export_colony_signals.export_one_record_type(
        "sales_income",
        "waigani",
        "2026-04-07",
        source_root=tmp_path,
        colony_root=colony_root,
        overwrite=True,
    )
    assert manifest["results"][0]["status"] == "skipped"
    assert manifest["results"][0]["reason"] == "needs_review"


def test_invalid_pricing_card_message_is_rejected_with_explicit_reason(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "BALE # 1",
                            "Item: Jeans Mix",
                            "WT: 40KG",
                            "A: 5.00",
                            "B: 3.00",
                            "C: 2.00",
                            "Sales: K200",
                            "Pricer: Maria",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-07T09:00:00Z",
                    "sender": "pricing-card-smoke",
                },
            },
        )
    )

    assert result.payload["status"] == "rejected"
    assert result.payload["governance"]["reasons"] == ["invalid_pricing_card_format"]


def _patch_record_paths(tmp_path: Path, monkeypatch) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")


def _sales_payload(*, status: str, gross_sales: float, warnings: list[dict[str, str]] | None = None) -> dict[str, object]:
    return {
        "signal_type": "sales_income",
        "source_agent": "sales_income_agent",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "confidence": 0.95,
        "metrics": {
            "gross_sales": gross_sales,
            "cash_sales": gross_sales / 2,
            "eftpos_sales": gross_sales / 2,
            "traffic": 12,
            "served": 9,
        },
        "items": [],
        "provenance": {"cashier": "Alice"},
        "warnings": warnings or [],
        "status": status,
    }
