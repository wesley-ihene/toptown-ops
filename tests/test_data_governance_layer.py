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


def test_governance_accepts_supervisor_control_as_intelligence(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    result = write_governed_structured(
        "supervisor_control",
        "waigani",
        "2026-04-07",
        _supervisor_payload(status="needs_review"),
        metadata={
            "validation": {"accepted": False, "status": "rejected"},
            "acceptance": {"decision": "reject", "reason": "validation_failed"},
            "governance_context": {
                "message_id": "wamid.intel-1",
                "raw_sha256": "raw-intel-1",
                "classified_report_type": "supervisor_control",
            },
        },
        root=tmp_path,
        colony_root=colony_root,
    )

    assert result.persisted is True
    assert result.path == tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json"
    assert result.governance.status == "accepted"
    assert result.governance.export_allowed is True
    assert result.governance.report_family == "intelligence"

    manifest = export_colony_signals.export_one_record_type(
        "supervisor_control",
        "waigani",
        "2026-04-07",
        source_root=tmp_path,
        colony_root=colony_root,
        overwrite=True,
    )
    assert manifest["results"][0]["status"] == "written"
    assert manifest["results"][0]["source_path"] == "records/intelligence/supervisor_control/2026-04-07/waigani.json"


def test_governance_allows_bale_replacement_same_scope_with_audit_history(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    initial = write_governed_structured(
        "pricing_stock_release",
        "waigani",
        "2026-04-07",
        _pricing_payload(status="accepted", report_date="2026-04-07", total_qty=100, total_amount=500.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {"message_id": "wamid.bale-1", "raw_sha256": "raw-bale-1"},
        },
        root=tmp_path,
        colony_root=colony_root,
    )
    replacement = write_governed_structured(
        "pricing_stock_release",
        "waigani",
        "2026-04-07",
        _pricing_payload(status="accepted", report_date="2026-04-07", total_qty=120, total_amount=720.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {
                "message_id": "wamid.bale-2",
                "raw_sha256": "raw-bale-2",
                "correction_intent_detected": True,
                "allow_same_scope_supersede": True,
                "replacement_reason": "correction_replacement_report",
            },
        },
        root=tmp_path,
        colony_root=colony_root,
    )

    active_payload = json.loads(replacement.path.read_text(encoding="utf-8"))
    audit_payload = json.loads(Path(str(active_payload["supersedes_record_path"])).read_text(encoding="utf-8"))

    assert initial.persisted is True
    assert replacement.persisted is True
    assert replacement.governance.status == "accepted"
    assert active_payload["metrics"]["total_qty"] == 120
    assert active_payload["replacement_source_message_id"] == "wamid.bale-2"
    assert audit_payload["metrics"]["total_qty"] == 100
    assert audit_payload["superseded"] is True


def test_governance_does_not_persist_non_exportable_bale_replacement(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(tmp_path, monkeypatch)
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    initial = write_governed_structured(
        "pricing_stock_release",
        "waigani",
        "2026-04-08",
        _pricing_payload(status="accepted", report_date="2026-04-08", total_qty=100, total_amount=500.0),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {"message_id": "wamid.bale-3", "raw_sha256": "raw-bale-3"},
        },
        root=tmp_path,
        colony_root=colony_root,
    )
    blocked = write_governed_structured(
        "pricing_stock_release",
        "waigani",
        "2026-04-08",
        _pricing_payload(
            status="needs_review",
            report_date="2026-04-08",
            total_qty=120,
            total_amount=720.0,
            warnings=[{"code": "data_mismatch", "severity": "warning", "message": "totals mismatch"}],
        ),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {
                "message_id": "wamid.bale-4",
                "raw_sha256": "raw-bale-4",
                "correction_intent_detected": True,
                "allow_same_scope_supersede": True,
                "replacement_reason": "correction_replacement_report",
            },
        },
        root=tmp_path,
        colony_root=colony_root,
    )

    active_payload = json.loads(initial.path.read_text(encoding="utf-8"))

    assert blocked.persisted is False
    assert blocked.governance.status == "needs_review"
    assert active_payload["metrics"]["total_qty"] == 100
    assert "supersedes_record_path" not in active_payload


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
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "DUPLICATES_DIR", records_dir / "duplicates" / "whatsapp")
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


def _pricing_payload(
    *,
    status: str,
    report_date: str,
    total_qty: int,
    total_amount: float,
    warnings: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "signal_type": "pricing_stock_release",
        "source_agent": "pricing_stock_release_agent",
        "branch": "waigani",
        "report_date": report_date,
        "confidence": 0.95,
        "metrics": {
            "bales_processed": 1,
            "bales_released": 1,
            "bales_pending_approval": 0,
            "total_qty": total_qty,
            "total_amount": total_amount,
            "release_ratio": 1.0,
        },
        "items": [
            {
                "bale_id": "1",
                "item_name": "OSH",
                "qty": total_qty,
                "amount": total_amount,
                "price_per_piece": round(total_amount / total_qty, 2),
            }
        ],
        "provenance": {"prepared_by": "Maria", "checked_by": "Peter"},
        "warnings": warnings or [],
        "status": status,
    }


def _supervisor_payload(*, status: str) -> dict[str, object]:
    return {
        "signal_type": "supervisor_control",
        "source_agent": "supervisor_control_agent",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "metrics": {
            "exception_count": 1,
            "open_exception_count": 1,
            "escalated_count": 1,
            "confirmed_count": 0,
            "control_gap_count": 1,
        },
        "items": [
            {
                "exception_type": "STAFF_ISSUE",
                "details": "Late opening",
                "action_taken": "Escalated",
                "supervisor_confirmed": "NO",
            }
        ],
        "warnings": [
            {"code": "missing_confirmation"},
            {"code": "escalation_required"},
        ],
        "status": status,
    }
