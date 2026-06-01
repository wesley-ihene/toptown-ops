"""Targeted duplicate outbound ack regressions."""

from __future__ import annotations

import json
from pathlib import Path

from analytics.phase3 import get_branch_daily_analytics_path
import packages.record_store.paths as record_paths
from packages.record_store.automation import generate_whatsapp_conversation_reply
from packages.record_store.writer import write_governed_structured, write_json_file
from packages.signal_contracts.agent_result import AgentResult


def test_duplicate_raw_sha256_generates_duplicate_ack_response(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    reply = generate_whatsapp_conversation_reply(
        outcome=AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "duplicate",
                "governance": {
                    "status": "duplicate",
                    "reasons": ["duplicate_raw_sha256"],
                },
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        ),
        source_message_id="wamid.duplicate-raw-sha-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
        mode="write_only",
    )

    assert reply is not None
    assert reply["response_type"] == "duplicate_ack"

    artifact_payload = _read_json(Path(reply["json_path"]))
    assert artifact_payload["response_type"] == "duplicate_ack"
    assert artifact_payload["governance_status"] == "duplicate"
    assert artifact_payload["reason"] == "duplicate_raw_sha256"
    assert artifact_payload["response_text"] == "\n".join(
        [
            "⚠️ DUPLICATE REPORT DETECTED",
            "Branch: WAIGANI",
            "Date: 07/04/26",
            "Type: Day-End Sales Report",
            "This report was already received. No new record was created.",
        ]
    )


def test_duplicate_raw_sha256_skips_structured_write_and_normal_analytics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
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
    current_raw_meta = (
        tmp_path
        / "records"
        / "raw"
        / "whatsapp"
        / "unknown"
        / "2026-04-07__waigani__current.meta.json"
    )

    result = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {
                "message_id": "wamid.duplicate-raw-sha-2",
                "sender_phone": "67570000000",
                "raw_sha256": "same-raw",
                "raw_txt_path": str(tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "duplicate.txt"),
                "raw_meta_path": str(current_raw_meta),
                "classified_report_type": "sales",
            },
        },
        root=tmp_path,
        colony_root=colony_root,
    )

    duplicate_paths = sorted((tmp_path / "records" / "duplicates" / "whatsapp").glob("*/*.json"))
    branch_daily_path = get_branch_daily_analytics_path("waigani", "2026-04-07", output_root=tmp_path)

    assert result.persisted is False
    assert result.governance.status == "duplicate"
    assert result.governance.reasons == ["duplicate_raw_sha256"]
    assert not result.path.exists()
    assert not branch_daily_path.exists()
    assert len(duplicate_paths) == 1

    archive_payload = _read_json(duplicate_paths[0])
    archive_date = duplicate_paths[0].parent.name
    duplicate_branch_analytics = (
        tmp_path / "analytics" / "duplicates" / "branch_daily" / "waigani" / f"{archive_date}.json"
    )

    assert archive_payload["duplicate_reason"] == "duplicate_raw_sha256"
    assert archive_payload["report_type"] == "sales"
    assert duplicate_branch_analytics.exists()


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
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


def _sales_payload() -> dict[str, object]:
    return {
        "status": "accepted",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "metrics": {
            "gross_sales": 1200.0,
            "cash_sales": 600.0,
            "eftpos_sales": 600.0,
            "traffic": 12,
            "served": 9,
        },
        "items": [],
        "warnings": [],
    }


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
