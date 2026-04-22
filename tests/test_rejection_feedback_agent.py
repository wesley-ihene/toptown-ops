"""Focused tests for rejection feedback generation and record writing."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.rejection_feedback_agent.formatter import format_feedback_message
from apps.rejection_feedback_agent.record_store import write_feedback_record
from apps.rejection_feedback_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def test_format_feedback_message_includes_rejection_details() -> None:
    message = format_feedback_message(
        report_type="sales_income",
        branch="waigani",
        report_date="2026-04-07",
        rejections=[
            {
                "code": "invalid_totals",
                "field": "metrics.gross_sales",
                "message": "Gross sales must match the sum of payment totals.",
            }
        ],
    )

    assert "TOPTOWN OPS REJECTION FEEDBACK" in message
    assert "Report Type: Sales Income" in message
    assert "Branch: Waigani" in message
    assert "Report Date: 2026-04-07" in message
    assert "1. invalid_totals [metrics.gross_sales]: Gross sales must match the sum of payment totals." in message
    assert message.endswith("Please resend one corrected report only.")


def test_write_feedback_record_writes_json_and_whatsapp_preview(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    paths = write_feedback_record(
        report_type="sales",
        channel="whatsapp",
        feedback_message="Fix the totals and resend.",
        payload={
            "agent": "rejection_feedback_agent",
            "report_type": "sales",
            "status": "accepted",
        },
        dry_run=False,
    )

    json_path = Path(paths["json_path"])
    preview_path = Path(paths["whatsapp_preview_path"])

    assert json_path.exists()
    assert preview_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["report_type"] == "sales"
    assert preview_path.read_text(encoding="utf-8") == "Fix the totals and resend.\n"


def test_process_work_item_writes_json_only_in_whatsapp_dry_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="rejection_feedback",
            payload={
                "report_type": "sales",
                "branch": "waigani",
                "report_date": "2026-04-07",
                "channel": "whatsapp",
                "dry_run": True,
                "source_record_path": "records/rejected/whatsapp/sales/example.txt",
                "rejections": [
                    {
                        "code": "invalid_totals",
                        "field": "metrics.gross_sales",
                        "message": "Gross sales must match the sum of payment totals.",
                    },
                    {
                        "code": "invalid_numeric_value",
                        "field": "metrics.served",
                        "message": "Served customer count cannot exceed traffic count.",
                    },
                ],
            },
        )
    )

    assert result.agent_name == "rejection_feedback_agent"
    assert result.payload["status"] == "accepted"
    assert result.payload["delivery"]["channel"] == "whatsapp"
    assert result.payload["delivery"]["dry_run"] is True
    assert result.payload["delivery"]["dispatch_status"] == "dry_run"
    assert result.payload["delivery"]["dispatched"] is False
    assert "invalid_totals" in result.payload["feedback_message"]
    assert result.payload["record"]["json_path"] is not None
    assert result.payload["record"]["whatsapp_preview_path"] is None

    json_path = Path(result.payload["record"]["json_path"])
    assert json_path.exists()
    stored_payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert stored_payload["dry_run"] is True
    assert stored_payload["source_record_path"] == "records/rejected/whatsapp/sales/example.txt"

    preview_paths = sorted((tmp_path / "records" / "rejected" / "whatsapp" / "feedback" / "sales").glob("*.whatsapp.txt"))
    assert preview_paths == []


def test_process_work_item_rejects_invalid_input_without_writing_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="rejection_feedback",
            payload={
                "report_type": "",
                "channel": "email",
                "dry_run": "yes",
                "rejections": [],
            },
        )
    )

    assert result.payload["status"] == "invalid_input"
    assert result.payload["record"]["json_path"] is None
    assert result.payload["record"]["whatsapp_preview_path"] is None
    assert len(result.payload["warnings"]) == 4
    assert not (tmp_path / "records").exists()


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
