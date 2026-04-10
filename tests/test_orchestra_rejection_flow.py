"""Phase 3 Orchestra routing tests for rejection feedback integration."""

from __future__ import annotations

import json
from pathlib import Path

from apps.orchestra.classifier import classify_work_item
from apps.orchestra.intake import intake_raw_message
from apps.orchestra.router import route_work_item
import packages.record_store.paths as record_paths


def test_valid_report_routes_normally(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    work_item = intake_raw_message(
        {
            "text": "DAY-END SALES REPORT\nGross Sales: 1200",
            "branch": "TTC Waigani Branch",
            "report_date": "07/04/2026",
            "confidence": 0.95,
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 700.0,
                "eftpos_sales": 500.0,
                "traffic": 12,
                "served": 10,
            },
        },
        received_at_utc="2026-04-07T00:00:00+00:00",
    )
    classify_work_item(work_item)

    result = route_work_item(work_item)

    assert result.destination == "income_agent"
    assert result.route_status == "routed"
    assert work_item.payload["routing"]["route_reason"] == "sales_reports_route_to_income_agent"
    assert work_item.payload["validation"]["accepted"] is True
    assert work_item.payload["normalized_report"]["branch"] == "waigani"
    assert work_item.payload["normalized_report"]["report_date"] == "2026-04-07"
    assert work_item.payload["acceptance"]["decision"] == "accept"
    assert "review_queue" not in work_item.payload
    assert not (tmp_path / "records" / "review").exists()


def test_medium_confidence_report_routes_to_review_after_validation_passes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    work_item = intake_raw_message(
        {
            "text": "DAY-END SALES REPORT\nGross Sales: 1200",
            "branch": "Waigani Branch",
            "report_date": "07/04/2026",
            "confidence": 0.65,
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 700.0,
                "eftpos_sales": 500.0,
                "traffic": 12,
                "served": 10,
            },
        },
        received_at_utc="2026-04-07T00:00:00+00:00",
    )
    classify_work_item(work_item)

    result = route_work_item(work_item)

    assert result.destination == "rejection_feedback_agent"
    assert result.route_status == "review"
    assert work_item.payload["validation"]["accepted"] is True
    assert work_item.payload["acceptance"]["decision"] == "review"
    assert work_item.payload["acceptance"]["reason"] == "confidence_between_review_and_accept_thresholds"
    assert work_item.payload["review_queue"]["reason"] == "confidence_between_review_and_accept_thresholds"
    assert work_item.payload["rejection_feedback"]["rejections"] == [
        {
            "code": "needs_review",
            "message": "Validation passed but the payload confidence requires manual review.",
        }
    ]
    review_path = Path(work_item.payload["review_queue"]["path"])
    assert review_path == (
        tmp_path
        / "records"
        / "review"
        / "2026_04_07"
        / "waigani"
        / "sales"
        / work_item.payload["message_hash"]
    ).with_suffix(".json")
    review_payload = json.loads(review_path.read_text(encoding="utf-8"))
    assert review_payload["report_type"] == "sales"
    assert review_payload["branch"] == "waigani"
    assert review_payload["date"] == "2026-04-07"
    assert review_payload["confidence"] == 0.65
    assert review_payload["reason"] == "confidence_between_review_and_accept_thresholds"
    assert review_payload["warnings"] == work_item.payload["rejection_feedback"]["rejections"]
    assert review_payload["provenance"]["source_message_hash"] == work_item.payload["message_hash"]
    review_provenance_path = (
        tmp_path
        / "records"
        / "provenance"
        / "review"
        / "2026_04_07"
        / "waigani"
        / "sales"
        / f"{work_item.payload['message_hash']}.json"
    )
    assert review_provenance_path.exists()
    review_provenance = json.loads(review_provenance_path.read_text(encoding="utf-8"))
    assert review_provenance["parser_used"] == "orchestra_router"
    assert review_provenance["parse_mode"] == "strict"
    assert review_provenance["validation_outcome"]["accepted"] is True
    assert review_provenance["acceptance_outcome"]["decision"] == "review"
    assert review_provenance["downstream_references"]["review_queue_path"] == str(review_path)


def test_invalid_report_triggers_rejection_feedback_agent(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    work_item = intake_raw_message(
        {
            "text": "DAY-END SALES REPORT\nGross Sales: 1200",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 600.0,
                "eftpos_sales": 500.0,
                "mobile_money_sales": 50.0,
                "traffic": 10,
                "served": 12,
            },
        },
        received_at_utc="2026-04-07T00:00:00+00:00",
    )
    classify_work_item(work_item)

    result = route_work_item(work_item)

    assert result.destination == "rejection_feedback_agent"
    assert result.route_status == "rejected"
    assert result.work_item is work_item
    assert work_item.kind == "raw_message"
    assert work_item.payload["validation"]["accepted"] is False
    assert work_item.payload["acceptance"]["decision"] == "reject"
    assert work_item.payload["rejection_feedback"]["report_type"] == "sales"
    assert work_item.payload["rejection_feedback"]["channel"] == "whatsapp"
    assert work_item.payload["rejection_feedback"]["dry_run"] is True
    assert [entry["code"] for entry in work_item.payload["rejection_feedback"]["rejections"]] == [
        "invalid_totals",
        "invalid_numeric_value",
    ]
    assert "review_queue" not in work_item.payload
    assert not (tmp_path / "records" / "review").exists()


def test_mixed_report_triggers_split_feedback_agent(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    work_item = intake_raw_message(
        {
            "text": (
                "Sales:\n"
                "Revenue 100\n"
                "Cash up complete\n"
                "Supervisor Control:\n"
                "Checklist approved\n"
                "Inspection complete"
            )
        },
        received_at_utc="2026-04-07T00:00:00+00:00",
    )
    classify_work_item(work_item)

    result = route_work_item(work_item)

    assert result.destination == "rejection_feedback_agent"
    assert result.route_status == "needs_split"
    assert result.work_item is work_item
    assert work_item.kind == "raw_message"
    assert work_item.payload["rejection_feedback"]["report_type"] == "mixed"
    assert work_item.payload["rejection_feedback"]["dry_run"] is True
    assert work_item.payload["rejection_feedback"]["rejections"] == [
        {
            "code": "needs_split",
            "message": "Mixed reports must be split before specialist routing.",
        }
    ]
    assert not (tmp_path / "records" / "review").exists()


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
