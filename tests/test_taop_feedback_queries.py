"""Tests for deterministic TAOP operational query detection and responses."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from packages.taop_feedback import build_operational_query_response, detect_message_intent


def test_detect_message_intent_marks_attendance_status_query() -> None:
    intent = detect_message_intent("confirm you receive all four STAFFS ATTENDANCE report for the date 25/04/26?")

    assert intent["message_intent"] == "operational_query"
    assert intent["query_type"] == "attendance_receipt_status"
    assert intent["date"] == "2026-04-25"


def test_attendance_status_query_reports_partial_branch_coverage(tmp_path: Path) -> None:
    _write_record(tmp_path, "hr_attendance", "waigani", "2026-04-25")
    _write_record(tmp_path, "hr_attendance", "lae_5th_street", "2026-04-25")
    _write_record(tmp_path, "hr_attendance", "lae_malaita", "2026-04-25")

    response = build_operational_query_response(
        {
            "message_intent": "operational_query",
            "query_type": "attendance_receipt_status",
            "report_type": "attendance_status",
            "branch": None,
            "date": "2026-04-25",
        },
        tmp_path,
    )

    assert response["response_type"] == "operational_query_status"
    assert "STATUS: ⚠️ PARTIAL / NEEDS CHECK" in response["response_text"]
    assert "✔ Waigani" in response["response_text"]
    assert "✔ LAE 5th Street" in response["response_text"]
    assert "✔ LAE Malaita" in response["response_text"]
    assert "❌ Bena Road" in response["response_text"]


def test_attendance_status_query_reports_all_received(tmp_path: Path) -> None:
    for branch in ("waigani", "lae_5th_street", "lae_malaita", "bena_road"):
        _write_record(tmp_path, "hr_attendance", branch, "2026-04-25")

    response = build_operational_query_response(
        {
            "message_intent": "operational_query",
            "query_type": "attendance_receipt_status",
            "report_type": "attendance_status",
            "branch": None,
            "date": "2026-04-25",
        },
        tmp_path,
    )

    assert "STATUS: ✅ ALL RECEIVED" in response["response_text"]
    assert "No further action required." in response["response_text"]


def test_attendance_status_query_marks_duplicate_submission_as_received(tmp_path: Path) -> None:
    _write_duplicate_meta(
        tmp_path,
        branch="waigani",
        report_date="2026-04-25",
        report_type="attendance",
    )

    response = build_operational_query_response(
        {
            "message_intent": "operational_query",
            "query_type": "attendance_receipt_status",
            "report_type": "attendance_status",
            "branch": "waigani",
            "date": "2026-04-25",
        },
        tmp_path,
    )

    assert "STATUS: ✅ RECEIVED" in response["response_text"]
    assert "✔ Waigani" in response["response_text"]
    assert "MISSING / NOT FOUND" not in response["response_text"]


def test_attendance_status_query_reports_missing_branch_when_no_record_exists(tmp_path: Path) -> None:
    response = build_operational_query_response(
        {
            "message_intent": "operational_query",
            "query_type": "attendance_receipt_status",
            "report_type": "attendance_status",
            "branch": "bena_road",
            "date": "2026-04-25",
        },
        tmp_path,
    )

    assert "STATUS: ❌ NOT FOUND" in response["response_text"]
    assert "❌ Bena Road" in response["response_text"]


def test_attendance_status_query_normalizes_branch_alias_and_date(tmp_path: Path) -> None:
    _write_record(tmp_path, "hr_attendance", "lae_5th_street", "2026-04-25")

    response = build_operational_query_response(
        {
            "message_intent": "operational_query",
            "query_type": "attendance_receipt_status",
            "report_type": "attendance_status",
            "branch": "LAE 5th Street",
            "date": "25/04/26",
        },
        tmp_path,
    )

    assert response["branch"] == "lae_5th_street"
    assert response["report_date"] == "2026-04-25"
    assert "STATUS: ✅ RECEIVED" in response["response_text"]
    assert "✔ LAE 5th Street" in response["response_text"]


def test_attendance_status_query_logs_found_and_missing_branches(tmp_path: Path, caplog) -> None:
    _write_record(tmp_path, "hr_attendance", "waigani", "2026-04-25")

    with caplog.at_level(logging.INFO, logger="packages.taop_feedback.queries"):
        build_operational_query_response(
            {
                "message_intent": "operational_query",
                "query_type": "attendance_receipt_status",
                "report_type": "attendance_status",
                "branch": None,
                "date": "2026-04-25",
            },
            tmp_path,
        )

    payload = json.loads(caplog.records[-1].message)
    assert payload == {
        "query_type": "attendance_status",
        "date": "2026-04-25",
        "found_branches": ["waigani"],
        "missing_branches": ["lae_5th_street", "lae_malaita", "bena_road"],
    }


def _write_record(root: Path, signal_type: str, branch: str, report_date: str) -> None:
    path = root / "records" / "structured" / signal_type / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"branch": branch, "report_date": report_date}, indent=2) + "\n", encoding="utf-8")


def _write_duplicate_meta(root: Path, *, branch: str, report_date: str, report_type: str) -> None:
    path = root / "records" / "raw" / "whatsapp" / "unknown" / f"{report_date}__{branch}__duplicate.meta.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "processing_status": "duplicate",
                "branch_hint": branch,
                "resolved_report_date": report_date,
                "normalized_report_date": report_date,
                "detected_report_type": report_type,
                "specialist_report_type": report_type,
                "policy_guard": {
                    "duplicate": True,
                    "reason": "duplicate_message",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
