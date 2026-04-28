"""Tests for duplicate analytics and explicit disposal automation."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import packages.record_store.automation as record_automation
from scripts.analytics.duplicate_analytics import generate_duplicate_analytics
from scripts.cleanup.duplicate_disposal import dispose_duplicate_archives


def test_duplicate_records_generate_branch_daily_analytics(tmp_path: Path) -> None:
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T100000Z__sales__a",
        branch="Waigani Branch",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-27T10:00:00Z",
    )
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T103000Z__sales__b",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_semantic",
        created_at="2026-04-27T10:30:00Z",
    )

    result = generate_duplicate_analytics("2026-04-27", root=tmp_path)
    payload = _read_json(tmp_path / "analytics" / "duplicates" / "branch_daily" / "waigani" / "2026-04-27.json")

    assert result["status"] == "generated"
    assert payload["branch"] == "waigani"
    assert payload["date"] == "2026-04-27"
    assert payload["duplicates"] == 2
    assert payload["by_reason"] == {
        "duplicate_message": 1,
        "duplicate_semantic": 1,
    }
    assert payload["by_report_type"] == {"sales": 2}
    assert payload["top_senders"] == [
        {"sender_phone": "67570000000", "duplicates": 2},
    ]
    assert payload["generated_at"].endswith("Z")


def test_duplicate_records_generate_global_daily_analytics(tmp_path: Path) -> None:
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T100000Z__sales__a",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-27T10:00:00Z",
    )
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T103000Z__sales__b",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000001",
        duplicate_reason="duplicate_message",
        created_at="2026-04-27T10:30:00Z",
    )
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T110000Z__staff__c",
        branch="bena road",
        report_type="staff_attendance",
        sender_phone="67571111111",
        duplicate_reason="duplicate_raw_sha256",
        created_at="2026-04-27T11:00:00Z",
    )

    generate_duplicate_analytics("2026-04-27", root=tmp_path)
    payload = _read_json(tmp_path / "analytics" / "duplicates" / "global_daily" / "2026-04-27.json")

    assert payload["date"] == "2026-04-27"
    assert payload["total_duplicates"] == 3
    assert payload["branches"] == {
        "bena_road": 1,
        "waigani": 2,
    }
    assert payload["dominant_reason"] == "duplicate_message"
    assert payload["worst_branch"] == "waigani"
    assert payload["generated_at"].endswith("Z")


def test_duplicate_trends_update_safely(tmp_path: Path) -> None:
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-26",
        stem="20260426T100000Z__sales__a",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-26T10:00:00Z",
    )
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T100000Z__sales__b",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-27T10:00:00Z",
    )
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T110000Z__sales__c",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000001",
        duplicate_reason="duplicate_semantic",
        created_at="2026-04-27T11:00:00Z",
    )

    generate_duplicate_analytics("2026-04-26", root=tmp_path)
    generate_duplicate_analytics("2026-04-27", root=tmp_path)
    payload = _read_json(tmp_path / "analytics" / "duplicates" / "trends" / "waigani.json")

    assert payload["branch"] == "waigani"
    assert payload["trend"] == [
        {"date": "2026-04-26", "duplicates": 1},
        {"date": "2026-04-27", "duplicates": 2},
    ]
    assert payload["generated_at"].endswith("Z")


def test_duplicate_disposal_dry_run_deletes_nothing(tmp_path: Path, caplog) -> None:
    candidate = _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-10",
        stem="20260410T100000Z__sales__a",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-10T10:00:00Z",
    )

    caplog.set_level(logging.INFO, logger="scripts.cleanup.duplicate_disposal")
    result = dispose_duplicate_archives(
        root=tmp_path,
        keep_days=7,
        now="2026-04-20T12:00:00Z",
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["candidate_count"] == 1
    assert result["candidate_paths"] == [str(candidate)]
    assert result["deleted_count"] == 0
    assert candidate.exists()
    assert '"event": "duplicate_disposal_dry_run"' in caplog.text


def test_duplicate_disposal_apply_deletes_only_eligible_old_duplicate_files(tmp_path: Path, caplog) -> None:
    eligible = _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-10",
        stem="20260410T100000Z__sales__eligible",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-10T10:00:00Z",
    )
    recent = _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-18",
        stem="20260418T100000Z__sales__recent",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000001",
        duplicate_reason="duplicate_message",
        created_at="2026-04-18T10:00:00Z",
    )
    blocked = _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-10",
        stem="20260410T110000Z__sales__blocked",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000002",
        duplicate_reason="duplicate_message",
        created_at="2026-04-10T11:00:00Z",
        disposal_allowed=False,
    )

    caplog.set_level(logging.INFO, logger="scripts.cleanup.duplicate_disposal")
    result = dispose_duplicate_archives(
        root=tmp_path,
        keep_days=7,
        now="2026-04-20T12:00:00Z",
        dry_run=False,
    )

    assert result["status"] == "applied"
    assert result["deleted_count"] == 1
    assert result["deleted_paths"] == [str(eligible)]
    assert not eligible.exists()
    assert recent.exists()
    assert blocked.exists()
    assert '"event": "duplicate_disposed"' in caplog.text


def test_raw_structured_and_intelligence_records_are_never_touched(tmp_path: Path) -> None:
    raw_txt = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-10__waigani__sales.txt",
        "RAW AUDIT",
    )
    raw_meta = _write_json(
        tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-10__waigani__sales.meta.json",
        {"raw_txt_path": str(raw_txt)},
    )
    structured = _write_json(
        tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-10.json",
        {"branch": "waigani"},
    )
    intelligence = _write_json(
        tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-10" / "waigani.json",
        {"branch": "waigani"},
    )
    duplicate = _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-10",
        stem="20260410T100000Z__sales__eligible",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-10T10:00:00Z",
    )

    dispose_duplicate_archives(
        root=tmp_path,
        keep_days=7,
        now="2026-04-20T12:00:00Z",
        dry_run=False,
    )

    assert not duplicate.exists()
    assert raw_txt.exists()
    assert raw_meta.exists()
    assert structured.exists()
    assert intelligence.exists()


def test_missing_null_branch_report_type_and_sender_are_handled_safely(tmp_path: Path) -> None:
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T100000Z__unknown__a",
        branch=None,
        report_type=None,
        sender_phone=None,
        duplicate_reason="duplicate_message",
        created_at="2026-04-27T10:00:00Z",
    )

    generate_duplicate_analytics("2026-04-27", root=tmp_path)
    branch_daily = _read_json(tmp_path / "analytics" / "duplicates" / "branch_daily" / "unknown" / "2026-04-27.json")
    global_daily = _read_json(tmp_path / "analytics" / "duplicates" / "global_daily" / "2026-04-27.json")

    assert branch_daily["branch"] == "unknown"
    assert branch_daily["duplicates"] == 1
    assert branch_daily["by_reason"] == {"duplicate_message": 1}
    assert branch_daily["by_report_type"] == {"unknown": 1}
    assert branch_daily["top_senders"] == [{"sender_phone": "unknown", "duplicates": 1}]
    assert global_daily["branches"] == {"unknown": 1}
    assert global_daily["worst_branch"] == "unknown"


def test_invalid_json_duplicate_files_do_not_crash_analytics(tmp_path: Path) -> None:
    invalid_path = tmp_path / "records" / "duplicates" / "whatsapp" / "2026-04-27" / "broken.json"
    invalid_path.parent.mkdir(parents=True, exist_ok=True)
    invalid_path.write_text("{not valid json", encoding="utf-8")
    _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T100000Z__sales__valid",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-27T10:00:00Z",
    )

    result = generate_duplicate_analytics("2026-04-27", root=tmp_path)
    branch_daily = _read_json(tmp_path / "analytics" / "duplicates" / "branch_daily" / "waigani" / "2026-04-27.json")

    assert result["status"] == "generated"
    assert branch_daily["duplicates"] == 1


def test_duplicate_archive_automation_generates_analytics_without_auto_disposal(
    tmp_path: Path,
    caplog,
) -> None:
    expired = _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-10",
        stem="20260410T100000Z__sales__expired",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000000",
        duplicate_reason="duplicate_message",
        created_at="2026-04-10T10:00:00Z",
    )
    current = _write_duplicate_archive(
        tmp_path,
        archive_date="2026-04-27",
        stem="20260427T100000Z__sales__current",
        branch="waigani",
        report_type="sales",
        sender_phone="67570000001",
        duplicate_reason="duplicate_message",
        created_at="2026-04-27T10:00:00Z",
    )

    caplog.set_level(logging.INFO, logger=record_automation.__name__)
    result = record_automation.run_post_duplicate_archive_automation(
        archive_path=current,
        source_root=tmp_path,
        keep_days=7,
        now="2026-04-28T12:00:00Z",
    )

    assert result["analytics"]["status"] == "generated"
    assert result["disposal"]["status"] == "not_invoked"
    assert result["disposal"]["reason"] == "explicit_apply_required"
    assert expired.exists()
    assert (tmp_path / "analytics" / "duplicates" / "branch_daily" / "waigani" / "2026-04-27.json").exists()
    assert (tmp_path / "analytics" / "duplicates" / "global_daily" / "2026-04-27.json").exists()
    assert (tmp_path / "analytics" / "duplicates" / "trends" / "waigani.json").exists()
    assert '"event": "duplicate_analytics_generated"' in caplog.text
    assert '"event": "duplicate_disposal_skipped"' in caplog.text


def _write_duplicate_archive(
    tmp_path: Path,
    *,
    archive_date: str,
    stem: str,
    branch: str | None,
    report_type: str | None,
    sender_phone: str | None,
    duplicate_reason: str,
    created_at: str,
    disposal_allowed: bool = True,
) -> Path:
    return _write_json(
        tmp_path / "records" / "duplicates" / "whatsapp" / archive_date / f"{stem}.json",
        {
            "source_message_id": f"wamid.{stem}",
            "sender_phone": sender_phone,
            "branch": branch,
            "report_type": report_type,
            "report_date": archive_date,
            "raw_txt_path": None,
            "raw_meta_path": None,
            "duplicate_reason": duplicate_reason,
            "duplicate_basis": "policy_guard:passed",
            "original_or_duplicate_of": None,
            "disposal_status": "ready_for_disposal",
            "disposal_allowed": disposal_allowed,
            "created_at": created_at,
        },
    )


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
