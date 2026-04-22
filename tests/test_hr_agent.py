"""Focused tests for the HR agent."""

from __future__ import annotations

import json
from pathlib import Path

from apps.hr_agent.parser import parse_work_item as parse_hr_work_item
from apps.hr_agent.worker import process_work_item
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from packages.signal_contracts.work_item import WorkItem


def test_valid_attendance_sample_writes_one_signal_file(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Mary Kila - Present",
                "Peter Ake - Present",
                "Lena Bina - Present",
                "Notes: Fully staffed",
            ]
        )
    )

    assert result.payload["status"] == "accepted"
    outbox_files = sorted(outbox_path.glob("*.json"))
    assert len(outbox_files) == 1
    event_path = signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json"
    assert event_path.exists()

    payload = json.loads(event_path.read_text(encoding="utf-8"))
    assert payload["signal_type"] == "staff_attendance_report"
    assert payload["branch"] == "waigani"
    assert payload["report_date"] == "2026-04-07"
    assert payload["source_record_type"] == "hr_attendance"
    assert payload["payload"]["attendance_totals"]["present"] == 4
    assert len(payload["payload"]["attendance_records"]) == 4
    assert payload["warnings"] == []

    assert json.loads(outbox_files[0].read_text(encoding="utf-8")) == result.payload


def test_low_coverage_sample_raises_low_coverage(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Mary Kila - Present",
                "Peter Ake - Absent",
                "Lena Bina - Absent",
                "Total Staff: 4",
                "Notes: Skeleton team only",
            ]
        )
    )

    assert result.payload["status"] == "needs_review"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "low_coverage" in warning_codes
    assert "unknown_attendance_status" not in warning_codes
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "structured" / "hr_attendance" / "waigani" / "2026-04-07.json").exists()
    assert not (signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json").exists()


def test_unknown_status_sample_raises_unknown_attendance_status(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Peter Ake - Present",
                "Lena Bina - Present",
                "Mary Kila - Standby",
                "Notes: Skeleton team only",
            ]
        )
    )

    assert result.payload["status"] == "needs_review"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "unknown_attendance_status" in warning_codes
    assert "low_coverage" not in warning_codes
    assert any(item["staff_name"] == "Mary Kila" and item["status"] == "unknown" for item in result.payload["items"])
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "structured" / "hr_attendance" / "waigani" / "2026-04-07.json").exists()
    assert not (signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json").exists()


def test_hr_agent_normalizes_branch_alias_date_and_short_statuses(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Shop: TTC LAE 5TH STREET BRANCH",
                "Date: Friday, 10/04 /26",
                "John Doe - P",
                "Mary Kila - P",
                "Peter Ake - Off",
                "Lena Bina - Leave",
                "Total Staff: (4)",
            ]
        )
    )

    assert result.payload["status"] == "needs_review"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "unknown_attendance_status" not in warning_codes
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-10"
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "structured" / "hr_attendance" / "lae_5th_street" / "2026-04-10.json").exists()
    assert not (signals_root / "lae_5th_street" / "2026-04-10" / "staff_attendance_report__lae_5th_street__2026-04-10.json").exists()


def test_hr_parser_ignores_headers_and_summary_lines_in_numbered_attendance_format() -> None:
    parsed = parse_hr_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "staff_attendance"},
                "raw_message": {"text": "\n".join(_numbered_attendance_lines())},
            },
        )
    )

    staff_names = {record.staff_name for record in parsed.records}

    assert parsed.report_date == "2026-03-11"
    assert len(parsed.records) == 27
    assert parsed.records[0].staff_name == "Erone Bana"
    assert parsed.records[-1].staff_name == "Rhoda Frank"
    assert all(record.status == "present" for record in parsed.records)
    assert staff_names.isdisjoint({"Saturday 11", "Saturday 11/03/26", "28", "Not at work", "0"})
    assert parsed.declared_total_staff == 27
    assert parsed.declared_summary_metrics == {
        "total_staff": 27,
        "staff_present": 28,
        "not_at_work": 0,
        "staff_off": 0,
        "suspend": 0,
        "absent": 0,
        "leave": 0,
        "sick": 0,
    }
    assert any("piso is also working" in note.casefold() for note in parsed.notes)


def _patch_output_paths(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    signals_root = colony_root / "SIGNALS" / "normalized"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    monkeypatch.setattr("apps.hr_agent.worker.OUTBOX_PATH", outbox_path)
    return signals_root, outbox_path


def _attendance_work_item(
    *,
    total_staff: str = "4",
    lines: list[str] | None = None,
) -> WorkItem:
    report_lines = lines or [
        "Branch: Waigani Branch",
        "Date: 07/04/2026",
        "John Doe - Present",
        "Mary Kila - Present",
        "Peter Ake - Off",
        "Lena Bina - Leave",
        f"Total Staff: {total_staff}",
        "Notes: Skeleton team only",
    ]
    return WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "staff_attendance"},
            "raw_message": {"text": "\n".join(report_lines)},
        },
    )


def _numbered_attendance_lines() -> list[str]:
    return [
        "TOP TOWN CLOTHING",
        "LAE MARKET BRANCH",
        "MALAITA STREET",
        "",
        "STAFF ATTENDANCE",
        "",
        "Saturday 11/03/26",
        "",
        "1.Erone Bana = P",
        "2.Renate Norman = P",
        "3.Maria Sine = P",
        "4.Peter Ake = P",
        "5.Lena Bina = P",
        "6.John Doe = P",
        "7.Mary Kila = P",
        "8.Timothy Kale = P",
        "9.Sarah Namo = P",
        "10.Ben Kora = P",
        "11.Lucy Wane = P",
        "12.Paula Sore = P",
        "13.Daniel Taka = P",
        "14.Grace Lari = P",
        "15.Nim Jonnah = P",
        "16.Ricky Lomu = P",
        "17.Sabila Seka = P",
        "18.Debra Wotavo = P",
        "19.Kimson David = P",
        "20.Hendry Ambiu = P",
        "21.Francis Ano = P",
        "22.Bethsien Ken = P",
        "23.Stanly Mathias = P",
        "24.Anita Tangoi = P",
        "25.Moviyo Alex = P",
        "26.Xeena Moris = P",
        "27.Rhoda Frank = P",
        "",
        "Note: Please note piso is also working, her attendance is recorded in record book.",
        "",
        "Total staff = 27",
        "Staff present = 28",
        "Not at work = 0",
        "Staff off = 0",
        "Suspend = 0",
        "Absent: 0",
        "Leave : 0",
        "Sick : 0",
        "",
        "Thankyou.",
    ]
