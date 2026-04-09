"""Focused tests for the HR agent."""

from __future__ import annotations

import json
from pathlib import Path

from apps.hr_agent.worker import process_work_item
from packages.common import signal_writer
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

    assert result.payload["status"] == "ready"
    signal_files = sorted((signals_root / "waigani" / "2026-04-07").glob("*.json"))
    outbox_files = sorted(outbox_path.glob("*.json"))
    assert len(signal_files) == 1
    assert len(outbox_files) == 1

    payload = json.loads(signal_files[0].read_text(encoding="utf-8"))
    assert payload["signal_type"] == "hr_staffing"
    assert payload["branch"] == "waigani"
    assert payload["report_date"] == "2026-04-07"
    assert payload["metrics"]["total_staff_listed"] == 4
    assert payload["metrics"]["present_count"] == 4
    assert payload["metrics"]["coverage_ratio"] == 1.0
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
    assert len(sorted((signals_root / "waigani" / "2026-04-07").glob("*.json"))) == 1
    assert len(sorted(outbox_path.glob("*.json"))) == 1


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
    assert len(sorted((signals_root / "waigani" / "2026-04-07").glob("*.json"))) == 1
    assert len(sorted(outbox_path.glob("*.json"))) == 1


def _patch_output_paths(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    signals_root = tmp_path / "SIGNALS" / "normalized"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(signal_writer, "SIGNALS_ROOT", signals_root)
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
