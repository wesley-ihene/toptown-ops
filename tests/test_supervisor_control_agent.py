"""Focused tests for the Supervisor Control Agent."""

from __future__ import annotations

import json
from pathlib import Path

from apps.supervisor_control_agent.worker import process_work_item
from packages.common import signal_writer
from packages.signal_contracts.work_item import WorkItem


def test_valid_supervisor_control_sample_writes_one_signal_file(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _supervisor_control_work_item(
            lines=[
                "Supervisor Control Report",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Exception Type: STAFF_ISSUE",
                "Details: Late opening",
                "Action Taken: Resolved",
                "Escalated By: Francis",
                "Time: 08:30",
                "Supervisor Confirmed: YES",
                "Notes: Transport delay",
            ]
        )
    )

    assert result.payload["status"] == "ready"
    signal_files = sorted((signals_root / "waigani" / "2026-04-07").glob("*.json"))
    outbox_files = sorted(outbox_path.glob("*.json"))
    assert len(signal_files) == 1
    assert len(outbox_files) == 1

    payload = json.loads(signal_files[0].read_text(encoding="utf-8"))
    assert payload["signal_type"] == "supervisor_control"
    assert payload["branch"] == "waigani"
    assert payload["report_date"] == "2026-04-07"
    assert payload["metrics"]["exception_count"] == 1
    assert payload["metrics"]["open_exception_count"] == 0
    assert payload["metrics"]["escalated_count"] == 0
    assert payload["metrics"]["confirmed_count"] == 1
    assert payload["metrics"]["control_gap_count"] == 0
    assert payload["warnings"] == []

    assert json.loads(outbox_files[0].read_text(encoding="utf-8")) == result.payload


def test_missing_supervisor_confirmation_raises_missing_confirmation(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _supervisor_control_work_item(
            lines=[
                "Supervisor Control Report",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Exception Type: STAFF_ISSUE",
                "Details: Late opening",
                "Action Taken: Resolved",
                "Escalated By: Francis",
                "Time: 08:30",
                "Notes: Transport delay",
            ]
        )
    )

    assert result.payload["status"] == "needs_review"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "missing_confirmation" in warning_codes
    assert len(sorted((signals_root / "waigani" / "2026-04-07").glob("*.json"))) == 1
    assert len(sorted(outbox_path.glob("*.json"))) == 1


def test_unresolved_exception_raises_escalation_required(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _supervisor_control_work_item(
            lines=[
                "Supervisor Control Report",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Exception Type: STAFF_ISSUE",
                "Details: Late opening",
                "Action Taken: Escalated",
                "Escalated By: Francis",
                "Time: 08:30",
                "Supervisor Confirmed: YES",
                "Notes: Transport delay",
            ]
        )
    )

    assert result.payload["status"] == "needs_review"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "escalation_required" in warning_codes
    assert len(sorted((signals_root / "waigani" / "2026-04-07").glob("*.json"))) == 1
    assert len(sorted(outbox_path.glob("*.json"))) == 1


def _patch_output_paths(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    signals_root = tmp_path / "SIGNALS" / "normalized"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(signal_writer, "SIGNALS_ROOT", signals_root)
    monkeypatch.setattr("apps.supervisor_control_agent.worker.OUTBOX_PATH", outbox_path)
    return signals_root, outbox_path


def _supervisor_control_work_item(*, lines: list[str]) -> WorkItem:
    return WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "supervisor_control"},
            "raw_message": {"text": "\n".join(lines)},
        },
    )
