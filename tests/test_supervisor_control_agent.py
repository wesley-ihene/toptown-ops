"""Focused tests for the Supervisor Control Agent."""

from __future__ import annotations

import json
from pathlib import Path

from apps.supervisor_control_agent.worker import process_work_item
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
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
    outbox_files = sorted(outbox_path.glob("*.json"))
    assert len(outbox_files) == 1
    event_path = signals_root / "waigani" / "2026-04-07" / "supervisor_control_report__waigani__2026-04-07.json"
    assert event_path.exists()

    payload = json.loads(event_path.read_text(encoding="utf-8"))
    assert payload["signal_type"] == "supervisor_control_report"
    assert payload["branch"] == "waigani"
    assert payload["report_date"] == "2026-04-07"
    assert payload["source_record_type"] == "supervisor_control"
    assert payload["event_kind"] == "supervisor_control_report"
    assert payload["payload"]["provenance"]["notes"] == ["Transport delay"]
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
    assert result.payload["source"] == "live"
    assert result.payload["sop_compliance"] == "strict"
    assert result.payload["signal_weight"] == 0.4
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "missing_confirmation" in warning_codes
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (signals_root / "waigani" / "2026-04-07" / "supervisor_control_report__waigani__2026-04-07.json").exists()


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
    assert result.payload["source"] == "live"
    assert result.payload["sop_compliance"] == "strict"
    assert result.payload["signal_weight"] == 0.4
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "escalation_required" in warning_codes
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (signals_root / "waigani" / "2026-04-07" / "supervisor_control_report__waigani__2026-04-07.json").exists()


def test_checklist_style_supervisor_report_synthesizes_contract_items(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _supervisor_control_work_item(
            lines=[
                "Supervisor Control Report",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Floor Check: Passed",
                "Cashier Reconciled: Yes",
                "- Front door display checked",
            ]
        )
    )

    assert result.payload["status"] == "ready"
    assert result.payload["source"] == "live"
    assert result.payload["sop_compliance"] == "fallback"
    assert result.payload["signal_weight"] == 0.4
    assert result.payload["metrics"]["exception_count"] == 3
    assert result.payload["metrics"]["control_gap_count"] == 0

    for item in result.payload["items"]:
        assert isinstance(item["exception_type"], str)
        assert item["exception_type"]
        assert isinstance(item["action_taken"], str)
        assert item["action_taken"]
        assert item["supervisor_confirmed"] in {"YES", "NO"}

    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (signals_root / "waigani" / "2026-04-07" / "supervisor_control_report__waigani__2026-04-07.json").exists()


def test_checklist_style_supervisor_report_uses_canonical_semantic_exception_types(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _supervisor_control_work_item(
            lines=[
                "Supervisor Control Report",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Cashier Reconciled: Yes",
                "Floor Check: Passed",
                "Stock issue: Empty rail",
                "Printer down: POS issue",
                "Staffing issue: Absent staff",
            ]
        )
    )

    assert [item["exception_type"] for item in result.payload["items"]] == [
        "CASH_CONTROL",
        "FLOOR_CONTROL",
        "STOCK_CONTROL",
        "PRICING_SYSTEM_CONTROL",
        "STAFFING_CONTROL",
    ]
    assert result.payload["source"] == "live"
    assert result.payload["sop_compliance"] == "fallback"
    assert result.payload["signal_weight"] == 0.4


def test_replay_marked_work_item_sets_source_to_replay(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _supervisor_control_work_item(
            lines=[
                "Supervisor Control Report",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Exception Type: STAFF_ISSUE",
                "Details: Late opening",
                "Action Taken: Resolved",
                "Supervisor Confirmed: YES",
            ],
            replay=True,
        )
    )

    assert result.payload["source"] == "replay"
    assert result.payload["signal_weight"] == 0.4


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
    monkeypatch.setattr("apps.supervisor_control_agent.worker.OUTBOX_PATH", outbox_path)
    return signals_root, outbox_path


def _supervisor_control_work_item(*, lines: list[str], replay: bool = False) -> WorkItem:
    payload = {
        "classification": {"report_type": "supervisor_control"},
        "raw_message": {"text": "\n".join(lines)},
    }
    if replay:
        payload["replay"] = {
            "is_replay": True,
            "source": "raw",
            "original_path": "records/raw/whatsapp/unknown/sample.txt",
            "replayed_at": "2026-04-09T00:00:00Z",
        }
    return WorkItem(kind="raw_message", payload=payload)
