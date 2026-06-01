"""Focused tests for the Supervisor Control Agent."""

from __future__ import annotations

import json
import logging
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

    assert result.payload["status"] == "accepted"
    assert result.payload["governance"]["status"] == "accepted"
    assert result.payload["governance"]["report_family"] == "intelligence"
    outbox_files = sorted(outbox_path.glob("*.json"))
    assert len(outbox_files) == 1
    intelligence_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json"
    structured_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-07.json"
    assert intelligence_path.exists()
    assert not structured_path.exists()
    event_path = signals_root / "waigani" / "2026-04-07" / "supervisor_control_report__waigani__2026-04-07.json"
    assert event_path.exists()

    stored_payload = json.loads(intelligence_path.read_text(encoding="utf-8"))
    payload = json.loads(event_path.read_text(encoding="utf-8"))
    assert stored_payload["report_family"] == "intelligence"
    assert stored_payload["report_type"] == "supervisor_control"
    assert stored_payload["raw_text"] is not None
    assert stored_payload["created_at"] is not None
    assert payload["signal_type"] == "supervisor_control_report"
    assert payload["branch"] == "waigani"
    assert payload["report_date"] == "2026-04-07"
    assert payload["source_record_type"] == "supervisor_control"
    assert payload["event_kind"] == "supervisor_control_report"
    assert payload["payload"]["provenance"]["notes"] == ["Transport delay"]
    assert payload["ceo_advisory"]["signal_class"] == "intelligence"
    assert payload["ceo_advisory"]["exception_count"] == 1
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

    assert result.payload["status"] == "accepted"
    assert result.payload["governance"]["status"] == "accepted"
    assert result.payload["source"] == "live"
    assert result.payload["sop_compliance"] == "strict"
    assert result.payload["signal_weight"] == 0.4
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "missing_confirmation" in warning_codes
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json").exists()
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

    assert result.payload["status"] == "accepted"
    assert result.payload["governance"]["status"] == "accepted"
    assert result.payload["source"] == "live"
    assert result.payload["sop_compliance"] == "strict"
    assert result.payload["signal_weight"] == 0.4
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "escalation_required" in warning_codes
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json").exists()
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

    assert result.payload["status"] == "accepted"
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


def test_checklist_style_supervisor_report_normalizes_fields_before_validation(
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
                "Cash variance: No",
                "Staffing issues: Yes",
                "Stock issues affecting sales: No",
                "Pricing or system issues: Yes",
                "Exceptions escalated to Ops Manager: no",
            ]
        )
    )

    assert [item["details"] for item in result.payload["items"]] == [
        "Cash_Variance: NO",
        "Staffing_Issues: YES",
        "Stock_Issues: NO",
        "Pricing_System_Issues: YES",
        "Exceptions: NO",
    ]
    assert [item["action_taken"] for item in result.payload["items"]] == [
        "Verified",
        "Verified",
        "Verified",
        "Verified",
        "Verified",
    ]
    assert [item["supervisor_confirmed"] for item in result.payload["items"]] == [
        "YES",
        "YES",
        "YES",
        "YES",
        "YES",
    ]


def test_supervisor_control_summary_normalizes_to_canonical_report_shape(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _supervisor_control_work_item(
            lines=[
                "Supervisor Control Summary",
                "Date: Friday 10/04/26",
                "Branch: Waigani",
                "Supervisor: Privien (acting)",
                "Cash variance: No",
                "Staffing issues: No",
                "Stock issues affecting sales: No",
                "Pricing or system issues:NO ",
                "Exceptions escalated to Ops Manager: ",
                "Supervisor confirmation:",
                "All material issues have been escalated.",
            ]
        )
    )

    assert [item["details"] for item in result.payload["items"]] == [
        "Cash_Variance: NO",
        "Staffing_Issues: NO",
        "Stock_Issues: NO",
        "Pricing_System_Issues: NO",
    ]
    assert [item["action_taken"] for item in result.payload["items"]] == [
        "Verified",
        "Verified",
        "Verified",
        "Verified",
    ]
    assert [item["exception_type"] for item in result.payload["items"]] == [
        "CASH_CONTROL",
        "STAFFING_CONTROL",
        "STOCK_CONTROL",
        "PRICING_SYSTEM_CONTROL",
    ]
    assert result.payload["supervisor_confirmation"] == "All material issues have been escalated."
    assert result.payload["provenance"]["notes"] == [
        "Supervisor: Privien (acting)",
        "Supervisor confirmation: All material issues have been escalated.",
    ]
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert warning_codes == set()


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


def test_supervisor_control_logs_intelligence_events(tmp_path: Path, monkeypatch, caplog) -> None:
    _patch_output_paths(tmp_path, monkeypatch)
    caplog.set_level(logging.INFO)

    process_work_item(
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
            ]
        )
    )

    assert '"event": "intelligence_signal_extracted"' in caplog.text
    assert '"event": "intelligence_report_accepted"' in caplog.text


def _patch_output_paths(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    signals_root = colony_root / "SIGNALS" / "normalized"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
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
