"""Tests for the structured-record bridge into IOI Colony signals."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import export_colony_signals


def test_sales_income_record_exports_canonical_downstream_json_event(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "sales_income",
        "waigani",
        "2026-04-06",
        {
            "branch": "waigani",
            "report_date": "2026-04-06",
            "signal_type": "sales_income",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 700.0,
                "eftpos_sales": 500.0,
                "traffic": 12,
                "served": 9,
                "staff_on_duty": 4,
                "cash_variance": 10.0,
            },
            "provenance": {
                "supervisor_confirmation": "YES",
                "notes": ["Balanced after review"],
            },
            "warnings": [],
        },
    )

    manifest = export_colony_signals.export_one_record_type(
        "sales_income",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    output_path = (
        tmp_path
        / "ioi-colony"
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-06"
        / "daily_sales_report__waigani__2026-04-06.json"
    )
    assert output_path.exists()
    event = _read_json(output_path)
    assert event["signal_type"] == "daily_sales_report"
    assert event["event_kind"] == "daily_sales_report"
    assert event["branch"] == "waigani"
    assert event["branch_slug"] == "waigani"
    assert event["report_date"] == "2026-04-06"
    assert event["source_system"] == "toptown_ops"
    assert event["source_record_type"] == "sales_income"
    assert event["payload"]["totals"]["gross_sales"] == 1200.0
    assert event["payload"]["traffic"]["total_customers"] == 12
    assert event["payload"]["staffing"]["staff_on_duty"] == 4
    assert manifest["results"][0]["status"] == "written"


def test_hr_performance_record_exports_staff_records_payload(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "hr_performance",
        "waigani",
        "2026-04-06",
        {
            "branch": "waigani",
            "report_date": "2026-04-06",
            "signal_type": "hr",
            "items": [
                {
                    "staff_name": "Alice John",
                    "activity_score": 42.5,
                    "section": "cashier",
                    "raw_section": "Cashier Counter",
                    "duty_status": "on_duty",
                }
            ],
            "warnings": [],
        },
    )

    manifest = export_colony_signals.export_one_record_type(
        "hr_performance",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    output_path = Path(tmp_path / "ioi-colony" / manifest["results"][0]["output_path"])
    event = _read_json(output_path)
    assert event["signal_type"] == "staff_performance_report"
    assert len(event["payload"]["staff_records"]) == 1
    assert event["payload"]["staff_records"][0]["staff_name"] == "Alice John"
    assert event["payload"]["staff_records"][0]["staff_name_normalized"] == "alice john"
    assert event["payload"]["staff_records"][0]["section"] == "cashier"


def test_hr_attendance_record_exports_without_inventing_totals(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "hr_attendance",
        "lae_malaita",
        "2026-04-06",
        {
            "branch": "lae_malaita",
            "report_date": "2026-04-06",
            "signal_type": "hr",
            "items": [
                {
                    "staff_name": "Bob",
                    "status": "present",
                    "raw_status": "p",
                    "presence_score": 1,
                }
            ],
            "metrics": {
                "present_count": 1,
                "off_count": 2,
                "declared_status_totals": {"present": 1, "off": 2},
                "total_staff_records": 3,
            },
            "warnings": [],
        },
    )

    manifest = export_colony_signals.export_one_record_type(
        "hr_attendance",
        "lae_malaita",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    output_path = Path(tmp_path / "ioi-colony" / manifest["results"][0]["output_path"])
    event = _read_json(output_path)
    assert event["signal_type"] == "staff_attendance_report"
    assert event["payload"]["attendance_totals"]["present"] == 1
    assert event["payload"]["attendance_totals"]["off_duty"] == 2
    assert "staff_on_duty" not in event["payload"]["attendance_totals"]
    assert event["payload"]["declared_totals"] == {"present": 1, "off": 2}


def test_pricing_stock_release_record_exports_bales_and_totals(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "pricing_stock_release",
        "waigani",
        "2026-04-06",
        {
            "branch_slug": "waigani",
            "branch": "TTC POM Waigani Branch",
            "report_date": "2026-04-06",
            "signal_type": "pricing_stock_release",
            "items": [
                {
                    "bale_id": "01",
                    "item_name": "OSH",
                    "qty": 10,
                    "amount": 100.0,
                }
            ],
            "metrics": {
                "total_qty": 10,
                "total_amount": 100.0,
            },
            "provenance": {
                "prepared_by": "Maria Sine",
                "role": "Supervisor",
            },
            "warnings": [],
        },
    )

    manifest = export_colony_signals.export_one_record_type(
        "pricing_stock_release",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    output_path = Path(tmp_path / "ioi-colony" / manifest["results"][0]["output_path"])
    event = _read_json(output_path)
    assert event["signal_type"] == "daily_bale_summary_report"
    assert event["payload"]["bales"][0]["bale_id"] == "01"
    assert event["payload"]["totals"]["total_qty"] == 10
    assert event["released_by"] == "Maria Sine"


def test_supervisor_control_record_exports_canonical_downstream_json_event(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "supervisor_control",
        "waigani",
        "2026-04-06",
        {
            "branch": "waigani",
            "report_date": "2026-04-06",
            "signal_type": "supervisor_control",
            "checklist": ["Escalated float issue"],
            "key_values": {
                "Supervisor": "Francis Ano",
                "Cash variance": "None",
            },
            "notes": ["Two tills requested for faster service."],
            "metrics": {
                "checklist_count": 1,
                "key_value_count": 2,
                "note_count": 1,
            },
            "provenance": {
                "raw_branch": "TTC Waigani Branch",
                "raw_date": "06/04/26",
                "detected_subtype": "supervisor_control",
                "notes": ["header_line:1:TTC Waigani Branch"],
            },
            "warnings": [],
        },
    )

    manifest = export_colony_signals.export_one_record_type(
        "supervisor_control",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    output_path = Path(tmp_path / "ioi-colony" / manifest["results"][0]["output_path"])
    event = _read_json(output_path)
    assert event["signal_type"] == "supervisor_control_report"
    assert event["event_kind"] == "supervisor_control_report"
    assert event["report_type"] == "supervisor_control_report"
    assert event["source_record_type"] == "supervisor_control"
    assert event["payload"]["checklist"] == ["Escalated float issue"]
    assert event["payload"]["key_values"]["Supervisor"] == "Francis Ano"
    assert event["payload"]["metrics"]["note_count"] == 1
    assert event["payload"]["provenance"]["raw_branch"] == "TTC Waigani Branch"
    assert manifest["results"][0]["status"] == "written"


def test_invalid_non_iso_date_is_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid ISO date"):
        export_colony_signals.export_all_record_types(
            "waigani",
            "06/04/2026",
            source_root=Path("/tmp/source"),
            colony_root=Path("/tmp/colony"),
        )


def test_missing_source_record_yields_missing_manifest_status(tmp_path: Path) -> None:
    manifest = export_colony_signals.export_one_record_type(
        "sales_income",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    assert manifest["results"][0]["status"] == "missing"
    assert manifest["summary"]["missing"] == 1
    manifest_path = (
        tmp_path
        / "ioi-colony"
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-06"
        / "_export_manifest.json"
    )
    assert manifest_path.exists()


def test_overwrite_requires_explicit_flag(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "sales_income",
        "waigani",
        "2026-04-06",
        {
            "branch": "waigani",
            "report_date": "2026-04-06",
            "signal_type": "sales_income",
            "metrics": {"gross_sales": 1200.0},
            "warnings": [],
        },
    )

    first_manifest = export_colony_signals.export_one_record_type(
        "sales_income",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )
    second_manifest = export_colony_signals.export_one_record_type(
        "sales_income",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )
    third_manifest = export_colony_signals.export_one_record_type(
        "sales_income",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
        overwrite=True,
    )

    assert first_manifest["results"][0]["status"] == "written"
    assert second_manifest["results"][0]["status"] == "skipped"
    assert second_manifest["results"][0]["reason"] == "output_exists_use_overwrite"
    assert third_manifest["results"][0]["status"] == "written"


def test_write_compat_is_gated_and_explicit(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "sales_income",
        "waigani",
        "2026-04-06",
        {
            "branch": "waigani",
            "report_date": "2026-04-06",
            "signal_type": "sales_income",
            "metrics": {"gross_sales": 1200.0},
            "warnings": [],
        },
    )

    manifest = export_colony_signals.export_one_record_type(
        "sales_income",
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
        overwrite=True,
        write_compat=True,
    )

    assert manifest["results"][0]["status"] == "written"
    assert export_colony_signals.COMPATIBILITY_WARNING in manifest["results"][0]["warnings"]


def test_export_all_includes_supervisor_control_manifest_entry(tmp_path: Path) -> None:
    _write_structured_record(
        tmp_path,
        "supervisor_control",
        "waigani",
        "2026-04-06",
        {
            "branch": "waigani",
            "report_date": "2026-04-06",
            "signal_type": "supervisor_control",
            "checklist": [],
            "key_values": {"Supervisor": "Francis Ano"},
            "notes": ["All material issues escalated."],
            "metrics": {
                "checklist_count": 0,
                "key_value_count": 1,
                "note_count": 1,
            },
            "warnings": [],
        },
    )

    manifest = export_colony_signals.export_all_record_types(
        "waigani",
        "2026-04-06",
        source_root=tmp_path,
        colony_root=tmp_path / "ioi-colony",
    )

    supervisor_result = next(result for result in manifest["results"] if result["record_type"] == "supervisor_control")
    assert supervisor_result["signal_type"] == "supervisor_control_report"
    assert supervisor_result["status"] == "written"
    assert manifest["summary"]["scanned"] == 5


def _write_structured_record(
    root: Path,
    record_type: str,
    branch: str,
    report_date: str,
    payload: dict[str, object],
) -> Path:
    path = (
        root
        / "records"
        / "structured"
        / record_type
        / branch
        / f"{report_date}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
