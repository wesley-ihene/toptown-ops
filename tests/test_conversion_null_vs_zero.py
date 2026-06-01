"""Regression tests for null-vs-zero conversion handling."""

from __future__ import annotations

import json
from pathlib import Path

from analytics.phase3 import build_branch_daily_analytics
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from apps.sales_income_agent.worker import process_work_item as process_sales_work_item
from packages.signal_contracts.work_item import WorkItem


def test_branch_daily_analytics_uses_null_conversion_for_missing_customer_inputs(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        "bena_road",
        "2026-04-27",
        gross_sales=150.0,
        traffic=None,
        served=None,
        conversion_rate=0.0,
    )
    _write_staff_record(tmp_path, "bena_road", "2026-04-27")

    payload = build_branch_daily_analytics("bena_road", "2026-04-27", root=tmp_path)

    assert payload["traffic"] is None
    assert payload["served"] is None
    assert payload["conversion_rate"] is None
    flag_codes = {flag["code"] for flag in payload["operational_flags"]}
    assert "missing_customer_data" in flag_codes
    assert "low_conversion_rate" not in flag_codes


def test_sales_income_missing_customer_data_does_not_emit_low_conversion_alert(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_sales_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: bena_road",
                "Date: 2026-04-27",
                "Total Cash: 100.00",
                "Total Card: 50.00",
                "Total Sales: 150.00",
            ]
        )
    )

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["metrics"]["traffic"] is None
    assert result.payload["metrics"]["served"] is None
    assert result.payload["metrics"]["conversion_rate"] is None
    assert result.payload["performance_alerts"] == []
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "missing_customer_data" in warning_codes
    assert "low_conversion_rate" not in warning_codes


def test_zero_traffic_stays_zero_and_triggers_low_conversion_when_inputs_are_present(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_sales_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: bena_road",
                "Date: 2026-04-28",
                "Total Cash: 90.00",
                "Total Card: 10.00",
                "Total Sales: 100.00",
                "Traffic: 0",
                "Served: 0",
            ]
        )
    )
    _write_staff_record(tmp_path, "bena_road", "2026-04-28")

    payload = build_branch_daily_analytics("bena_road", "2026-04-28", root=tmp_path)

    assert result.payload["status"] == "accepted"
    assert result.payload["metrics"]["traffic"] == 0
    assert result.payload["metrics"]["served"] == 0
    assert result.payload["metrics"]["conversion_rate"] == 0.0
    assert result.payload["performance_alerts"] == [
        {
            "alert_type": "low_conversion_rate",
            "alert_level": "warning",
            "alert_message": "Conversion rate is low at 0.00%.",
            "alert_category": "performance_alert",
        }
    ]

    assert payload["conversion_rate"] == 0.0
    flag_codes = {flag["code"] for flag in payload["operational_flags"]}
    assert "low_conversion_rate" in flag_codes
    assert "missing_customer_data" not in flag_codes


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "DUPLICATES_DIR", records_dir / "duplicates" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))


def _sales_work_item(lines: list[str]) -> WorkItem:
    return WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "sales"},
            "raw_message": {"text": "\n".join(lines)},
        },
    )


def _write_sales_record(
    root: Path,
    branch: str,
    report_date: str,
    *,
    gross_sales: float,
    traffic: int | None,
    served: int | None,
    conversion_rate: float | None,
) -> None:
    path = root / "records" / "structured" / "sales_income" / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "branch": branch,
                "report_date": report_date,
                "signal_type": "sales_income",
                "status": "accepted",
                "warnings": [],
                "metrics": {
                    "gross_sales": gross_sales,
                    "traffic": traffic,
                    "served": served,
                    "conversion_rate": conversion_rate,
                    "sales_per_labor_hour": None,
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_staff_record(root: Path, branch: str, report_date: str) -> None:
    path = root / "records" / "structured" / "hr_performance" / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "branch": branch,
                "report_date": report_date,
                "signal_type": "hr",
                "signal_subtype": "staff_performance",
                "status": "accepted",
                "warnings": [],
                "items": [
                    {
                        "staff_name": "Alice Demo",
                        "duty_status": "on_duty",
                        "section": "mens_tshirt",
                        "raw_section": "Men's Tshirt",
                        "items_moved": 5,
                        "assisting_count": 1,
                        "activity_score": 5.5,
                        "role": "Cashier",
                    }
                ],
                "metrics": {
                    "total_items_moved": 5,
                    "total_assisting_count": 1,
                    "unresolved_section_count": 0,
                },
                "diagnostics": {
                    "section_resolution_stats": {
                        "resolved_count": 1,
                        "unresolved_count": 0,
                        "unresolved_examples": [],
                    }
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
