"""Tests for read-only TAOP ops summaries."""

from __future__ import annotations

import json
from pathlib import Path

from packages.taop_ops.summaries import (
    build_daily_operations_summary,
    summarize_attendance,
    summarize_bale_release,
    summarize_sales,
)


def test_summarize_sales_maps_stored_metrics_and_preserves_warnings() -> None:
    summary = summarize_sales(
        [
            {
                "branch": "waigani",
                "report_date": "2026-05-01",
                "status": "accepted_with_warning",
                "metrics": {
                    "cash_sales": 700.0,
                    "eftpos_sales": 500.0,
                    "gross_sales": 1200.0,
                    "till_total": 700.0,
                    "traffic": 12,
                    "served": 9,
                    "conversion_rate": 0.75,
                    "sales_per_customer": 133.33,
                },
                "warnings": [{"code": "till_mismatch", "message": "Till mismatch.", "severity": "warning"}],
            }
        ]
    )

    row = summary["rows"][0]

    assert summary["record_count"] == 1
    assert row["branch"] == "waigani"
    assert row["total_cash"] == 700.0
    assert row["total_card"] == 500.0
    assert row["total_sales"] == 1200.0
    assert row["till_count"] == 700.0
    assert row["traffic"] == 12
    assert row["served"] == 9
    assert row["conversion_rate"] == 0.75
    assert row["sales_per_customer"] == 133.33
    assert row["warnings"][0]["code"] == "till_mismatch"


def test_summarize_bale_release_uses_stored_metrics_not_item_recalculation() -> None:
    summary = summarize_bale_release(
        [
            {
                "branch": "lae_5th_street",
                "report_date": "2026-05-01",
                "status": "accepted_with_warning",
                "items": [
                    {"bale_id": "01", "qty": 400, "amount": 2179.0},
                    {"bale_id": "02", "qty": 600, "amount": 9999.0},
                ],
                "metrics": {
                    "total_qty": 492,
                    "total_amount": 3292.0,
                    "bales_processed": 2,
                    "bales_released": 2,
                    "bales_pending_approval": 0,
                },
                "warnings": [{"code": "format_cleanup", "message": "Format cleanup.", "severity": "warning"}],
            }
        ]
    )

    row = summary["rows"][0]

    assert row["branch"] == "lae_5th_street"
    assert row["item_count"] == 2
    assert row["total_qty"] == 492
    assert row["total_amount"] == 3292.0
    assert row["bales_processed"] == 2
    assert row["bales_released"] == 2
    assert row["bales_pending_approval"] == 0
    assert row["warnings"][0]["code"] == "format_cleanup"


def test_summarize_attendance_uses_stored_metrics() -> None:
    summary = summarize_attendance(
        [
            {
                "branch": "bena_road",
                "report_date": "2026-05-01",
                "status": "accepted",
                "metrics": {
                    "present_count": 7,
                    "off_count": 1,
                    "leave_count": 1,
                    "absent_count": 2,
                    "total_staff_listed": 11,
                },
                "warnings": [],
            }
        ]
    )

    row = summary["rows"][0]

    assert row["branch"] == "bena_road"
    assert row["present_count"] == 7
    assert row["off_count"] == 1
    assert row["leave_count"] == 1
    assert row["absent_count"] == 2
    assert row["staff_total"] == 11


def test_build_daily_operations_summary_returns_empty_category_rows_with_warnings(tmp_path: Path) -> None:
    _write_record(
        tmp_path,
        "sales_income",
        "waigani",
        "2026-05-01",
        {
            "branch": "waigani",
            "report_date": "2026-05-01",
            "metrics": {"gross_sales": 1200.0, "cash_sales": 700.0, "eftpos_sales": 500.0},
            "warnings": [],
        },
    )
    _write_record(
        tmp_path,
        "hr_attendance",
        "waigani",
        "2026-05-01",
        {
            "branch": "waigani",
            "report_date": "2026-05-01",
            "metrics": {"present_count": 3, "off_count": 1, "leave_count": 0, "absent_count": 0, "total_staff_listed": 4},
            "warnings": [],
        },
    )

    summary = build_daily_operations_summary("2026-05-01", branch="waigani", root=tmp_path)

    assert summary["sales"]["record_count"] == 1
    assert summary["bale_release"]["record_count"] == 0
    assert summary["attendance"]["record_count"] == 1
    assert len(summary["branches"]) == 1
    branch_summary = summary["branches"][0]
    assert branch_summary["branch"] == "waigani"
    assert branch_summary["sales"]["total_sales"] == 1200.0
    assert branch_summary["attendance"]["present_count"] == 3
    assert branch_summary["bale_release"]["total_qty"] is None
    assert "missing_bale_release_records" in {warning["code"] for warning in branch_summary["warnings"]}


def _write_record(root: Path, record_type: str, branch: str, report_date: str, payload: dict) -> None:
    path = root / "records" / "structured" / record_type / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
