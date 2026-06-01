"""Tests for TAOP ops exports and route reachability."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from analytics import phase4_portal
from packages.taop_ops.exports import (
    default_export_directory,
    export_attendance_csv,
    export_bale_release_csv,
    export_daily_operations_json,
    export_sales_csv,
)
from scripts import export_taop_ops


def test_export_functions_write_expected_csv_and_json(tmp_path: Path) -> None:
    _seed_ops_records(tmp_path)

    export_dir = default_export_directory("2026-05-01", root=tmp_path)
    sales_path = export_sales_csv("2026-05-01", export_dir, root=tmp_path)
    bale_release_path = export_bale_release_csv("2026-05-01", export_dir, root=tmp_path)
    attendance_path = export_attendance_csv("2026-05-01", export_dir, root=tmp_path)
    daily_path = export_daily_operations_json("2026-05-01", export_dir, root=tmp_path)

    assert sales_path == export_dir / "sales.csv"
    assert bale_release_path == export_dir / "bale_release.csv"
    assert attendance_path == export_dir / "attendance.csv"
    assert daily_path == export_dir / "daily_operations.json"

    with sales_path.open("r", encoding="utf-8", newline="") as handle:
        sales_rows = list(csv.DictReader(handle))
    assert sales_rows == [
        {
            "branch": "waigani",
            "date": "2026-05-01",
            "total_cash": "700.0",
            "total_card": "500.0",
            "total_sales": "1200.0",
            "till_count": "700.0",
            "traffic": "12",
            "served": "9",
            "conversion_rate": "0.75",
            "sales_per_customer": "133.33",
        }
    ]

    with bale_release_path.open("r", encoding="utf-8", newline="") as handle:
        bale_rows = list(csv.DictReader(handle))
    assert bale_rows[0]["total_qty"] == "10"
    assert bale_rows[0]["total_amount"] == "100.0"

    with attendance_path.open("r", encoding="utf-8", newline="") as handle:
        attendance_rows = list(csv.DictReader(handle))
    assert attendance_rows[0]["present_count"] == "3"
    assert attendance_rows[0]["staff_total"] == "4"

    daily_payload = json.loads(daily_path.read_text(encoding="utf-8"))
    assert daily_payload["branches"][0]["branch"] == "waigani"
    assert daily_payload["branches"][0]["bale_release"]["total_qty"] == 10


def test_taop_ops_api_routes_are_reachable_via_phase4_portal(tmp_path: Path) -> None:
    _seed_ops_records(tmp_path)

    daily_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/taop/ops/daily?date=2026-05-01",
        root=tmp_path,
    )
    daily_body = json.loads(daily_response.body.decode("utf-8"))

    assert daily_response.status_code == 200
    assert daily_body["ok"] is True
    assert daily_body["product"] == "daily_operations"
    assert daily_body["payload"]["branches"][0]["branch"] == "waigani"

    export_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/taop/ops/export?date=2026-05-01&type=sales",
        root=tmp_path,
    )
    export_body = json.loads(export_response.body.decode("utf-8"))

    assert export_response.status_code == 200
    assert export_body["export_type"] == "sales"
    assert export_body["output_path"] == "records/exports/taop_ops/2026-05-01/sales.csv"
    assert (tmp_path / "records" / "exports" / "taop_ops" / "2026-05-01" / "sales.csv").exists()


def test_export_taop_ops_cli_writes_branch_filtered_outputs(tmp_path: Path) -> None:
    _seed_ops_records(tmp_path)

    exit_code = export_taop_ops.main(["--date", "2026-05-01", "--branch", "waigani", "--root", str(tmp_path)])

    assert exit_code == 0
    export_dir = tmp_path / "records" / "exports" / "taop_ops" / "2026-05-01"
    assert (export_dir / "sales.csv").exists()
    assert (export_dir / "bale_release.csv").exists()
    assert (export_dir / "attendance.csv").exists()
    assert (export_dir / "daily_operations.json").exists()


def _seed_ops_records(root: Path) -> None:
    _write_record(
        root,
        "sales_income",
        "waigani",
        "2026-05-01",
        {
            "branch": "waigani",
            "report_date": "2026-05-01",
            "status": "accepted",
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
            "warnings": [],
        },
    )
    _write_record(
        root,
        "pricing_stock_release",
        "waigani",
        "2026-05-01",
        {
            "branch": "waigani",
            "report_date": "2026-05-01",
            "status": "accepted_with_warning",
            "items": [{"bale_id": "01", "qty": 10, "amount": 100.0}],
            "metrics": {
                "total_qty": 10,
                "total_amount": 100.0,
                "bales_processed": 1,
                "bales_released": 1,
                "bales_pending_approval": 0,
            },
            "warnings": [{"code": "format_cleanup", "message": "Format cleanup.", "severity": "warning"}],
        },
    )
    _write_record(
        root,
        "hr_attendance",
        "waigani",
        "2026-05-01",
        {
            "branch": "waigani",
            "report_date": "2026-05-01",
            "status": "accepted",
            "metrics": {
                "present_count": 3,
                "off_count": 1,
                "leave_count": 0,
                "absent_count": 0,
                "total_staff_listed": 4,
            },
            "warnings": [],
        },
    )


def _write_record(root: Path, record_type: str, branch: str, report_date: str, payload: dict) -> None:
    path = root / "records" / "structured" / record_type / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
