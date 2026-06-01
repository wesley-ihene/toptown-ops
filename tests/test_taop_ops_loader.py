"""Tests for read-only TAOP ops structured loaders."""

from __future__ import annotations

import json
from pathlib import Path

from packages.taop_ops.loader import load_daily_operations, load_sales_records


def test_load_sales_records_filters_branch_and_ignores_sidecars(tmp_path: Path) -> None:
    _write_record(
        tmp_path,
        "sales_income",
        "waigani",
        "2026-05-01",
        {
            "branch": "waigani",
            "report_date": "2026-05-01",
            "metrics": {"gross_sales": 1200.0},
        },
    )
    _write_record(
        tmp_path,
        "sales_income",
        "lae_5th_street",
        "2026-05-01",
        {
            "branch": "lae_5th_street",
            "report_date": "2026-05-01",
            "metrics": {"gross_sales": 800.0},
        },
    )
    _write_record(
        tmp_path,
        "sales_income",
        "waigani",
        "2026-04-30",
        {
            "branch": "waigani",
            "report_date": "2026-04-30",
            "metrics": {"gross_sales": 900.0},
        },
    )
    sidecar_dir = tmp_path / "records" / "structured" / "sales_income" / "waigani"
    (sidecar_dir / "2026-05-01.governance.json").write_text("{}", encoding="utf-8")
    (sidecar_dir / "2026-05-01.validation.json").write_text("{}", encoding="utf-8")

    records = load_sales_records(date="2026-05-01", root=tmp_path)

    assert [(record["branch"], record["report_date"]) for record in records] == [
        ("lae_5th_street", "2026-05-01"),
        ("waigani", "2026-05-01"),
    ]

    lae_records = load_sales_records(date="2026-05-01", branch="LAE 5th Street", root=tmp_path)

    assert len(lae_records) == 1
    assert lae_records[0]["branch"] == "lae_5th_street"


def test_load_daily_operations_returns_missing_category_warnings(tmp_path: Path) -> None:
    _write_record(
        tmp_path,
        "sales_income",
        "waigani",
        "2026-05-01",
        {
            "branch": "waigani",
            "report_date": "2026-05-01",
            "metrics": {"gross_sales": 1200.0},
        },
    )

    bundle = load_daily_operations("2026-05-01", branch="waigani", root=tmp_path)

    assert len(bundle["sales_records"]) == 1
    assert bundle["bale_release_records"] == []
    assert bundle["attendance_records"] == []
    assert {warning["code"] for warning in bundle["warnings"]} == {
        "missing_attendance_records",
        "missing_bale_release_records",
    }


def _write_record(root: Path, record_type: str, branch: str, report_date: str, payload: dict) -> None:
    path = root / "records" / "structured" / record_type / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
