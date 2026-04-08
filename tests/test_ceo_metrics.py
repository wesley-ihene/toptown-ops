"""Tests for read-only CEO analytics summaries."""

from __future__ import annotations

import pytest

from analytics.ceo_metrics import (
    build_ceo_summary,
    compute_payroll_efficiency,
    compute_productivity,
    compute_stock_velocity,
    main,
    write_ceo_summary_json,
    write_ceo_summary_markdown,
)


def test_compute_productivity_with_valid_sales_and_attendance() -> None:
    sales_signal = {
        "metrics": {
            "gross_sales": 1200.0,
            "traffic": 12,
            "served": 9,
        }
    }
    attendance_signal = {"metrics": {"present_count": 4}}

    summary = compute_productivity(sales_signal, attendance_signal=attendance_signal)

    assert summary["sales_per_staff"] == 300.0
    assert summary["sales_per_customer"] == 133.33
    assert summary["conversion_rate"] == 0.75
    assert summary["efficiency_index"] == 225.0
    assert summary["staff_count_used"] == 4


def test_compute_stock_velocity_with_pricing_totals() -> None:
    sales_signal = {"metrics": {"gross_sales": 1200.0}}
    pricing_signal = {"metrics": {"total_qty": 428, "total_amount": 3282.0}}

    summary = compute_stock_velocity(sales_signal, pricing_signal)

    assert summary["released_qty"] == 428
    assert summary["released_value"] == 3282.0
    assert summary["sales"] == 1200.0
    assert summary["velocity_ratio"] == 0.3656
    assert summary["avg_value_per_released_item"] == 7.67


def test_compute_payroll_efficiency_with_attendance_and_sales() -> None:
    sales_signal = {"metrics": {"gross_sales": 1200.0}}
    attendance_signal = {"metrics": {"present_count": 4}}

    summary = compute_payroll_efficiency(sales_signal, attendance_signal)

    assert summary["staff_present"] == 4
    assert summary["active_staff"] is None
    assert summary["utilization_ratio"] is None
    assert summary["revenue_per_staff"] == 300.0
    assert summary["labor_efficiency_flag"] == "NORMAL"


def test_build_ceo_summary_with_missing_pricing_signal(tmp_path) -> None:
    sales_dir = tmp_path / "records" / "structured" / "sales_income" / "waigani"
    attendance_dir = tmp_path / "records" / "structured" / "hr_attendance" / "waigani"
    sales_dir.mkdir(parents=True)
    attendance_dir.mkdir(parents=True)
    (sales_dir / "2026-04-06.json").write_text(
        '{"branch":"waigani","report_date":"2026-04-06","metrics":{"gross_sales":1200.0,"traffic":12,"served":9}}',
        encoding="utf-8",
    )
    (attendance_dir / "2026-04-06.json").write_text(
        '{"branch":"waigani","report_date":"2026-04-06","metrics":{"present_count":4}}',
        encoding="utf-8",
    )

    summary = build_ceo_summary("waigani", "2026-04-06", root=tmp_path)

    assert summary["sources"]["pricing"] is False
    assert summary["stock_velocity"]["released_value"] is None


def test_build_ceo_summary_ignores_legacy_noncanonical_pricing_path(tmp_path) -> None:
    sales_dir = tmp_path / "records" / "structured" / "sales_income" / "waigani"
    legacy_pricing_dir = (
        tmp_path
        / "records"
        / "structured"
        / "pricing_stock_release"
        / "ttc_pom_waigani_branch"
    )
    sales_dir.mkdir(parents=True)
    legacy_pricing_dir.mkdir(parents=True)
    (sales_dir / "2026-04-06.json").write_text(
        '{"branch":"waigani","report_date":"2026-04-06","metrics":{"gross_sales":1200.0}}',
        encoding="utf-8",
    )
    (legacy_pricing_dir / "2026-04-06.json").write_text(
        '{"branch":"TTC POM Waigani Branch","report_date":"2026-04-06","metrics":{"total_qty":428,"total_amount":3282.0}}',
        encoding="utf-8",
    )

    summary = build_ceo_summary("waigani", "2026-04-06", root=tmp_path)

    assert summary["sources"]["pricing"] is False
    assert summary["stock_velocity"]["released_qty"] is None
    assert summary["stock_velocity"]["released_value"] is None


def test_build_ceo_summary_rejects_non_iso_payload_date(tmp_path) -> None:
    sales_dir = tmp_path / "records" / "structured" / "sales_income" / "waigani"
    sales_dir.mkdir(parents=True)
    (sales_dir / "2026-04-06.json").write_text(
        '{"branch":"waigani","report_date":"06/04/2026","metrics":{"gross_sales":1200.0}}',
        encoding="utf-8",
    )

    summary = build_ceo_summary("waigani", "2026-04-06", root=tmp_path)

    assert summary["sources"]["sales"] is False
    assert summary["productivity"]["sales_per_staff"] is None
    assert summary["stock_velocity"]["sales"] is None


def test_writer_paths_and_overwrite_rules(tmp_path) -> None:
    summary = {
        "branch": "waigani",
        "date": "2026-04-06",
        "sources": {"sales": True, "staff": False, "attendance": True, "pricing": False},
        "productivity": {
            "sales_per_staff": 300.0,
            "sales_per_customer": 133.33,
            "conversion_rate": 0.75,
            "efficiency_index": 225.0,
            "staff_count_used": 4,
        },
        "stock_velocity": {
            "released_qty": None,
            "released_value": None,
            "sales": 1200.0,
            "velocity_ratio": None,
            "avg_value_per_released_item": None,
        },
        "payroll_efficiency": {
            "staff_present": 4,
            "active_staff": None,
            "utilization_ratio": None,
            "revenue_per_staff": 300.0,
            "labor_efficiency_flag": "NORMAL",
        },
        "alerts": [],
    }

    json_path = write_ceo_summary_json(summary, output_root=tmp_path)
    markdown_path = write_ceo_summary_markdown(summary, output_root=tmp_path)

    assert json_path == tmp_path / "REPORTS" / "ceo" / "waigani" / "2026-04-06_ceo_summary.json"
    assert markdown_path == tmp_path / "REPORTS" / "ceo" / "waigani" / "2026-04-06_ceo_summary.md"

    with pytest.raises(FileExistsError):
        write_ceo_summary_json(summary, output_root=tmp_path)
    with pytest.raises(FileExistsError):
        write_ceo_summary_markdown(summary, output_root=tmp_path)


def test_cli_rejects_non_iso_date() -> None:
    with pytest.raises(SystemExit) as error:
        main(["--branch", "waigani", "--date", "06/04/2026"])

    assert error.value.code == 2
