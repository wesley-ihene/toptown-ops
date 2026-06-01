"""Tests for cashier and supervisor adjustment risk intelligence."""

from __future__ import annotations

import json
from pathlib import Path

from apps.cashier_supervisor_risk_agent.worker import build_risk_outputs


def test_item_returns_creates_low_risk_entry(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        balanced_by="Francis Ano",
        item_returns=25.0,
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    entry = _find_entry(result["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="item_returns")
    assert entry["severity"] == "low"
    assert entry["total_amount"] == 25.0


def test_cash_over_above_twenty_creates_medium_risk_entry(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        cash_over=21.0,
        variance_reason="Till float correction",
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    entry = _find_entry(result["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="cash_over")
    assert entry["severity"] == "medium"


def test_cash_down_above_fifty_creates_high_risk_entry(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        cash_down=55.0,
        variance_reason="Safe drop",
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    entry = _find_entry(result["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="cash_down")
    assert entry["severity"] == "high"


def test_repeated_cashier_adjustments_within_seven_days_escalate_severity(tmp_path: Path) -> None:
    for report_date in ("2026-05-01", "2026-05-03", "2026-05-06"):
        _write_sales_record(
            tmp_path,
            branch="waigani",
            report_date=report_date,
            cashier="Fidelma Wobilo",
            supervisor="Francis Ano",
            cash_over=10.0,
            variance_reason="Till float correction",
        )

    result = build_risk_outputs(start_date="2026-05-01", end_date="2026-05-06", output_root=tmp_path)

    entry = _find_entry(result["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="repeated_adjustment")
    assert entry["occurrence_count"] == 3
    assert entry["severity"] == "high"


def test_same_supervisor_repeated_adjustments_are_tracked(tmp_path: Path) -> None:
    for report_date in ("2026-05-01", "2026-05-03", "2026-05-05"):
        _write_sales_record(
            tmp_path,
            branch="waigani",
            report_date=report_date,
            cashier=f"Cashier {report_date[-2:]}",
            supervisor="Francis Ano",
            cash_over=8.0,
            variance_reason="Till float correction",
        )

    result = build_risk_outputs(start_date="2026-05-01", end_date="2026-05-05", output_root=tmp_path)

    entry = _find_entry(result["entries"], person_role="supervisor", person_name="Francis Ano", risk_type="repeated_adjustment")
    assert entry["occurrence_count"] == 3
    assert entry["severity"] == "medium"


def test_balanced_by_same_as_cashier_triggers_medium_risk(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        balanced_by="Fidelma Wobilo",
        cash_over=15.0,
        variance_reason="Till float correction",
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    entry = _find_entry(
        result["entries"],
        person_role="cashier",
        person_name="Fidelma Wobilo",
        risk_type="balanced_by_same_as_cashier",
    )
    assert entry["severity"] == "medium"
    assert entry["recommended_action"] == "Supervisor review recommended."


def test_z_reading_mismatch_triggers_risk_entry(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        z_reading=95.0,
        expected_z_reading=120.0,
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    entry = _find_entry(result["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="z_reading_mismatch")
    assert entry["severity"] == "medium"
    assert entry["total_amount"] == 25.0


def test_unexplained_variance_escalates_z_reading_mismatch_severity(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        z_reading=95.0,
        expected_z_reading=120.0,
        unexplained_variance=25.0,
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    entry = _find_entry(result["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="z_reading_mismatch")
    assert entry["severity"] == "high"


def test_no_adjustment_means_no_risk_entry(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    assert result["entries"] == []


def test_empty_runs_still_create_daily_and_branch_audit_outputs(tmp_path: Path) -> None:
    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    daily_dir = tmp_path / "records" / "risk" / "daily"
    branch_dir = tmp_path / "records" / "risk" / "branch"
    daily_path = daily_dir / "2026-05-20.json"
    branch_path = branch_dir / "all.json"

    assert result["entries"] == []
    assert daily_dir.exists()
    assert branch_dir.exists()
    assert daily_path.exists()
    assert branch_path.exists()
    assert json.loads(daily_path.read_text(encoding="utf-8"))["entries"] == []
    assert json.loads(branch_path.read_text(encoding="utf-8"))["entries"] == []


def test_idempotent_run_does_not_duplicate_occurrence_counts(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        cash_over=10.0,
        variance_reason="Till float correction",
    )

    first = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)
    second = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    first_entry = _find_entry(first["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="cash_over")
    second_entry = _find_entry(second["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="cash_over")
    assert first_entry["occurrence_count"] == 1
    assert second_entry["occurrence_count"] == 1


def test_output_files_are_created_with_expected_schema(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        branch="waigani",
        report_date="2026-05-20",
        cashier="Fidelma Wobilo",
        supervisor="Francis Ano",
        balanced_by="Francis Ano",
        cash_over=12.0,
        variance_reason="Till float correction",
    )

    result = build_risk_outputs(start_date="2026-05-20", end_date="2026-05-20", output_root=tmp_path)

    aggregate_path = Path(result["output_paths"]["aggregate"])
    summary_path = Path(result["output_paths"]["summary"])
    daily_path = tmp_path / "records" / "risk" / "daily" / "2026-05-20.json"
    branch_path = tmp_path / "records" / "risk" / "branch" / "waigani.json"

    assert aggregate_path.exists()
    assert daily_path.exists()
    assert branch_path.exists()
    assert summary_path.exists()

    payload = json.loads(aggregate_path.read_text(encoding="utf-8"))
    assert sorted(payload.keys()) == ["entries", "generated_at"]
    entry = _find_entry(payload["entries"], person_role="cashier", person_name="Fidelma Wobilo", risk_type="cash_over")
    assert entry["risk_id"].startswith("cashier|Fidelma Wobilo|waigani|")
    assert entry["source_records"] == [
        str(tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-05-20.json")
    ]


def _write_sales_record(
    root: Path,
    *,
    branch: str,
    report_date: str,
    cashier: str,
    supervisor: str | None,
    balanced_by: str | None = None,
    item_returns: float = 0.0,
    cash_over: float = 0.0,
    cash_down: float = 0.0,
    variance_reason: str | None = None,
    warnings: list[dict[str, str]] | None = None,
    unexplained_variance: float = 0.0,
    z_reading: float | None = None,
    expected_z_reading: float | None = None,
) -> None:
    path = root / "records" / "structured" / "sales_income" / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "branch": branch,
        "report_date": report_date,
        "status": "accepted_with_warning",
        "metrics": {
            "gross_sales": 200.0,
            "net_sales": round(200.0 - item_returns, 2),
            "item_returns": item_returns,
            "cash_over": cash_over,
            "cash_down": cash_down,
            "variance_reason": variance_reason,
            "z_reading": z_reading,
        },
        "provenance": {
            "cashier": cashier,
            "assistant": None,
            "supervisor": supervisor,
            "balanced_by": balanced_by,
            "notes": [f"Variance Reason: {variance_reason}"] if variance_reason else [],
        },
        "reconciliation": {
            "cash_over": cash_over,
            "cash_down": cash_down,
            "item_return_adjustment": item_returns,
            "unexplained_variance": unexplained_variance,
            "variance_explained": unexplained_variance == 0.0,
            "declared_z_reading": z_reading,
            "expected_z_reading": expected_z_reading,
        },
        "warnings": warnings or [],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _find_entry(
    entries: list[dict],
    *,
    person_role: str,
    person_name: str,
    risk_type: str,
) -> dict:
    for entry in entries:
        if (
            entry["person_role"] == person_role
            and entry["person_name"] == person_name
            and entry["risk_type"] == risk_type
        ):
            return entry
    raise AssertionError(f"entry not found for {person_role=} {person_name=} {risk_type=}")
