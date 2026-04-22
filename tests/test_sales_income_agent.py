"""Focused tests for the sales income agent."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from apps.sales_income_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def test_sales_income_valid_optional_warnings_write_structured_as_accepted_with_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Till Total: 500",
                "Deposit Total: 50",
                "Traffic: 20",
                "Served: 4",
                "Notes: Eftpos was slow tonight",
            ]
        )
    )

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"

    assert result.payload["status"] == "accepted_with_warning"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "low_conversion" in warning_codes
    assert "cash_variance_present" in warning_codes
    assert "till_mismatch" in warning_codes
    assert structured_path.exists()

    payload = json.loads(structured_path.read_text(encoding="utf-8"))
    assert payload["status"] == "accepted_with_warning"
    assert payload["branch"] == "waigani"
    assert payload["report_date"] == "2026-04-07"


def test_sales_income_core_consistency_issue_stays_needs_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 500",
                "Mobile Money: 50",
                "Traffic: 10",
                "Served: 12",
            ]
        )
    )

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"

    assert result.payload["status"] == "needs_review"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "invalid_totals" in warning_codes
    assert "data_mismatch" in warning_codes
    assert structured_path.exists()


def test_sales_income_normalizes_live_whatsapp_date_and_label_variants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY end sales report",
                "Shop: Lae 5th Street Branch",
                "Date: Friday, 10/04 /26",
                "Total Sales: 1200",
                "Cash Sales: 700",
                "Card Sales: 500",
                "Main Door: 15",
                "Customers Served: 12",
                "Remark: Balanced and checked",
            ]
        )
    )

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "lae_5th_street" / "2026-04-10.json"

    assert result.payload["status"] == "accepted"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-10"
    assert structured_path.exists()

    payload = json.loads(structured_path.read_text(encoding="utf-8"))
    assert payload["status"] == "accepted"
    assert payload["branch"] == "lae_5th_street"
    assert payload["report_date"] == "2026-04-10"
    assert payload["metrics"]["gross_sales"] == 1200.0
    assert payload["metrics"]["eftpos_sales"] == 500.0
    assert payload["metrics"]["traffic"] == 15
    assert payload["metrics"]["served"] == 12


def test_sales_income_normalizes_branch_alias_and_messy_money_strings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY end sales report",
                "Shop: TTC LAE 5TH STREET BRANCH",
                "Date: Friday, 10/04 /26",
                "Total Sales: K3,489. 00",
                "Cash Sales: 1 236.00",
                "Card Sales: 2 253.00",
                "Main Door: 43",
                "Customers Served: 20",
            ]
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-10"
    assert result.payload["metrics"]["gross_sales"] == 3489.0
    assert result.payload["metrics"]["cash_sales"] == 1236.0
    assert result.payload["metrics"]["eftpos_sales"] == 2253.0


def test_sales_income_extracts_exact_labeled_whatsapp_values(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY end sales report",
                "Shop: TTC LAE 5TH STREET BRANCH",
                "Date: Friday, 10/04 /26",
                "T/Cash : K3,489.00",
                "T/Card : K1,236.00",
                "Total sales : K4,725.00",
                "Main door : 326",
                "Guest/customer serve : 143",
            ]
        )
    )

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "lae_5th_street" / "2026-04-10.json"

    assert result.payload["status"] == "accepted"
    assert structured_path.exists()

    payload = json.loads(structured_path.read_text(encoding="utf-8"))
    assert payload["branch"] == "lae_5th_street"
    assert payload["report_date"] == "2026-04-10"
    assert payload["metrics"]["cash_sales"] == 3489.0
    assert payload["metrics"]["eftpos_sales"] == 1236.0
    assert payload["metrics"]["gross_sales"] == 4725.0
    assert payload["metrics"]["traffic"] == 326
    assert payload["metrics"]["served"] == 143


def test_sales_income_extracts_operator_provenance_from_live_whatsapp_shapes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY end sales report",
                "Shop: TTC LAE 5TH STREET BRANCH",
                "Date: Friday, 10/04 /26",
                "T/Cash : K3,489.00",
                "T/Card : K1,236.00",
                "Total sales : K4,725.00",
                "Main door : 326",
                "Guest/customer serve : 143",
                "Cashier : Maria Sine",
                "Assistant - John Kalo",
                "Balanced by Mary Pita",
            ]
        )
    )

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "lae_5th_street" / "2026-04-10.json"

    assert result.payload["status"] == "accepted"
    assert structured_path.exists()

    payload = json.loads(structured_path.read_text(encoding="utf-8"))
    assert payload["metrics"]["cash_sales"] == 3489.0
    assert payload["metrics"]["eftpos_sales"] == 1236.0
    assert payload["metrics"]["gross_sales"] == 4725.0
    assert payload["provenance"]["cashier"] == "Maria Sine"
    assert payload["provenance"]["assistant"] == "John Kalo"
    assert payload["provenance"]["balanced_by"] == "Mary Pita"


def _patch_output_paths(tmp_path: Path, monkeypatch) -> None:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
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
