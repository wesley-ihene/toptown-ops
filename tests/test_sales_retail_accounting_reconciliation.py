"""Regression tests for retail-accounting sales reconciliation."""

from __future__ import annotations

from pathlib import Path

import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from apps.response_engine import render_whatsapp_response
from apps.sales_income_agent.parser import parse_work_item
from apps.sales_income_agent.worker import process_work_item
from packages.report_acceptance import decide_acceptance
from packages.signal_contracts.work_item import WorkItem
from packages.sop_validation.sales import validate_sales


def test_item_returns_included_and_reconciled_is_accepted_with_warning(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "Cash Sales: 100",
                "Card Sales: 50",
                "Item Returns: K5.00",
                "Return Type: Declined-card return",
                "Supervisor Confirmation: YES",
                "Total Sales: 145",
                "Z/Reading: 150",
                "Traffic: 12",
                "Served: 10",
            ]
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert result.payload["status"] == "accepted_with_warning"
    assert "item_returns_present" in warning_codes
    assert "invalid_totals" not in warning_codes


def test_cash_over_reconciles_to_accepted_with_warning(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "T/Cash: 100",
                "T/Card: 50",
                "C/over: 10",
                "Variance Reason: Till float correction",
                "Supervisor Confirmation: YES",
                "Total Sales: 160",
                "Z/Reading: 140",
                "Traffic: 12",
                "Served: 10",
            ]
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert result.payload["status"] == "accepted_with_warning"
    assert "cash_over_present" in warning_codes
    assert "invalid_totals" not in warning_codes
    assert result.payload["reconciliation"]["unexplained_variance"] == 0.0


def test_cash_down_reconciles_to_accepted_with_warning(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "T/Cash: 100",
                "T/Card: 50",
                "C/down: 5",
                "Variance Reason: Safe drop",
                "Supervisor Confirmation: YES",
                "Total Sales: 145",
                "Z/Reading: 155",
                "Traffic: 12",
                "Served: 10",
            ]
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert result.payload["status"] == "accepted_with_warning"
    assert "cash_down_present" in warning_codes
    assert "invalid_totals" not in warning_codes
    assert result.payload["reconciliation"]["unexplained_variance"] == 0.0


def test_item_returns_and_cash_over_together_reconcile_to_accepted_with_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "T/Cash: 100",
                "T/Card: 50",
                "Item Returns: K5.00",
                "C/over: 10",
                "Return Type: Declined-card return",
                "Variance Reason: Till float correction",
                "Supervisor Confirmation: YES",
                "Total Sales: 155",
                "Z/Reading: 140",
                "Traffic: 12",
                "Served: 10",
            ]
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert result.payload["status"] == "accepted_with_warning"
    assert {"item_returns_present", "cash_over_present"} <= warning_codes
    assert "invalid_totals" not in warning_codes


def test_unexplained_z_mismatch_requires_review_with_sales_totals_mismatch() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-05-20",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T/Cash: 100",
                "T/Card: 50",
                "Total Sales: 155",
                "Z/Reading: 150",
            ]
        ),
        "metrics": {
            "gross_sales": 155.0,
            "cash_sales": 100.0,
            "eftpos_sales": 50.0,
        },
    }

    validation_result = validate_sales(payload)
    acceptance = decide_acceptance(
        "sales",
        validation_result=validation_result,
        work_item_payload={"confidence": 0.8, "status": "accepted"},
    )

    assert validation_result.rejection_codes == ["sales_totals_mismatch"]
    assert acceptance.governed_status() == "needs_review"


def test_duplicate_sales_totals_mismatch_is_rendered_once() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "sales_income",
            "feedback_context": {
                "diagnostics": {
                    "report_type": "sales_income",
                    "branch": "waigani",
                    "date": "2026-05-20",
                    "status": "needs_review",
                    "checks": [
                        {"name": "report_type_detected", "passed": True, "detail": "Day-End Sales Report"},
                        {"name": "branch_resolved", "passed": True, "detail": "waigani"},
                        {"name": "date_resolved", "passed": True, "detail": "2026-05-20"},
                    ],
                    "failed_rules": [
                        {
                            "code": "sales_totals_mismatch",
                            "reason": "Sales totals do not match till/payment totals.",
                            "expected": "Cash K150.00 | Card K50.00 | Sales K200.00",
                            "received": "Cash K100.00 | Card K50.00 | Sales K150.00",
                        },
                        {
                            "code": "sales_totals_mismatch",
                            "reason": "Sales totals do not match till/payment totals.",
                            "expected": "Cash K150.00 | Card K50.00 | Sales K200.00",
                            "received": "Cash K100.00 | Card K50.00 | Sales K150.00",
                        },
                    ],
                    "confidence": 0.92,
                }
            },
        }
    )

    assert rendered["response_text"].count("Sales totals do not match till/payment totals.") == 1


def test_parser_extracts_item_returns_cash_over_and_embedded_cash_variance() -> None:
    parsed_item_returns = parse_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "Item Returns: K25.00",
            ]
        )
    )
    parsed_cash_over = parse_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "C/over: K35.00",
            ]
        )
    )
    parsed_embedded_variance = parse_work_item(
        _sales_work_item(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "Supervisor Control Summary",
                "Cash variance: K35.00 cash over",
            ]
        )
    )

    assert parsed_item_returns.figures.item_returns == 25.0
    assert parsed_cash_over.figures.cash_over == 35.0
    assert parsed_embedded_variance.figures.cash_over == 35.0


def test_waigani_2026_05_20_sample_no_longer_false_flags_sales_totals_mismatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    lines = [
        "DAY-END SALES REPORT",
        "Branch: Waigani Branch",
        "Date: 20/05/2026",
        "Cash Sales: 4868.20",
        "Card Sales: 1607.00",
        "Total Item Returns: K46.00",
        "Supervisor Control Summary",
        "Total item returns today were K46.00",
        "Total Sales: 6475.20",
        "Z/Reading: 6521.20",
        "Traffic: 120",
        "Served: 98",
    ]

    result = process_work_item(_sales_work_item(lines))
    validation_result = validate_sales(
        {
            "branch": "waigani",
            "report_date": "2026-05-20",
            "raw_text": "\n".join(lines),
            "metrics": {
                "gross_sales": 6475.20,
                "cash_sales": 4868.20,
                "eftpos_sales": 1607.00,
                "item_returns": 46.00,
                "z_reading": 6521.20,
            },
        }
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert result.payload["status"] == "accepted_with_warning"
    assert "invalid_totals" not in warning_codes
    assert validation_result.accepted is True
    assert validation_result.rejection_codes == []


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
