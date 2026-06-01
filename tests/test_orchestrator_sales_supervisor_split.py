"""Targeted regression tests for mixed sales + supervisor control splits."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from apps.conversation_router.worker import route_conversation_response
from apps.orchestrator_agent.worker import process_work_item
from apps.response_engine.worker import render_whatsapp_response
from packages.signal_contracts.work_item import WorkItem


def test_orchestrator_processes_regression_sales_and_supervisor_summary_message(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _regression_mixed_sales_and_supervisor_summary_text()},
                "metadata": {
                    "received_at": "2026-04-28T13:00:00Z",
                    "sender": "regression-sales-supervisor",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "waigani.json"
    legacy_supervisor_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-28.json"

    assert result.payload["status"] == "accepted"
    assert result.payload["routing"]["review_reason"] is None
    assert sales_path.exists()
    assert supervisor_path.exists()
    assert not legacy_supervisor_path.exists()

    sales_payload = json.loads(sales_path.read_text(encoding="utf-8"))
    supervisor_payload = json.loads(supervisor_path.read_text(encoding="utf-8"))

    assert sales_payload["metrics"]["total_cash"] == 4167.60
    assert sales_payload["metrics"]["total_card"] == 759.00
    assert sales_payload["metrics"]["total_sales"] == 4926.60
    assert sales_payload["metrics"]["z_reading"] == 4926.60
    assert sales_payload["metrics"]["traffic"] == 288
    assert sales_payload["metrics"]["served"] == 148
    assert sales_payload["metrics"]["conversion_rate"] == 0.5139

    assert supervisor_payload["cash_variance"] is False
    assert supervisor_payload["staffing_issues"] is False
    assert supervisor_payload["stock_issues_affecting_sales"] is True
    assert supervisor_payload["pricing_or_system_issues"] is False
    assert "run out of stock" in supervisor_payload["stock_issues_affecting_sales_detail"]
    assert "did not break any bales" in supervisor_payload["stock_issues_affecting_sales_detail"]

    rendered = render_whatsapp_response(route_conversation_response(result))
    assert rendered["response_type"] == "accepted_ack"
    assert "✅ Mixed split reports received for WAIGANI, 28/04/26." in rendered["response_text"]
    assert "Processed: Day-End Sales Report, Supervisor Control Summary." in rendered["response_text"]
    assert "REVIEW REQUIRED" not in rendered["response_text"]
    assert "Please resend the report" not in rendered["response_text"]


def test_orchestrator_keeps_processed_sales_child_when_supervisor_summary_is_truncated(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_with_truncated_supervisor_summary_text()},
                "metadata": {
                    "received_at": "2026-04-28T13:20:00Z",
                    "sender": "partial-sales-supervisor",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "waigani.json"

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["routing"]["review_reason"] is None
    assert sales_path.exists()
    assert not supervisor_path.exists()

    rendered = render_whatsapp_response(route_conversation_response(result))
    assert rendered["response_type"] == "accepted_ack"
    assert "Processed: Day-End Sales Report." in rendered["response_text"]
    assert "Needs review: Supervisor Control Summary." in rendered["response_text"]
    assert 'Issue: incomplete after "Exceptions es\u2026"' in rendered["response_text"]
    assert "Resend only the Supervisor Control Summary as one complete report." in rendered["response_text"]
    assert "Please resend the report" not in rendered["response_text"]
    assert "TAOP REVIEW REQUIRED" not in rendered["response_text"]


def _patch_output_paths(tmp_path: Path, monkeypatch) -> None:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    monkeypatch.setattr("apps.supervisor_control_agent.worker.OUTBOX_PATH", outbox_path)


def _regression_mixed_sales_and_supervisor_summary_text() -> str:
    return "\n".join(
        [
            "TTC POM WAIGANI  Branch.",
            "Date: Tuesday 28/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Till#1: Main Shop",
            "Cashier: Dorothy Morofa",
            "Assistant : Privian Keiby",
            "",
            "T/Cash:K4,167.60",
            "T/Card: K759.00",
            "Z/Reading: K4,926.60",
            "Balance: Yes",
            "",
            "TOTALS",
            "Total Cash: K4,167.60",
            "Total Card: K759.00",
            "Total Sales: K4,926.60",
            "",
            "CUSTOMER COUNT",
            "Main Door: 288",
            "Guest/customer serve: 148",
            "",
            "Balanced by: Francis Ano",
            "",
            "ADDITIONAL INFORMATION.",
            "",
            "*Sales per labor hours = K27.60",
            "*Sale per customer:= K33.20",
            "*Conversion rate:=51 %",
            "",
            "--------------------------------",
            "",
            "Supervisor Control Summary",
            "Date: 28/04/26",
            "Branch: Waigani",
            "Supervisor: Francis Ano.",
            "",
            "Cash variance: None",
            "Staffing issues: No",
            "Stock issues affecting sales: We run out of stock so we did not break any bales today.",
            "Pricing or system issues:NO",
            "",
            "Exceptions escalated to Ops Manager:",
            "",
            "Supervisor confirmation:",
            "All material issues have been escalated.",
        ]
    )


def _sales_with_truncated_supervisor_summary_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 28/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 700",
            "Eftpos Sales: 500",
            "Traffic: 12",
            "Served: 9",
            "",
            "Supervisor Control Summary",
            "Cash variance: No",
            "Staffing issues: No",
            "Stock issues affecting sales: No",
            "Pricing or system issues: No",
            "Exceptions es\u2026",
        ]
    )
