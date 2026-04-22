"""Tests for the rules-based consistency agent."""

from __future__ import annotations

from apps.consistency_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def test_consistency_agent_rejects_missing_records_mapping() -> None:
    result = process_work_item(
        WorkItem(
            kind="consistency_check",
            payload={"branch": "waigani", "report_date": "2026-04-07"},
        )
    )

    assert result.agent_name == "consistency_agent"
    assert result.payload["status"] == "invalid_input"
    assert result.metadata["validation"]["accepted"] is False
    assert result.metadata["validation"]["reason_codes"] == ["missing_records"]


def test_consistency_agent_reports_cross_record_issues() -> None:
    result = process_work_item(
        WorkItem(
            kind="consistency_check",
            payload={
                "branch": "waigani",
                "report_date": "2026-04-07",
                "records": {
                    "sales_income": {"metrics": {"traffic": 10, "served": 12, "gross_sales": 1200}},
                    "hr_attendance": {"metrics": {"total_staff_listed": 4, "active_count": 0}},
                    "pricing_stock_release": {"metrics": {"bales_processed": 1, "bales_released": 2}},
                    "supervisor_control": {"metrics": {"exception_count": 1, "escalated_count": 2}},
                },
            },
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["issue_count"] == 4
    assert {issue["report_type"] for issue in result.payload["issues"]} == {
        "sales_income",
        "pricing_stock_release",
        "supervisor_control",
        "cross_record",
    }
    assert result.metadata["validation"]["accepted"] is False
    assert result.metadata["validation"]["status"] == "issues_found"
