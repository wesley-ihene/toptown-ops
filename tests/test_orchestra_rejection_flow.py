"""Phase 3 Orchestra routing tests for rejection feedback integration."""

from __future__ import annotations

from apps.orchestra.classifier import classify_work_item
from apps.orchestra.intake import intake_raw_message
from apps.orchestra.router import route_work_item


def test_valid_report_routes_normally() -> None:
    work_item = intake_raw_message(
        {
            "text": "DAY-END SALES REPORT\nGross Sales: 1200",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 700.0,
                "eftpos_sales": 500.0,
                "traffic": 12,
                "served": 10,
            },
        },
        received_at_utc="2026-04-07T00:00:00+00:00",
    )
    classify_work_item(work_item)

    result = route_work_item(work_item)

    assert result.destination == "income_agent"
    assert result.route_status == "routed"
    assert work_item.payload["routing"]["route_reason"] == "sales_reports_route_to_income_agent"
    assert work_item.payload["validation"]["accepted"] is True


def test_invalid_report_triggers_rejection_feedback_agent() -> None:
    work_item = intake_raw_message(
        {
            "text": "DAY-END SALES REPORT\nGross Sales: 1200",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 600.0,
                "eftpos_sales": 500.0,
                "mobile_money_sales": 50.0,
                "traffic": 10,
                "served": 12,
            },
        },
        received_at_utc="2026-04-07T00:00:00+00:00",
    )
    classify_work_item(work_item)

    result = route_work_item(work_item)

    assert result.destination == "rejection_feedback_agent"
    assert result.route_status == "rejected"
    assert result.work_item is work_item
    assert work_item.kind == "raw_message"
    assert work_item.payload["rejection_feedback"]["report_type"] == "sales"
    assert work_item.payload["rejection_feedback"]["channel"] == "whatsapp"
    assert work_item.payload["rejection_feedback"]["dry_run"] is True
    assert [entry["code"] for entry in work_item.payload["rejection_feedback"]["rejections"]] == [
        "invalid_totals",
        "invalid_numeric_value",
    ]


def test_mixed_report_triggers_split_feedback_agent() -> None:
    work_item = intake_raw_message(
        {
            "text": (
                "Sales:\n"
                "Revenue 100\n"
                "Cash up complete\n"
                "Supervisor Control:\n"
                "Checklist approved\n"
                "Inspection complete"
            )
        },
        received_at_utc="2026-04-07T00:00:00+00:00",
    )
    classify_work_item(work_item)

    result = route_work_item(work_item)

    assert result.destination == "rejection_feedback_agent"
    assert result.route_status == "needs_split"
    assert result.work_item is work_item
    assert work_item.kind == "raw_message"
    assert work_item.payload["rejection_feedback"]["report_type"] == "mixed"
    assert work_item.payload["rejection_feedback"]["dry_run"] is True
    assert work_item.payload["rejection_feedback"]["rejections"] == [
        {
            "code": "needs_split",
            "message": "Mixed reports must be split before specialist routing.",
        }
    ]
