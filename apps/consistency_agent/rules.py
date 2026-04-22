"""Rules-based consistency checks over structured upstream records."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from packages.sop_validation import rejection_codes
from packages.validation import build_rejection


def run_rules(*, branch: str, report_date: str, records: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Run deterministic consistency rules for one branch/day."""

    issues: list[dict[str, Any]] = []
    sales = _mapping(records.get("sales_income"))
    attendance = _mapping(records.get("hr_attendance"))
    pricing = _mapping(records.get("pricing_stock_release"))
    supervisor = _mapping(records.get("supervisor_control"))

    sales_metrics = _mapping(sales.get("metrics"))
    traffic = sales_metrics.get("traffic")
    served = sales_metrics.get("served")
    if isinstance(traffic, (int, float)) and isinstance(served, (int, float)) and served > traffic:
        issues.append(
            _issue(
                branch=branch,
                report_date=report_date,
                report_type="sales_income",
                reason_code=rejection_codes.INVALID_COUNT_MISMATCH,
                reason_detail="Sales served count exceeds traffic.",
            )
        )

    attendance_metrics = _mapping(attendance.get("metrics"))
    total_staff = attendance_metrics.get("total_staff_listed")
    active_count = attendance_metrics.get("active_count")
    if isinstance(total_staff, (int, float)) and isinstance(active_count, (int, float)) and active_count > total_staff:
        issues.append(
            _issue(
                branch=branch,
                report_date=report_date,
                report_type="hr_attendance",
                reason_code=rejection_codes.INVALID_COUNT_MISMATCH,
                reason_detail="Attendance active count exceeds total staff listed.",
            )
        )

    pricing_metrics = _mapping(pricing.get("metrics"))
    processed = pricing_metrics.get("bales_processed")
    released = pricing_metrics.get("bales_released")
    if isinstance(processed, (int, float)) and isinstance(released, (int, float)) and released > processed:
        issues.append(
            _issue(
                branch=branch,
                report_date=report_date,
                report_type="pricing_stock_release",
                reason_code=rejection_codes.INVALID_COUNT_MISMATCH,
                reason_detail="Released bale count exceeds processed bale count.",
            )
        )

    supervisor_metrics = _mapping(supervisor.get("metrics"))
    escalated = supervisor_metrics.get("escalated_count")
    exceptions = supervisor_metrics.get("exception_count")
    if isinstance(escalated, (int, float)) and isinstance(exceptions, (int, float)) and escalated > exceptions:
        issues.append(
            _issue(
                branch=branch,
                report_date=report_date,
                report_type="supervisor_control",
                reason_code=rejection_codes.CONSISTENCY_CONFLICT,
                reason_detail="Escalated supervisor controls exceed exception count.",
            )
        )

    gross_sales = sales_metrics.get("gross_sales")
    if isinstance(gross_sales, (int, float)) and gross_sales > 0 and isinstance(active_count, (int, float)) and active_count <= 0:
        issues.append(
            _issue(
                branch=branch,
                report_date=report_date,
                report_type="cross_record",
                reason_code=rejection_codes.CONSISTENCY_CONFLICT,
                reason_detail="Positive sales were recorded with zero active staff.",
            )
        )

    return issues


def _issue(*, branch: str, report_date: str, report_type: str, reason_code: str, reason_detail: str) -> dict[str, Any]:
    payload = build_rejection(reason_code=reason_code, reason_detail=reason_detail)
    payload.update(
        {
            "branch": branch,
            "report_date": report_date,
            "report_type": report_type,
            "severity": "warning",
        }
    )
    return payload


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}
