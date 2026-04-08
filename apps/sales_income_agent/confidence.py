"""Confidence scoring helpers for sales report extraction."""

from __future__ import annotations

from apps.sales_income_agent.warnings import WarningEntry


def compute_confidence(
    *,
    branch: str | None,
    report_date: str | None,
    totals_found: bool,
    critical_fields_complete: bool,
    warnings: list[WarningEntry],
) -> float:
    """Return a conservative confidence score for a parsed sales result."""

    confidence = 1.0
    if not branch:
        confidence -= 0.2
    if not report_date:
        confidence -= 0.2
    if not totals_found:
        confidence -= 0.2
    if not critical_fields_complete:
        confidence -= 0.2

    for warning in warnings:
        if warning.severity == "error":
            confidence -= 0.25
        elif warning.severity == "warning":
            confidence -= 0.1

    return round(max(confidence, 0.0), 2)
