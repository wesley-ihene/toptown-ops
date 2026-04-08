"""Confidence scoring helpers for HR extraction."""

from __future__ import annotations

from apps.hr_agent.warnings import WarningEntry


def compute_confidence(
    *,
    branch: str | None,
    report_date: str | None,
    record_count: int,
    warnings: list[WarningEntry],
) -> float:
    """Return a conservative confidence score for HR parsing."""

    confidence = 1.0
    if not branch:
        confidence -= 0.2
    if not report_date:
        confidence -= 0.2
    if record_count <= 0:
        confidence -= 0.2

    for warning in warnings:
        if warning.severity == "error":
            confidence -= 0.2
        elif warning.severity == "warning":
            confidence -= 0.08

    return round(max(confidence, 0.0), 2)
