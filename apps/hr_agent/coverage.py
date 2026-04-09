"""Coverage derivation helpers for HR attendance reports."""

from __future__ import annotations

from dataclasses import dataclass

from apps.hr_agent.staffing import StaffingSummary


@dataclass(slots=True)
class CoverageSummary:
    """Coverage ratios and staffing pressure derived from staffing totals."""

    coverage_ratio: float = 0.0


def derive_coverage(staffing: StaffingSummary) -> CoverageSummary:
    """Return simple branch coverage and staffing pressure metrics."""

    baseline_total = (
        staffing.declared_total_staff
        if staffing.declared_total_staff is not None and staffing.declared_total_staff > 0
        else staffing.total_staff_listed
    )
    if baseline_total > 0:
        coverage_ratio = round(staffing.active_count / baseline_total, 4)
    else:
        coverage_ratio = 0.0

    return CoverageSummary(
        coverage_ratio=coverage_ratio,
    )
