"""Staffing derivation helpers for HR attendance reports."""

from __future__ import annotations

from dataclasses import dataclass

from apps.hr_agent.attendance import AttendanceSummary


@dataclass(slots=True)
class StaffingSummary:
    """Derived staffing totals and simple adequacy measures."""

    total_staff_listed: int = 0
    active_count: int = 0
    declared_total_staff: int | None = None
    attendance_gap: int = 0


def derive_staffing(
    attendance: AttendanceSummary,
    *,
    declared_total_staff: int | None,
) -> StaffingSummary:
    """Return conservative staffing totals from attendance counts."""

    total_staff_listed = attendance.total_staff_records
    active_count = attendance.active_count
    baseline_total = declared_total_staff if declared_total_staff is not None else total_staff_listed
    attendance_gap = max(baseline_total - active_count, 0)

    return StaffingSummary(
        total_staff_listed=total_staff_listed,
        active_count=active_count,
        declared_total_staff=declared_total_staff,
        attendance_gap=attendance_gap,
    )
