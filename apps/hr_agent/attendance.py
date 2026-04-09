"""Attendance derivation helpers for HR reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.hr_agent.parser import ParsedHrReport


@dataclass(slots=True)
class AttendanceSummary:
    """Derived attendance totals from parsed HR input."""

    status_totals: dict[str, int] = field(default_factory=dict)
    present_count: int = 0
    absent_count: int = 0
    off_count: int = 0
    leave_count: int = 0
    total_staff_records: int = 0
    active_count: int = 0


def derive_attendance(parsed: ParsedHrReport) -> AttendanceSummary:
    """Return attendance totals from parsed records or declared summary counts."""

    status_totals = {
        "present": 0,
        "absent": 0,
        "off": 0,
        "leave": 0,
        "unknown": 0,
    }

    for record in parsed.records:
        status_totals[record.status] = status_totals.get(record.status, 0) + 1

    total_staff_records = sum(status_totals.values())
    return AttendanceSummary(
        status_totals=status_totals,
        present_count=status_totals["present"],
        absent_count=status_totals["absent"],
        off_count=status_totals["off"],
        leave_count=status_totals["leave"],
        total_staff_records=total_staff_records,
        active_count=status_totals["present"],
    )
