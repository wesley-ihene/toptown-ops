"""Attendance derivation helpers for HR reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.hr_agent.normalizer import STATUS_BUCKETS
from apps.hr_agent.parser import ParsedHrReport


@dataclass(slots=True)
class AttendanceSummary:
    """Derived attendance totals from parsed HR input."""

    status_totals: dict[str, int] = field(default_factory=dict)
    present_count: int = 0
    present_half_count: int = 0
    absent_count: int = 0
    off_count: int = 0
    leave_count: int = 0
    non_active_count: int = 0
    total_staff_records: int = 0
    active_count: int = 0
    effective_present: float = 0.0


def derive_attendance(parsed: ParsedHrReport) -> AttendanceSummary:
    """Return attendance totals from parsed records or declared summary counts."""

    status_totals = {
        "present": 0,
        "present_half": 0,
        "absent": 0,
        "off": 0,
        "leave": 0,
        "sick": 0,
        "suspend": 0,
        "awn": 0,
        "awon": 0,
        "lay_off": 0,
        "transfer": 0,
        "late": 0,
        "non_active": 0,
        "nil": 0,
        "unknown": 0,
    }
    grouped_totals = {
        "present": 0,
        "present_half": 0,
        "absent": 0,
        "off": 0,
        "leave": 0,
        "non_active": 0,
        "unknown": 0,
    }

    for record in parsed.records:
        status_totals[record.status] = status_totals.get(record.status, 0) + 1
        bucket = STATUS_BUCKETS.get(record.status, "unknown")
        grouped_totals[bucket] = grouped_totals.get(bucket, 0) + 1

    total_staff_records = sum(status_totals.values())
    return AttendanceSummary(
        status_totals=status_totals,
        present_count=grouped_totals["present"],
        present_half_count=grouped_totals["present_half"],
        absent_count=grouped_totals["absent"],
        off_count=grouped_totals["off"],
        leave_count=grouped_totals["leave"],
        non_active_count=grouped_totals["non_active"],
        total_staff_records=total_staff_records,
        active_count=grouped_totals["present"],
        effective_present=round(grouped_totals["present"] + (grouped_totals["present_half"] * 0.5), 2),
    )
