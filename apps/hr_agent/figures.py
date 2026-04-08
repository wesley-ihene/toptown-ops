"""Structured HR record and summary dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class PerformanceRecord:
    """One parsed staff-performance record."""

    record_number: int
    staff_name: str
    section: str | None = None
    raw_section: str | None = None
    role: str | None = None
    duty_status: str = "on_duty"
    performance_grade: int | None = None
    items_moved: int = 0
    assisting_count: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AttendanceRecord:
    """One parsed staff-attendance record."""

    record_number: int
    staff_name: str
    status: str
    raw_status: str | None = None
    section: str | None = None
    raw_section: str | None = None


@dataclass(slots=True)
class PerformanceFigures:
    """Performance totals derived from parsed records and declared totals."""

    parsed_record_count: int = 0
    parsed_items_moved: int = 0
    parsed_assisting_count: int = 0
    declared_record_count: int | None = None
    declared_items_moved: int | None = None
    declared_assisting_count: int | None = None


@dataclass(slots=True)
class AttendanceFigures:
    """Attendance totals derived from parsed records and declared summaries."""

    parsed_record_count: int = 0
    parsed_status_totals: dict[str, int] = field(
        default_factory=lambda: {
            "present": 0,
            "off": 0,
            "annual_leave": 0,
            "suspended": 0,
            "absent": 0,
            "sick": 0,
        }
    )
    declared_status_totals: dict[str, int] = field(default_factory=dict)
