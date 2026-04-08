"""Attendance summary helpers for HR reports."""

from __future__ import annotations

from apps.hr_agent.figures import AttendanceFigures, AttendanceRecord
from apps.hr_agent.warnings import WarningEntry, make_warning

_KNOWN_STATUSES = (
    "present",
    "off",
    "annual_leave",
    "suspended",
    "absent",
    "sick",
)


def summarize_attendance(
    records: list[AttendanceRecord],
    *,
    declared_status_totals: dict[str, int],
) -> tuple[AttendanceFigures, list[WarningEntry]]:
    """Return summarized attendance figures and reconciliation warnings."""

    figures = AttendanceFigures(
        parsed_record_count=len(records),
        declared_status_totals=dict(declared_status_totals),
    )
    for record in records:
        if record.status in figures.parsed_status_totals:
            figures.parsed_status_totals[record.status] += 1

    warnings: list[WarningEntry] = []
    for status in _KNOWN_STATUSES:
        declared_count = declared_status_totals.get(status)
        if declared_count is None:
            continue
        parsed_count = figures.parsed_status_totals[status]
        if declared_count != parsed_count:
            warnings.append(
                make_warning(
                    code="data_mismatch",
                    severity="warning",
                    message=(
                        f"Declared {status} total {declared_count} does not match parsed total "
                        f"{parsed_count}."
                    ),
                )
            )

    return figures, warnings
