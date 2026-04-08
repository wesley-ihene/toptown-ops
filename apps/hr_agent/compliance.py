"""Compliance checks for HR specialist outputs."""

from __future__ import annotations

from apps.hr_agent.figures import AttendanceRecord, PerformanceRecord
from apps.hr_agent.staff_identity import normalize_staff_name
from apps.hr_agent.warnings import WarningEntry, make_warning


def evaluate_performance_compliance(records: list[PerformanceRecord]) -> list[WarningEntry]:
    """Return conservative compliance warnings for performance records."""

    for record in records:
        if record.duty_status == "off_duty" and (record.items_moved > 0 or record.assisting_count > 0):
            return [
                make_warning(
                    code="compliance_issue",
                    severity="warning",
                    message="An off-duty staff record still reports moved items or assisting work.",
                )
            ]
    return []


def evaluate_attendance_compliance(records: list[AttendanceRecord]) -> list[WarningEntry]:
    """Return conservative compliance warnings for conflicting attendance records."""

    statuses_by_staff: dict[str, set[str]] = {}
    for record in records:
        normalized_name = normalize_staff_name(record.staff_name)
        if not normalized_name:
            continue
        statuses_by_staff.setdefault(normalized_name, set()).add(record.status)

    for statuses in statuses_by_staff.values():
        if len(statuses) > 1:
            return [
                make_warning(
                    code="compliance_issue",
                    severity="warning",
                    message="One or more staff members appears with conflicting attendance statuses.",
                )
            ]
    return []
