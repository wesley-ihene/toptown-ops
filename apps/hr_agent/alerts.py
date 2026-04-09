"""Warning generation helpers for HR attendance reports."""

from __future__ import annotations

from apps.hr_agent.attendance import AttendanceSummary
from apps.hr_agent.coverage import CoverageSummary
from apps.hr_agent.parser import ParsedHrReport
from apps.hr_agent.staffing import StaffingSummary
from packages.common.warnings import WarningEntry, make_warning


def generate_alerts(
    *,
    parsed: ParsedHrReport,
    attendance: AttendanceSummary,
    staffing: StaffingSummary,
    coverage: CoverageSummary,
) -> list[WarningEntry]:
    """Return warnings from parsed and derived HR attendance state."""

    warnings: list[WarningEntry] = []

    if parsed.branch is None or parsed.report_date is None:
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch or report date is required for HR attendance signaling.",
            )
        )

    if attendance.total_staff_records == 0:
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="No attendance totals were available for HR staffing calculations.",
            )
        )

    if staffing.declared_total_staff is not None and staffing.declared_total_staff != staffing.total_staff_listed:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message=(
                    f"Declared total staff {staffing.declared_total_staff} does not match listed total "
                    f"{staffing.total_staff_listed}."
                ),
            )
        )

    if staffing.attendance_gap > 0:
        warnings.append(
            make_warning(
                code="attendance_gap_present",
                severity="warning",
                message=(
                    f"Attendance gap detected: {staffing.attendance_gap} staff were not active."
                ),
            )
        )

    if attendance.status_totals.get("unknown", 0) > 0:
        warnings.append(
            make_warning(
                code="unknown_attendance_status",
                severity="warning",
                message="One or more attendance lines used an unknown attendance status.",
            )
        )

    if attendance.total_staff_records > 0 and coverage.coverage_ratio < 0.6:
        warnings.append(
            make_warning(
                code="low_coverage",
                severity="warning",
                message=f"Coverage ratio {coverage.coverage_ratio:.2f} is below the safe threshold.",
            )
        )

    return warnings
