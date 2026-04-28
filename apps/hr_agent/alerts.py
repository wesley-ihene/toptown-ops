"""Warning generation helpers for HR attendance reports."""

from __future__ import annotations

from apps.hr_agent.attendance import AttendanceSummary
from apps.hr_agent.coverage import CoverageSummary
from apps.hr_agent.normalizer import STATUS_BUCKETS
from apps.hr_agent.parser import ParsedHrReport
from apps.hr_agent.staff_identity import duplicate_staff_names
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

    declared_bucket_totals = _declared_bucket_totals(parsed)
    actual_bucket_totals = {
        "present": attendance.present_count,
        "off": attendance.off_count,
        "leave": attendance.leave_count,
        "absent": attendance.absent_count,
    }
    mismatched_buckets = [
        f"{_bucket_label(bucket)} declared {declared} but parsed {actual_bucket_totals.get(bucket, 0)}"
        for bucket, declared in sorted(declared_bucket_totals.items())
        if actual_bucket_totals.get(bucket, 0) != declared
    ]
    if mismatched_buckets:
        warnings.append(
            make_warning(
                code="attendance_totals_mismatch",
                severity="warning",
                message=(
                    "Declared attendance counts do not match parsed staff statuses: "
                    + "; ".join(mismatched_buckets)
                    + "."
                ),
            )
        )

    duplicate_names = sorted(duplicate_staff_names([record.staff_name for record in parsed.records]))
    if duplicate_names:
        warnings.append(
            make_warning(
                code="duplicate_staff_names",
                severity="warning",
                message=(
                    "Duplicate staff names detected in attendance lines: "
                    + ", ".join(duplicate_names[:3])
                    + ("." if len(duplicate_names) <= 3 else ", and more.")
                ),
            )
        )

    if staffing.attendance_gap > 0:
        warnings.append(
            make_warning(
                code="attendance_inactive_info",
                severity="info",
                message=(
                    f"{staffing.attendance_gap} staff are not active today "
                    "(Day Off / Leave / Lay Off / Sick / Notice / Absent statuses)."
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
                severity="info",
                message=f"Coverage ratio {coverage.coverage_ratio:.2f} indicates reduced active staffing today.",
            )
        )

    return warnings


def _declared_bucket_totals(parsed: ParsedHrReport) -> dict[str, int]:
    """Return declared attendance totals grouped into reviewable attendance buckets."""

    grouped: dict[str, int] = {}
    for status, count in parsed.declared_status_totals.items():
        bucket = STATUS_BUCKETS.get(status)
        if bucket not in {"present", "off", "leave", "absent"}:
            continue
        grouped[bucket] = grouped.get(bucket, 0) + count
    return grouped


def _bucket_label(bucket: str) -> str:
    """Return a human-readable label for one grouped attendance bucket."""

    labels = {
        "present": "Present",
        "off": "Day Off",
        "leave": "Leave",
        "absent": "Absent",
    }
    return labels.get(bucket, bucket.replace("_", " ").title())
