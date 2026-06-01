"""Scoring and accountability helpers for HR specialist outputs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from apps.hr_agent.parser import ParsedHrReport
from apps.hr_agent.warnings import is_blocking_accountability_rule, resolve_accountability_rule
from packages.common.warnings import WarningEntry

_DETAILED_STATUS_ORDER = (
    "present",
    "present_half",
    "off",
    "leave",
    "suspend",
    "late",
    "awn",
    "awon",
    "sick",
    "lay_off",
    "non_active",
    "absent",
    "transfer",
    "nil",
    "unknown",
)
_SUMMARY_STATUS_ORDER = (
    "present",
    "off",
    "leave",
    "suspend",
    "late",
    "awn",
    "awon",
    "sick",
    "lay_off",
    "non_active",
    "absent",
    "transfer",
)
_SUMMARY_OVERLAP_OPTIONAL_STATUSES = {"late"}
_HEADCOUNT_EXCLUDED_STATUSES = {"non_active"}
_STATUS_DISPLAY_LABELS = {
    "present": "PRESENT",
    "present_half": "PRESENT_HALF",
    "present_full": "PRESENT_FULL",
    "off": "DAY_OFF",
    "leave": "LEAVE",
    "suspend": "SUSPEND",
    "late": "LATE",
    "awn": "AWN",
    "awon": "AWON",
    "sick": "SICK",
    "lay_off": "LAY_OFF",
    "non_active": "NON_ACTIVE",
    "absent": "ABSENT",
    "transfer": "TRANSFER",
    "nil": "NIL",
    "unknown": "UNKNOWN",
}


def compute_performance_score(*, items_moved: int, assisting_count: int) -> float:
    """Return a small derived activity score for a performance record."""

    return round(items_moved + (assisting_count * 0.5), 2)


def attendance_presence_score(status: str) -> float:
    """Return a conservative binary presence score for attendance records."""

    if status == "present_half":
        return 0.5
    return 1 if status == "present" else 0


def compute_attendance_confidence(
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
    status: str,
) -> float:
    """Return a confidence score for the normalized attendance parse."""

    if status == "invalid_input":
        return 0.0

    confidence = 1.0
    if parsed.branch_slug is None:
        confidence -= 0.2
    if parsed.report_date is None:
        confidence -= 0.2
    if not parsed.records and not parsed.declared_status_totals:
        confidence -= 0.4
    elif not parsed.records:
        confidence -= 0.1

    penalties = {
        "missing_branch": 0.25,
        "missing_report_date": 0.25,
        "no_attendance_rows": 0.25,
        "unknown_attendance_status": 0.1,
    }
    for warning in warnings:
        if warning.severity != "warning":
            continue
        confidence -= penalties.get(warning.code, 0.0)

    return round(max(confidence, 0.0), 2)


def calculate_attendance_totals(parsed: ParsedHrReport) -> dict[str, int | float]:
    """Return detailed attendance totals from normalized staff rows."""

    totals = {status: 0 for status in _DETAILED_STATUS_ORDER}
    totals["present_full"] = 0
    totals["effective_present"] = 0.0
    totals["total_staff_listed"] = 0
    totals["total_staff"] = 0
    for record in parsed.records:
        status = record.status if record.status in totals else "unknown"
        totals[status] += 1
        totals["total_staff_listed"] += 1
    totals["present_full"] = int(totals["present"])
    totals["effective_present"] = round(int(totals["present"]) + (int(totals["present_half"]) * 0.5), 2)
    totals["total_staff"] = max(
        int(totals["total_staff_listed"]) - sum(int(totals.get(status, 0)) for status in _HEADCOUNT_EXCLUDED_STATUSES),
        0,
    )
    return totals


def declared_attendance_totals(parsed: ParsedHrReport) -> dict[str, int | None]:
    """Return detailed declared attendance totals from summary lines."""

    totals: dict[str, int | None] = {status: 0 for status in _SUMMARY_STATUS_ORDER}
    for status, count in parsed.declared_status_totals.items():
        if status == "not_at_work":
            continue
        if status not in totals:
            continue
        totals[status] = int(totals.get(status, 0) or 0) + count
    totals["declared_reconciled_total"] = sum(int(totals.get(status, 0) or 0) for status in _SUMMARY_STATUS_ORDER)
    totals["total_staff"] = parsed.declared_total_staff
    return totals


def resolve_attendance_validation_issue(
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
    status: str,
) -> dict[str, Any] | None:
    """Return the exact blocking attendance validation issue when one exists."""

    calculated_totals = calculate_attendance_totals(parsed)
    declared_totals = declared_attendance_totals(parsed)
    failing_rule = _primary_attendance_failure_rule(
        parsed=parsed,
        warnings=warnings,
        status=status,
        calculated_totals=calculated_totals,
        declared_totals=declared_totals,
    )
    if failing_rule is None:
        return None

    rule = resolve_accountability_rule(failing_rule)
    message = _reason_detail(
        rule.rule,
        parsed=parsed,
        warnings=warnings,
        calculated_totals=calculated_totals,
        declared_totals=declared_totals,
    )
    return {
        "code": rule.rule,
        "message": message,
        "layer": rule.layer,
        "calculated_totals": calculated_totals,
        "declared_totals": declared_totals,
        "recommended_correction": _recommended_correction(
            rule.rule,
            parsed=parsed,
            calculated_totals=calculated_totals,
            declared_totals=declared_totals,
            default=rule.recommended_correction,
        ),
    }


def build_attendance_accountability(
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
    status: str,
    confidence: float,
) -> dict[str, Any]:
    """Return one explicit accountability payload for review or rejection outputs."""

    issue = resolve_attendance_validation_issue(
        parsed=parsed,
        warnings=warnings,
        status=status,
    )
    if issue is None:
        issue = {
            "code": "validation_review_required",
            "message": "The attendance report requires manual review.",
            "layer": "hr_validation",
            "calculated_totals": calculate_attendance_totals(parsed),
            "declared_totals": declared_attendance_totals(parsed),
            "recommended_correction": resolve_accountability_rule("validation_review_required").recommended_correction,
        }

    return {
        "failing_layer": issue["layer"],
        "failing_rule": issue["code"],
        "validation_error_code": issue["code"],
        "validation_error_message": issue["message"],
        "normalized_values_attempted": list(parsed.normalization_attempts),
        "calculated_totals": issue["calculated_totals"],
        "declared_totals": issue["declared_totals"],
        "final_confidence_score": confidence,
        "reason_detail": issue["message"],
        "recommended_correction": issue["recommended_correction"],
    }


def attendance_validation_issue_is_blocking(issue: Mapping[str, Any] | None) -> bool:
    """Return whether one attendance validation issue should block acceptance."""

    if not isinstance(issue, Mapping):
        return False
    code = issue.get("validation_error_code") or issue.get("code") or issue.get("failing_rule")
    if not isinstance(code, str) or not code.strip():
        return False
    return is_blocking_accountability_rule(code.strip())


def _primary_attendance_failure_rule(
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
    status: str,
    calculated_totals: Mapping[str, int | float],
    declared_totals: Mapping[str, int | None],
) -> str | None:
    warning_codes = {warning.code for warning in warnings}
    if "parser_failure" in warning_codes:
        return "parser_failure"
    if "missing_branch" in warning_codes or parsed.branch_slug is None:
        return "missing_branch"
    if "missing_report_date" in warning_codes or parsed.report_date is None:
        return "missing_report_date"
    if "no_attendance_rows" in warning_codes or (not parsed.records and not parsed.declared_status_totals):
        return "no_attendance_rows"
    if "unknown_attendance_status" in warning_codes or calculated_totals["unknown"] > 0:
        return "unknown_attendance_status"
    if _declared_summary_total_mismatch(parsed=parsed, declared_totals=declared_totals):
        return "declared_summary_total_mismatch"
    if parsed.declared_total_staff is not None and calculated_totals["total_staff"] != parsed.declared_total_staff:
        return "declared_total_staff_mismatch"
    if _detailed_mismatch_detail(parsed=parsed, calculated_totals=calculated_totals):
        return "attendance_totals_mismatch"
    if "duplicate_staff_names" in warning_codes:
        return "duplicate_staff_names"
    if status in {"needs_review", "invalid_input"}:
        return "confidence_review_required"
    return None


def _declared_summary_total_mismatch(
    *,
    parsed: ParsedHrReport,
    declared_totals: Mapping[str, int | None],
) -> bool:
    if parsed.declared_total_staff is None or not parsed.declared_status_totals:
        return False
    minimum_total, maximum_total = _declared_summary_total_bounds(declared_totals)
    declared_total_staff = int(parsed.declared_total_staff)
    return declared_total_staff < minimum_total or declared_total_staff > maximum_total


def _declared_summary_total_bounds(declared_totals: Mapping[str, int | None]) -> tuple[int, int]:
    minimum_total = 0
    maximum_total = 0
    for status in _SUMMARY_STATUS_ORDER:
        count = int(declared_totals.get(status, 0) or 0)
        maximum_total += count
        if status not in _SUMMARY_OVERLAP_OPTIONAL_STATUSES and status not in _HEADCOUNT_EXCLUDED_STATUSES:
            minimum_total += count
    return minimum_total, maximum_total


def _detailed_mismatch_detail(
    *,
    parsed: ParsedHrReport,
    calculated_totals: Mapping[str, int | float],
) -> list[str]:
    mismatches: list[str] = []
    for status, declared_count in sorted(parsed.declared_status_totals.items()):
        if status == "not_at_work":
            continue
        actual_count = int(calculated_totals.get(status, 0))
        if actual_count != declared_count:
            mismatches.append(
                f"{_status_label(status)} declared {declared_count} but calculated {actual_count}"
            )
    return mismatches


def _reason_detail(
    rule: str,
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
    calculated_totals: Mapping[str, int | float],
    declared_totals: Mapping[str, int | None],
) -> str:
    if rule == "missing_branch":
        return "Branch could not be resolved from the attendance report."
    if rule == "missing_report_date":
        return "Report date could not be resolved from the attendance report."
    if rule == "no_attendance_rows":
        return "No attendance rows or usable declared attendance totals were extracted."
    if rule == "parser_failure":
        return "The attendance report could not be parsed safely."
    if rule == "declared_summary_total_mismatch":
        return (
            f"Declared attendance summary {_summary_expression(declared_totals, calculated_totals)} = "
            f"{declared_totals.get('declared_reconciled_total')} but TOTAL_STAFF = {declared_totals.get('total_staff')}."
        )
    if rule == "declared_total_staff_mismatch":
        listed_total = int(calculated_totals.get("total_staff_listed", 0))
        active_total = int(calculated_totals.get("total_staff", 0))
        headcount_excluded_total = sum(int(calculated_totals.get(status, 0)) for status in _HEADCOUNT_EXCLUDED_STATUSES)
        if headcount_excluded_total > 0:
            return (
                f"Normalized attendance rows total {listed_total} with NON_ACTIVE = {headcount_excluded_total}, "
                f"so ACTIVE TOTAL_STAFF = {active_total}, but declared TOTAL_STAFF = {parsed.declared_total_staff}."
            )
        return (
            f"Normalized attendance rows total {calculated_totals.get('total_staff')} but TOTAL_STAFF = "
            f"{parsed.declared_total_staff}."
        )
    if rule == "attendance_totals_mismatch":
        return "Declared attendance totals do not match normalized staff rows: " + "; ".join(
            _detailed_mismatch_detail(parsed=parsed, calculated_totals=calculated_totals)
        ) + "."
    if rule == "unknown_attendance_status":
        unknown_count = calculated_totals.get("unknown", 0)
        return f"{unknown_count} attendance row(s) still used an unknown status after normalization."
    if rule == "duplicate_staff_names":
        for warning in warnings:
            if warning.code == "duplicate_staff_names":
                return warning.message
    if rule == "confidence_review_required":
        return "The attendance report remained below the auto-accept confidence floor after normalization."
    return "The attendance report requires manual review."


def _recommended_correction(
    rule: str,
    *,
    parsed: ParsedHrReport,
    calculated_totals: Mapping[str, int | float],
    declared_totals: Mapping[str, int | None],
    default: str,
) -> str:
    if rule == "declared_summary_total_mismatch":
        return (
            f"Correct the declared summary so {_summary_expression(declared_totals, calculated_totals)} = "
            f"TOTAL_STAFF {declared_totals.get('total_staff')}."
        )
    if rule == "declared_total_staff_mismatch":
        headcount_excluded_total = sum(int(calculated_totals.get(status, 0)) for status in _HEADCOUNT_EXCLUDED_STATUSES)
        if headcount_excluded_total > 0:
            return (
                f"Correct TOTAL_STAFF to {calculated_totals.get('total_staff')} or move NON_ACTIVE rows out of the "
                f"attendance list if they should not affect the listed headcount."
            )
        return (
            f"Correct TOTAL_STAFF to {calculated_totals.get('total_staff')} or update the attendance rows so the "
            f"normalized row count matches {parsed.declared_total_staff}."
        )
    if rule == "attendance_totals_mismatch":
        return "Correct the declared attendance totals to match the normalized staff rows: " + _status_summary(
            calculated_totals=calculated_totals,
            declared_statuses=parsed.declared_status_totals.keys(),
            total_staff=calculated_totals.get("total_staff"),
        ) + "."
    if rule == "unknown_attendance_status":
        return (
            "Replace unsupported status tokens with PRESENT, P_HALF, DAY_OFF, LEAVE, ABSENT, SUSPEND, "
            "LATE, AWN, AWON, SICK, or LAY_OFF."
        )
    return default


def _summary_expression(
    declared_totals: Mapping[str, int | None],
    calculated_totals: Mapping[str, int | float],
) -> str:
    labels = [
        _status_label(status)
        for status in _SUMMARY_STATUS_ORDER
        if int(declared_totals.get(status, 0) or 0) > 0 or int(calculated_totals.get(status, 0)) > 0
    ]
    if not labels:
        labels = [_status_label(status) for status in ("present", "leave", "absent")]
    return " + ".join(labels)


def _status_summary(
    *,
    calculated_totals: Mapping[str, int | float],
    declared_statuses,
    total_staff: int | None,
) -> str:
    labels: list[str] = []
    seen_statuses = {
        status
        for status in declared_statuses
        if isinstance(status, str) and status != "not_at_work"
    }
    if not seen_statuses:
        seen_statuses = {status for status in _SUMMARY_STATUS_ORDER if int(calculated_totals.get(status, 0)) > 0}
    for status in _SUMMARY_STATUS_ORDER:
        if status not in seen_statuses and int(calculated_totals.get(status, 0)) == 0:
            continue
        labels.append(f"{_status_label(status)} {calculated_totals.get(status, 0)}")
    if total_staff is not None:
        labels.append(f"TOTAL_STAFF {total_staff}")
    return ", ".join(labels)


def _status_label(status: str) -> str:
    return _STATUS_DISPLAY_LABELS.get(status, status.replace("_", " ").upper())
