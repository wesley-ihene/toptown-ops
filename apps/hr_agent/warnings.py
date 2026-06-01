"""Structured warning helpers for the HR agent."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class WarningEntry:
    """Structured warning object shared across the HR agent."""

    code: str
    severity: str
    message: str

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-safe warning payload."""

        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
        }


def make_warning(*, code: str, severity: str, message: str) -> WarningEntry:
    """Create a structured warning entry."""

    return WarningEntry(code=code, severity=severity, message=message)


def dedupe_warnings(warnings: list[WarningEntry]) -> list[WarningEntry]:
    """Return warnings de-duplicated by warning code."""

    unique: dict[str, WarningEntry] = {}
    for warning in warnings:
        if warning.code not in unique:
            unique[warning.code] = warning
    return list(unique.values())


@dataclass(slots=True, frozen=True)
class AccountabilityRule:
    """Stable rule metadata used by attendance accountability payloads."""

    rule: str
    layer: str
    recommended_correction: str
    blocking: bool = True


_ACCOUNTABILITY_RULES: dict[str, AccountabilityRule] = {
    "missing_input_contract_fields": AccountabilityRule(
        rule="missing_input_contract_fields",
        layer="hr_input_contract",
        recommended_correction="Provide a non-empty HR attendance message with classification, branch, date, and staff lines.",
    ),
    "missing_branch": AccountabilityRule(
        rule="missing_branch",
        layer="hr_parser",
        recommended_correction="Add a recognizable branch header or branch line before resubmitting the attendance report.",
    ),
    "missing_report_date": AccountabilityRule(
        rule="missing_report_date",
        layer="hr_parser",
        recommended_correction="Add a recognizable report date before resubmitting the attendance report.",
    ),
    "no_attendance_rows": AccountabilityRule(
        rule="no_attendance_rows",
        layer="hr_parser",
        recommended_correction="List each staff member on its own line with one attendance status.",
    ),
    "parser_failure": AccountabilityRule(
        rule="parser_failure",
        layer="hr_parser",
        recommended_correction="Resend the attendance report in plain text with clear branch, date, and staff lines.",
    ),
    "declared_summary_total_mismatch": AccountabilityRule(
        rule="declared_summary_total_mismatch",
        layer="hr_validation",
        recommended_correction="Correct the declared attendance summary so the normalized status totals add up to TOTAL_STAFF.",
    ),
    "declared_total_staff_mismatch": AccountabilityRule(
        rule="declared_total_staff_mismatch",
        layer="hr_validation",
        recommended_correction="Correct Total Staff or fix the attendance rows so the normalized total staff count matches.",
    ),
    "attendance_totals_mismatch": AccountabilityRule(
        rule="attendance_totals_mismatch",
        layer="hr_validation",
        recommended_correction="Correct the declared attendance summary counts so they match the normalized attendance rows.",
    ),
    "unknown_attendance_status": AccountabilityRule(
        rule="unknown_attendance_status",
        layer="hr_normalizer",
        recommended_correction="Replace unknown status tokens with supported attendance values such as PRESENT, DAY_OFF, LEAVE, ABSENT, SUSPEND, LATE, AWN, AWON, SICK, or LAY_OFF.",
    ),
    "duplicate_staff_names": AccountabilityRule(
        rule="duplicate_staff_names",
        layer="hr_validation",
        recommended_correction="Remove duplicate staff lines so each staff member appears only once.",
    ),
    "confidence_review_required": AccountabilityRule(
        rule="confidence_review_required",
        layer="hr_scoring",
        recommended_correction="Correct the flagged attendance fields and resend the report.",
        blocking=False,
    ),
    "validation_review_required": AccountabilityRule(
        rule="validation_review_required",
        layer="hr_validation",
        recommended_correction="Correct the flagged attendance fields and resend the report.",
        blocking=False,
    ),
}


def resolve_accountability_rule(rule: str) -> AccountabilityRule:
    """Return stable accountability metadata for one failing attendance rule."""

    return _ACCOUNTABILITY_RULES.get(
        rule,
        AccountabilityRule(
            rule=rule,
            layer="hr_validation",
            recommended_correction="Correct the flagged attendance fields and resend the report.",
        ),
    )


def is_blocking_accountability_rule(rule: str | None) -> bool:
    """Return whether one accountability rule should block auto-accept."""

    if not isinstance(rule, str) or not rule.strip():
        return False
    return resolve_accountability_rule(rule.strip()).blocking
