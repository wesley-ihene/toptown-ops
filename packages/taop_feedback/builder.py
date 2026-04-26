"""Deterministic TAOP feedback rendering for WhatsApp replies."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
import re
from typing import Any
import unicodedata

from apps.pricing_stock_release_agent.parser import ParsedBaleSummary, parse_work_item
from packages.normalization.branches import CANONICAL_BRANCHES, normalize_branch
from packages.normalization.currency import normalize_money
from packages.normalization.dates import normalize_report_date
from packages.report_policy import get_report_policy
from packages.signal_contracts.work_item import WorkItem

_REPORT_LABELS = {
    "sales": "Day-End Sales Report",
    "sales_income": "Day-End Sales Report",
    "day_end_sales": "Day-End Sales Report",
    "attendance": "Staff Attendance Report",
    "staff_attendance": "Staff Attendance Report",
    "hr_attendance": "Staff Attendance Report",
    "hr_staffing": "Staff Attendance Report",
    "staff_performance": "Staff Performance Report",
    "hr_performance": "Staff Performance Report",
    "bale_summary": "Daily Bale Summary",
    "pricing_stock_release": "Daily Bale Summary",
    "supervisor_control": "Supervisor Control Report",
    "store_monitoring": "Store Monitoring Report",
}
_DUPLICATE_REPORT_LABELS = {
    "sales": "Day-End Sales Report",
    "sales_income": "Day-End Sales Report",
    "day_end_sales": "Day-End Sales Report",
    "attendance": "Staff Attendance Report",
    "staff_attendance": "Staff Attendance Report",
    "hr_attendance": "Staff Attendance Report",
    "pricing_stock_release": "Daily Bale Summary",
    "bale_summary": "Daily Bale Summary",
    "bale_release": "Daily Bale Summary",
    "staff_performance": "Staff Performance Report",
    "supervisor_control": "Supervisor Control Report",
    "store_monitoring": "Store Monitoring Report",
}
_BRANCH_LINE_PATTERN = re.compile(r"^\s*branch\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE | re.MULTILINE)
_DATE_LINE_PATTERN = re.compile(r"^\s*date\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE | re.MULTILINE)
_DAY_FIELD_PATTERN = re.compile(r"^\s*day\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE | re.MULTILINE)
_TOTAL_QTY_LINE_PATTERN = re.compile(
    r"^\s*total\s+(?:qty|quantity)\s*[:=-]\s*(.+?)\s*$",
    flags=re.IGNORECASE | re.MULTILINE,
)
_TOTAL_AMOUNT_LINE_PATTERN = re.compile(
    r"^\s*total\s+amount\s*[:=-]\s*(.+?)\s*$",
    flags=re.IGNORECASE | re.MULTILINE,
)
_CURRENCY_FRAGMENT_PATTERN = re.compile(
    r"(?P<fragment>(?:PGK\s*|K\s*)\d[\d,\s]*\.\d+|(?:PGK\s*|K\s*)\d[\d,\s]*)",
    flags=re.IGNORECASE,
)
_QTY_WITH_UNIT_PATTERN = re.compile(r"\b(?P<qty>\d+)\s*(?P<unit>pcs?|pce)\b", flags=re.IGNORECASE)


def build_report_feedback(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return structured TAOP feedback when the response can be upgraded safely."""

    response_type = _text(response_context.get("response_type"))
    if response_type is None:
        return None

    if response_type == "duplicate_notice":
        return build_duplicate_feedback(response_context)

    report_type = _canonical_report_type(_reported_type(response_context))
    if report_type == "bale_summary":
        return build_bale_summary_feedback(response_context)
    return None


def build_review_feedback(response_context: Mapping[str, Any]) -> str:
    """Render deterministic TAOP review feedback when bale-specific details are unavailable."""

    feedback_context = _mapping(response_context.get("feedback_context"))
    human_tolerance = _human_tolerance(feedback_context)
    report_type = _reported_type(response_context)
    report_label = _REPORT_LABELS.get(report_type or "", "Report")
    branch = _normalized_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
        _branch_from_text(_raw_text(feedback_context)),
    )
    report_date = _normalized_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        _report_date_from_text(_raw_text(feedback_context)),
    )
    confidence = _first_float(
        feedback_context.get("confidence"),
        _mapping(feedback_context.get("acceptance")).get("confidence"),
    )
    thresholds = _thresholds_for_report(report_type or "unknown", feedback_context)
    issues = _generic_review_issues(
        response_reason=_text(response_context.get("reason")),
        feedback_context=feedback_context,
        confidence=confidence,
        thresholds=thresholds,
    )
    validation = _generic_validation_results(report_label=report_label, branch=branch, report_date=report_date)
    normalized_lines = _normalized_by_taop_lines(human_tolerance)

    lines = [
        "⚠️ TAOP REVIEW REQUIRED",
        f"Report: {report_label}",
    ]
    if branch is not None:
        lines.append(f"Branch: {_upper_branch(branch)}")
    if report_date is not None:
        lines.append(f"Date: {_display_report_date(report_date)}")
    if normalized_lines:
        lines.extend(["", "NORMALIZED BY TAOP", *normalized_lines])
    lines.extend(
        [
            "",
            "VALIDATION",
            *validation,
            "",
            "ISSUES",
            *issues,
            "",
            "ACTION",
            "Please correct the issues above and resend.",
        ]
    )
    if confidence is not None:
        lines.extend(["", "CONFIDENCE", f"Score: {confidence:.2f}"])
        auto_accept_min = _float_or_none(thresholds.get("auto_accept_min"))
        if auto_accept_min is not None:
            lines.append(f"Auto-accept threshold: {auto_accept_min:.2f}")
    return "\n".join(lines)


def build_rejection_feedback(response_context: Mapping[str, Any]) -> str:
    """Render deterministic TAOP rejection feedback for unsupported or malformed inputs."""

    response_type = _text(response_context.get("response_type"))
    report_type = _reported_type(response_context)
    if report_type in {None, "unknown"}:
        return _unsupported_report_feedback()

    feedback_context = _mapping(response_context.get("feedback_context"))
    report_label = _REPORT_LABELS.get(report_type or "", "Report")
    branch = _normalized_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
        _branch_from_text(_raw_text(feedback_context)),
    )
    report_date = _normalized_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        _report_date_from_text(_raw_text(feedback_context)),
    )
    surfaced_reason = _generic_rejection_reason(_text(response_context.get("reason")))
    action_line = f"Resend using the exact SOP format for {report_label}."
    if response_type == "correction_repeat_fix_request":
        action_line = f"Resend with the missing corrections applied using the exact SOP format for {report_label}."

    lines = [
        "❌ TAOP REPORT REJECTED",
        f"Report: {report_label}",
    ]
    if branch is not None:
        lines.append(f"Branch: {_upper_branch(branch)}")
    if report_date is not None:
        lines.append(f"Date: {_display_report_date(report_date)}")
    lines.extend(
        [
            "",
            "ISSUES",
            f"1. {surfaced_reason}",
            "2. TAOP could not approve this report with the current format.",
            "",
            "ACTION",
            action_line,
        ]
    )
    return "\n".join(lines)


def build_bale_summary_feedback(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return deterministic TAOP feedback for bale-summary replies."""

    response_type = _text(response_context.get("response_type"))
    if response_type not in {
        "accepted_ack",
        "correction_accepted_ack",
        "review_ack",
        "correction_review_ack",
        "rejected_fix_request",
        "correction_repeat_fix_request",
    }:
        return None

    feedback_context = _mapping(response_context.get("feedback_context"))
    raw_text = _raw_text(feedback_context)
    parsed = _parse_bale_summary(raw_text) if raw_text is not None else None
    metrics = _mapping(feedback_context.get("metrics"))
    items = _mapping_list(feedback_context.get("items"))
    if not metrics and not items and parsed is None:
        return None

    branch = _resolved_branch(response_context, feedback_context, parsed)
    report_date = _resolved_report_date(response_context, feedback_context, parsed)
    branch_display = _upper_branch(branch)
    date_display = _display_report_date(report_date)
    decision = _decision_for_response_type(response_type)
    confidence = _first_float(
        feedback_context.get("confidence"),
        _mapping(feedback_context.get("acceptance")).get("confidence"),
    )
    thresholds = _thresholds_for_report(_canonical_report_type(_reported_type(response_context)) or "bale_summary", feedback_context)

    calculated_total_qty = _calculated_total_qty(metrics, items)
    calculated_total_amount = _calculated_total_amount(metrics, items)
    declared_total_qty = _declared_total_qty(raw_text, parsed)
    declared_total_amount = _declared_total_amount(raw_text, parsed)
    validation_results = _bale_validation_results(
        decision=decision,
        branch=branch,
        structured_output_path=_text(feedback_context.get("structured_output_path"))
        or _text(response_context.get("structured_output_path")),
        declared_total_qty=declared_total_qty,
        calculated_total_qty=calculated_total_qty,
        declared_total_amount=declared_total_amount,
        calculated_total_amount=calculated_total_amount,
    )
    issues_detected = _bale_issues(
        raw_text=raw_text,
        branch=branch,
        feedback_context=feedback_context,
        confidence=confidence,
        thresholds=thresholds,
        decision=decision,
        validation_results=validation_results,
        response_reason=_text(response_context.get("reason")),
    )
    action_required = _action_required(
        decision=decision,
        issues_detected=issues_detected,
        validation_results=validation_results,
    )
    normalized_summary = {
        "items": _item_count(items, parsed),
        "total_qty": calculated_total_qty,
        "total_amount": calculated_total_amount,
    }

    response_text = _render_bale_summary_feedback_text(
        response_type=response_type,
        branch_display=branch_display,
        date_display=date_display,
        validation_results=validation_results,
        issues_detected=issues_detected,
        action_required=action_required,
        normalized_summary=normalized_summary,
        confidence=confidence,
        thresholds=thresholds,
    )

    return {
        "report_type": "bale_summary",
        "branch": branch,
        "report_date": report_date,
        "decision": decision,
        "status": _text(response_context.get("governance_status")) or _text(feedback_context.get("status")),
        "confidence": confidence,
        "auto_accept_threshold": thresholds.get("auto_accept_min"),
        "review_threshold": thresholds.get("review_min"),
        "validation_results": validation_results,
        "issues_detected": issues_detected,
        "action_required": action_required,
        "normalized_summary": normalized_summary,
        "response_text": response_text,
    }


def build_duplicate_feedback(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return a more specific duplicate notice when scope can be inferred safely."""

    response_type = _text(response_context.get("response_type"))
    if response_type != "duplicate_notice":
        return None

    feedback_context = _mapping(response_context.get("feedback_context"))
    human_tolerance = _human_tolerance(feedback_context)
    raw_text = _raw_text(feedback_context)
    report_type = _duplicate_report_type(
        _reported_type(response_context),
        _text(response_context.get("signal_subtype")),
        _text(feedback_context.get("signal_subtype")),
        _report_type_from_human_tolerance(human_tolerance),
        _infer_report_type_from_text(raw_text),
    )
    if report_type is None:
        return None

    branch = _normalized_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
        _branch_from_text(raw_text),
    )
    report_date = _normalized_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        _report_date_from_text(raw_text),
    )

    report_label = _duplicate_report_label(report_type)
    lines = ["ℹ️ TAOP DUPLICATE REPORT DETECTED", f"Report: {report_label}"]
    if branch is not None:
        lines.append(f"Branch: {_upper_branch(branch)}")
    if report_date is not None:
        lines.append(f"Date: {_display_report_date(report_date)}")
    lines.extend(
        [
            "",
            "This report was already received and processed earlier.",
            "No new processing was applied.",
        ]
    )
    return {
        "report_type": report_type,
        "branch": branch,
        "report_date": report_date,
        "decision": "duplicate",
        "status": "duplicate",
        "confidence": None,
        "auto_accept_threshold": None,
        "review_threshold": None,
        "validation_results": [],
        "issues_detected": [],
        "action_required": "No new processing was applied.",
        "normalized_summary": None,
        "response_text": "\n".join(lines),
    }


def _unsupported_report_feedback() -> str:
    return "\n".join(
        [
            "❌ TAOP REPORT REJECTED",
            "Reason: Format does not match any supported report type.",
            "",
            "SUPPORTED REPORT TYPES",
            "1. DAY-END SALES REPORT",
            "2. DAILY BALE SUMMARY - RELEASED TO RAIL",
            "3. ATTENDANCE REPORT",
            "4. STAFF PERFORMANCE REPORT",
            "5. SUPERVISOR CONTROL REPORT",
            "6. STORE MONITORING REPORT",
            "",
            "ACTION",
            "If this is a question, ask it as a status request.",
            "If this is a report, resend using one exact report title above.",
        ]
    )


def _generic_validation_results(
    *,
    report_label: str,
    branch: str | None,
    report_date: str | None,
) -> list[str]:
    results = [f"✔ Report type detected: {report_label}"]
    results.append("✔ Branch resolved" if branch is not None else "❌ Branch not resolved")
    results.append("✔ Date resolved" if report_date is not None else "❌ Date not resolved")
    return results


def _generic_review_issues(
    *,
    response_reason: str | None,
    feedback_context: Mapping[str, Any],
    confidence: float | None,
    thresholds: Mapping[str, Any],
) -> list[str]:
    issues = _generic_issue_lines(feedback_context=feedback_context)
    auto_accept_min = _float_or_none(thresholds.get("auto_accept_min"))
    if not issues:
        issues = ["1. TAOP could not identify the exact validation issue from the current parser output."]
        if confidence is not None and auto_accept_min is not None:
            issues.append("2. Report was below auto-accept confidence threshold.")
        elif response_reason is not None:
            issues.append(f"2. {_generic_rejection_reason(response_reason)}")
        else:
            issues.append("2. Report was routed to review by the current validation result.")
        return issues

    numbered: list[str] = []
    for index, issue in enumerate(issues, start=1):
        numbered.append(f"{index}. {issue}")
    return numbered


def _generic_issue_lines(*, feedback_context: Mapping[str, Any]) -> list[str]:
    issue_lines: list[str] = []
    warnings = _mapping_list(feedback_context.get("warnings"))
    for warning in warnings:
        message = _text(warning.get("message"))
        if message is not None:
            issue_lines.append(message.rstrip("."))
    validation = _mapping(feedback_context.get("validation"))
    for rejection in _mapping_list(validation.get("rejections")):
        detail = _text(rejection.get("reason_detail")) or _text(rejection.get("message"))
        if detail is not None:
            issue_lines.append(detail.rstrip("."))
    return issue_lines[:3]


def _generic_rejection_reason(reason: str | None) -> str:
    mapping = {
        "validation_failed": "Required report fields were missing or invalid.",
        "confidence_below_reject_threshold": "Confidence was below the configured acceptance threshold.",
        "fallback_validation_failed": "Required report fields were missing or invalid.",
        "invalid_input": "Format does not match the expected SOP structure.",
        "unknown_report_type": "Format does not match any supported report type.",
    }
    if reason is None:
        return "Format does not match the expected SOP structure."
    return mapping.get(reason, reason.replace("_", " ").capitalize() + ".")


def _render_bale_summary_feedback_text(
    *,
    response_type: str,
    branch_display: str,
    date_display: str | None,
    validation_results: list[dict[str, Any]],
    issues_detected: list[dict[str, Any]],
    action_required: str,
    normalized_summary: dict[str, Any],
    confidence: float | None,
    thresholds: Mapping[str, Any],
) -> str:
    header = _bale_header(response_type=response_type, branch_display=branch_display)
    lines = [header]
    if date_display is not None:
        lines.append(f"Date: {date_display}")
    lines.append("")

    if response_type in {"review_ack", "correction_review_ack"}:
        lines.extend(["STATUS: ⚠️ REVIEW REQUIRED", ""])
    elif response_type in {"rejected_fix_request", "correction_repeat_fix_request"}:
        lines.extend(["STATUS: ❌ CORRECTION REQUIRED", ""])

    lines.append("VALIDATION RESULTS")
    lines.extend(_render_validation_results(validation_results))

    if response_type in {"accepted_ack", "correction_accepted_ack"}:
        lines.extend(
            [
                "",
                "SUMMARY",
                f"Items: {int(normalized_summary.get('items') or 0)}",
                f"Total Qty: {_format_qty_value(normalized_summary.get('total_qty'))}",
                f"Total Amount: {_format_money_value(normalized_summary.get('total_amount'))}",
                "",
                "No correction required.",
            ]
        )
        return "\n".join(lines)

    lines.append("")
    lines.append("ISSUES DETECTED")
    if issues_detected:
        lines.extend(_render_issues(issues_detected))
    else:
        lines.append("1. Review required:")
        lines.append("- Operator review was triggered for this bale summary.")

    lines.extend(["", "ACTION REQUIRED", action_required])
    if confidence is not None:
        lines.extend(
            [
                "",
                "CONFIDENCE",
                f"Score: {confidence:.2f}",
            ]
        )
        auto_accept_min = _float_or_none(thresholds.get("auto_accept_min"))
        if auto_accept_min is not None:
            lines.append(f"Auto-accept threshold: {auto_accept_min:.2f}")
        review_min = _float_or_none(thresholds.get("review_min"))
        if review_min is not None:
            lines.append(f"Review threshold: {review_min:.2f}")
    return "\n".join(lines)


def _bale_validation_results(
    *,
    decision: str,
    branch: str | None,
    structured_output_path: str | None,
    declared_total_qty: float | None,
    calculated_total_qty: float | None,
    declared_total_amount: float | None,
    calculated_total_amount: float | None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if declared_total_qty is not None and calculated_total_qty is not None:
        if _qty_matches(declared_total_qty, calculated_total_qty):
            results.append(
                {
                    "status": "pass",
                    "label": "Total Qty matches item sum",
                    "details": _format_qty_value(calculated_total_qty),
                }
            )
        else:
            results.append(
                {
                    "status": "fail",
                    "label": "Total Qty mismatch",
                    "declared": _format_plain_number(declared_total_qty),
                    "calculated": _format_plain_number(calculated_total_qty),
                    "difference": _format_plain_number(abs(calculated_total_qty - declared_total_qty)),
                }
            )
    elif calculated_total_qty is not None:
        results.append(
            {
                "status": "pass",
                "label": "Total Qty verified",
                "details": _format_qty_value(calculated_total_qty),
            }
        )

    if declared_total_amount is not None and calculated_total_amount is not None:
        if _money_matches(declared_total_amount, calculated_total_amount):
            results.append(
                {
                    "status": "pass",
                    "label": "Total Amount matches item sum",
                    "details": _format_money_value(calculated_total_amount),
                }
            )
        else:
            results.append(
                {
                    "status": "fail",
                    "label": "Total Amount mismatch",
                    "declared": _format_money_value(declared_total_amount),
                    "calculated": _format_money_value(calculated_total_amount),
                    "difference": _format_money_value(abs(calculated_total_amount - declared_total_amount)),
                }
            )
    elif calculated_total_amount is not None:
        results.append(
            {
                "status": "pass",
                "label": "Total Amount verified",
                "details": _format_money_value(calculated_total_amount),
            }
        )

    if decision == "accepted" and branch is not None:
        results.append({"status": "pass", "label": "Branch resolved", "details": None})
    if decision == "accepted" and structured_output_path is not None:
        results.append({"status": "pass", "label": "Report stored successfully", "details": None})
    return results


def _bale_issues(
    *,
    raw_text: str | None,
    branch: str | None,
    feedback_context: Mapping[str, Any],
    confidence: float | None,
    thresholds: Mapping[str, Any],
    decision: str,
    validation_results: list[dict[str, Any]],
    response_reason: str | None,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if raw_text:
        day_match = _DAY_FIELD_PATTERN.search(raw_text)
        if day_match is not None:
            issues.append(
                {
                    "code": "non_standard_field_day",
                    "title": "Non-standard field detected",
                    "details": [f"\"Day: {day_match.group(1).strip()}\" is not required by SOP"],
                }
            )

        currency_details = _currency_cleanup_details(raw_text)
        if currency_details:
            issues.append(
                {
                    "code": "currency_format_cleanup",
                    "title": "Currency formatting needs cleanup",
                    "details": currency_details,
                }
            )

        qty_details = _qty_cleanup_details(raw_text)
        if qty_details:
            issues.append(
                {
                    "code": "qty_format_inconsistent",
                    "title": "Quantity format inconsistent",
                    "details": qty_details,
                }
            )

        if not (_TOTAL_QTY_LINE_PATTERN.search(raw_text) and _TOTAL_AMOUNT_LINE_PATTERN.search(raw_text)):
            issues.append(
                {
                    "code": "missing_total_section",
                    "title": "Total section is incomplete",
                    "details": ["Include both `Total Qty` and `Total Amount` lines in the bale summary."],
                }
            )

        raw_branch = _branch_from_text(raw_text)
        normalized_branch = _normalized_branch_from_any(raw_branch, branch)
        if raw_branch is not None and normalized_branch is not None:
            canonical_display = CANONICAL_BRANCHES.get(normalized_branch, normalized_branch.replace("_", " ").title())
            if _text(raw_branch) != canonical_display and decision != "accepted":
                issues.append(
                    {
                        "code": "branch_needs_normalization",
                        "title": "Branch label needs cleanup",
                        "details": [f"Use the canonical branch name for {canonical_display} in the bale summary."],
                    }
                )

    warnings = _mapping_list(feedback_context.get("warnings"))
    for warning in warnings:
        code = _text(warning.get("code")) or "warning"
        message = _text(warning.get("message"))
        if code in {"data_mismatch", "missing_fields", "approval_backlog", "low_release_ratio", "financial_anomaly"} and message is not None:
            issues.append(
                {
                    "code": code,
                    "title": _warning_title(code),
                    "details": [message],
                }
            )

    validation = _mapping(feedback_context.get("validation"))
    for rejection in _mapping_list(validation.get("rejections")):
        code = _text(rejection.get("reason_code")) or _text(rejection.get("code"))
        detail = _text(rejection.get("reason_detail")) or _text(rejection.get("message"))
        if code in {"invalid_totals"}:
            continue
        if code is None or detail is None:
            continue
        issues.append(
            {
                "code": code,
                "title": _validation_issue_title(code),
                "details": [detail],
            }
        )

    if decision in {"review", "rejected"} and not issues and confidence is not None:
        auto_accept_min = _float_or_none(thresholds.get("auto_accept_min"))
        if auto_accept_min is not None and confidence < auto_accept_min:
            issues.append(
                {
                    "code": "confidence_below_auto_accept_threshold",
                    "title": "Confidence below auto-accept threshold",
                    "details": [f"Score {confidence:.2f} is below the auto-accept threshold of {auto_accept_min:.2f}."],
                }
            )
    if decision in {"review", "rejected"} and not issues and response_reason is not None:
        issues.append(
            {
                "code": response_reason,
                "title": "Review reason",
                "details": [_safe_reason_text(response_reason)],
            }
        )

    if any(result.get("status") == "fail" for result in validation_results):
        issues = [issue for issue in issues if issue.get("code") != "data_mismatch"]
    return _dedupe_issues(issues)


def _action_required(
    *,
    decision: str,
    issues_detected: list[dict[str, Any]],
    validation_results: list[dict[str, Any]],
) -> str:
    if decision == "accepted":
        return "No correction required."
    if decision == "duplicate":
        return "No new processing was applied."

    issue_codes = {str(issue.get("code")) for issue in issues_detected}
    has_totals_failure = any(result.get("status") == "fail" for result in validation_results)
    if has_totals_failure:
        return "Please correct the totals and resend using the standard bale summary format."
    if issue_codes & {"missing_fields", "missing_total_section"}:
        return "Please resend with all required bale rows, branch, date, total quantity, and total amount."
    if issue_codes:
        return "Please resend using the standard bale summary format."
    return "Please correct the flagged issues and resend using the standard bale summary format."


def _render_validation_results(results: list[dict[str, Any]]) -> list[str]:
    if not results:
        return ["- No validation details were available."]

    lines: list[str] = []
    for index, result in enumerate(results):
        status = result.get("status")
        label = str(result.get("label") or "Validation check")
        if status == "pass":
            details = _text(result.get("details"))
            lines.append(f"✔ {label}" + (f": {details}" if details is not None else ""))
        else:
            lines.append(f"❌ {label}")
            declared = _text(result.get("declared"))
            calculated = _text(result.get("calculated"))
            difference = _text(result.get("difference"))
            if declared is not None:
                lines.append(f"Declared: {declared}")
            if calculated is not None:
                lines.append(f"Calculated: {calculated}")
            if difference is not None:
                lines.append(f"Difference: {difference}")
        if index < len(results) - 1 and status != "pass":
            lines.append("")
    return lines


def _render_issues(issues: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for index, issue in enumerate(issues, start=1):
        title = str(issue.get("title") or "Issue detected")
        lines.append(f"{index}. {title}:")
        details = issue.get("details")
        if isinstance(details, list) and details:
            for detail in details:
                detail_text = _text(detail)
                if detail_text is not None:
                    lines.append(f"- {detail_text}")
        else:
            lines.append("- Review this bale summary and resend in the standard format.")
    return lines


def _bale_header(*, response_type: str, branch_display: str) -> str:
    if response_type in {"accepted_ack", "correction_accepted_ack"}:
        return f"✅ TAOP BALE SUMMARY ACCEPTED – {branch_display}"
    if response_type in {"review_ack", "correction_review_ack"}:
        return f"📊 TAOP BALE SUMMARY REVIEW – {branch_display}"
    return f"❌ TAOP BALE SUMMARY REJECTED – {branch_display}"


def _reported_type(response_context: Mapping[str, Any]) -> str | None:
    feedback_context = _mapping(response_context.get("feedback_context"))
    human_tolerance = _human_tolerance(feedback_context)
    return _duplicate_report_type(
        response_context.get("report_type"),
        response_context.get("signal_subtype"),
        response_context.get("signal_type"),
        feedback_context.get("report_type"),
        feedback_context.get("signal_subtype"),
        feedback_context.get("signal_type"),
        _report_type_from_human_tolerance(human_tolerance),
        _infer_report_type_from_text(_raw_text(feedback_context)),
    )


def _canonical_report_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().casefold()
    if normalized in {"bale_summary", "pricing_stock_release", "bale_release"}:
        return "bale_summary"
    return normalized or None


def _decision_for_response_type(response_type: str) -> str:
    if response_type in {"accepted_ack", "correction_accepted_ack"}:
        return "accepted"
    if response_type in {"review_ack", "correction_review_ack"}:
        return "review"
    if response_type == "duplicate_notice":
        return "duplicate"
    return "rejected"


def _thresholds_for_report(report_type: str, feedback_context: Mapping[str, Any]) -> dict[str, float | None]:
    acceptance = _mapping(feedback_context.get("acceptance"))
    thresholds = _mapping(acceptance.get("thresholds"))
    if thresholds:
        return {
            "auto_accept_min": _float_or_none(thresholds.get("auto_accept_min")),
            "review_min": _float_or_none(thresholds.get("review_min")),
            "reject_max": _float_or_none(thresholds.get("reject_max")),
        }
    try:
        policy = get_report_policy(report_type)
    except ValueError:
        return {"auto_accept_min": None, "review_min": None, "reject_max": None}
    return policy.confidence_thresholds.to_payload()


def _resolved_branch(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
    parsed: ParsedBaleSummary | None,
) -> str | None:
    return _normalized_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
        parsed.branch if parsed is not None else None,
        _branch_from_text(_raw_text(feedback_context)),
    )


def _resolved_report_date(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
    parsed: ParsedBaleSummary | None,
) -> str | None:
    return _normalized_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        parsed.report_date if parsed is not None else None,
        _report_date_from_text(_raw_text(feedback_context)),
    )


def _declared_total_qty(raw_text: str | None, parsed: ParsedBaleSummary | None) -> float | None:
    del parsed
    if raw_text is None:
        return None
    match = _TOTAL_QTY_LINE_PATTERN.search(raw_text)
    if match is None:
        return None
    return _parse_loose_number(match.group(1))


def _declared_total_amount(raw_text: str | None, parsed: ParsedBaleSummary | None) -> float | None:
    del parsed
    if raw_text is None:
        return None
    match = _TOTAL_AMOUNT_LINE_PATTERN.search(raw_text)
    if match is None:
        return None
    return _parse_loose_money(match.group(1))


def _calculated_total_qty(metrics: Mapping[str, Any], items: list[dict[str, Any]]) -> float | None:
    metrics_value = _float_or_none(metrics.get("total_qty"))
    if metrics_value is not None:
        return metrics_value
    total = 0.0
    found = False
    for item in items:
        qty = _float_or_none(item.get("qty"))
        if qty is None:
            continue
        total += qty
        found = True
    return total if found else None


def _calculated_total_amount(metrics: Mapping[str, Any], items: list[dict[str, Any]]) -> float | None:
    metrics_value = _float_or_none(metrics.get("total_amount"))
    if metrics_value is not None:
        return metrics_value
    total = 0.0
    found = False
    for item in items:
        amount = _float_or_none(item.get("amount"))
        if amount is None:
            continue
        total += amount
        found = True
    return round(total, 2) if found else None


def _item_count(items: list[dict[str, Any]], parsed: ParsedBaleSummary | None) -> int:
    if items:
        return len(items)
    if parsed is not None:
        return len(parsed.items)
    return 0


def _raw_text(feedback_context: Mapping[str, Any]) -> str | None:
    text = _text(feedback_context.get("raw_text"))
    if text is not None:
        return text
    raw_txt_path = _text(feedback_context.get("raw_txt_path"))
    if raw_txt_path is None:
        return None
    try:
        loaded = Path(raw_txt_path).read_text(encoding="utf-8")
    except OSError:
        return None
    stripped = loaded.strip()
    return stripped or None


def _human_tolerance(feedback_context: Mapping[str, Any]) -> Mapping[str, Any]:
    human_tolerance = _mapping(feedback_context.get("human_tolerance"))
    if human_tolerance:
        return human_tolerance
    raw_meta_path = _text(feedback_context.get("raw_meta_path"))
    if raw_meta_path is None:
        return {}
    loaded_meta = _load_json_mapping(Path(raw_meta_path))
    return _mapping(loaded_meta.get("human_tolerance"))


def _load_json_mapping(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        return {}
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _parse_bale_summary(raw_text: str) -> ParsedBaleSummary | None:
    try:
        return parse_work_item(
            WorkItem(
                kind="raw_message",
                payload={
                    "classification": {"report_type": "bale_summary"},
                    "raw_message": {"text": raw_text},
                },
            )
        )
    except Exception:
        return None


def _currency_cleanup_details(raw_text: str) -> list[str]:
    details: list[str] = []
    seen: set[str] = set()
    for match in _CURRENCY_FRAGMENT_PATTERN.finditer(raw_text):
        fragment = match.group("fragment").strip()
        if fragment in seen or ", " not in fragment:
            continue
        normalized_amount = _parse_loose_money(fragment)
        if normalized_amount is None:
            continue
        details.append(f"\"{fragment}\" should be \"{normalized_amount:.2f}\"")
        seen.add(fragment)
    return details


def _qty_cleanup_details(raw_text: str) -> list[str]:
    details: list[str] = []
    seen: set[str] = set()
    for match in _QTY_WITH_UNIT_PATTERN.finditer(raw_text):
        fragment = match.group(0).strip()
        if fragment in seen:
            continue
        qty = match.group("qty")
        details.append(f"Use numbers only, e.g. \"{qty}\" not \"{fragment}\"")
        seen.add(fragment)
    return details[:3]


def _warning_title(code: str) -> str:
    mapping = {
        "approval_backlog": "Approval backlog detected",
        "data_mismatch": "Totals or counts need review",
        "financial_anomaly": "Amount or quantity anomaly detected",
        "low_release_ratio": "Release ratio needs review",
        "missing_fields": "Required fields missing or incomplete",
    }
    return mapping.get(code, "Issue detected")


def _validation_issue_title(code: str) -> str:
    mapping = {
        "missing_branch": "Branch is missing",
        "missing_report_date": "Report date is missing",
        "invalid_report_date": "Report date format is invalid",
        "missing_metrics": "Metrics section is missing",
        "missing_items": "Item list is missing",
        "missing_required_field": "Required item field is missing",
        "invalid_numeric_value": "Numeric value is invalid",
        "invalid_count_mismatch": "Bale counts do not align",
    }
    return mapping.get(code, "Validation issue detected")


def _safe_reason_text(reason: str) -> str:
    if reason == "confidence_between_review_and_accept_thresholds":
        return "Confidence is below the auto-accept threshold and needs review."
    if reason == "validation_failed":
        return "The bale summary did not satisfy the required validation checks."
    return reason.replace("_", " ")


def _dedupe_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for issue in issues:
        code = str(issue.get("code") or "")
        title = str(issue.get("title") or "")
        key = (code, title)
        if key not in unique:
            unique[key] = issue
    return list(unique.values())


def _infer_report_type_from_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    normalized = unicodedata.normalize("NFKC", raw_text).casefold()
    if "daily bale summary" in normalized:
        return "bale_summary"
    if "day-end sales report" in normalized:
        return "sales_income"
    if "attendance report" in normalized or "staff attendance" in normalized or "staffs attendance" in normalized:
        return "staff_attendance"
    if "staff performance report" in normalized:
        return "staff_performance"
    if "supervisor control report" in normalized:
        return "supervisor_control"
    return None


def _report_type_from_human_tolerance(human_tolerance: Mapping[str, Any]) -> str | None:
    report_type_hint = _text(human_tolerance.get("report_type_hint"))
    if report_type_hint is not None:
        return report_type_hint
    normalized_fields = _mapping(human_tolerance.get("normalized_fields"))
    report_title = _text(normalized_fields.get("report_title"))
    if report_title == "ATTENDANCE REPORT":
        return "staff_attendance"
    return None


def _duplicate_report_type(*values: object) -> str | None:
    for value in values:
        text = _text(value)
        if text is None:
            continue
        normalized = text.strip().casefold().replace("-", "_").replace(" ", "_")
        if normalized in {"report", "reports", "report_status", "status", "routing", "unknown"}:
            continue
        if normalized in {"sales", "sales_income", "day_end_sales"}:
            return "sales_income"
        if normalized in {"attendance", "staff_attendance", "hr_attendance", "hr_staffing"}:
            return "staff_attendance"
        if normalized in {"bale_summary", "pricing_stock_release", "bale_release"}:
            return "bale_summary"
        if normalized in {"staff_performance", "hr_performance", "performance"}:
            return "staff_performance"
        if normalized in {"supervisor_control", "supervisor"}:
            return "supervisor_control"
        if normalized in {"store_monitoring", "monitoring"}:
            return "store_monitoring"
        return normalized or None
    return None


def _duplicate_report_label(report_type: str) -> str:
    label = _DUPLICATE_REPORT_LABELS.get(report_type)
    if label is not None:
        return label
    label = _REPORT_LABELS.get(report_type)
    if label is not None:
        return label
    return report_type.replace("_", " ").title() or "Report"


def _branch_from_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    match = _BRANCH_LINE_PATTERN.search(raw_text)
    if match is None:
        return None
    return _text(match.group(1))


def _report_date_from_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    match = _DATE_LINE_PATTERN.search(raw_text)
    if match is None:
        return None
    return _text(match.group(1))


def _normalized_branch_from_any(*values: object) -> str | None:
    for value in values:
        text = _text(value)
        if text is None:
            continue
        normalized = normalize_branch(text).normalized_value or text.strip()
        if normalized:
            return normalized
    return None


def _normalized_report_date_from_any(*values: object) -> str | None:
    for value in values:
        text = _text(value)
        if text is None:
            continue
        normalized = normalize_report_date(text).normalized_value or text.strip()
        if normalized:
            return normalized
    return None


def _upper_branch(branch: str | None) -> str:
    if branch is None:
        return "UNKNOWN BRANCH"
    display = CANONICAL_BRANCHES.get(branch, branch.replace("_", " ").title())
    return display.upper()


def _display_report_date(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _normalized_report_date_from_any(value)
    if normalized is None or len(normalized) < 10 or normalized[4] != "-" or normalized[7] != "-":
        return value
    return f"{normalized[8:10]}/{normalized[5:7]}/{normalized[2:4]}"


def _normalized_by_taop_lines(human_tolerance: Mapping[str, Any]) -> list[str]:
    corrections = _mapping_list(human_tolerance.get("corrections"))
    if not corrections:
        return []

    lines: list[str] = []
    seen_fields: set[str] = set()
    for correction in corrections:
        field = _text(correction.get("field"))
        if field is None or field in seen_fields:
            continue
        if field == "branch":
            branch_slug = _text(correction.get("normalized_value"))
            if branch_slug is None:
                continue
            display_name = CANONICAL_BRANCHES.get(branch_slug, branch_slug.replace("_", " ").title())
            lines.append(f"✔ Branch detected from header: {display_name}")
            seen_fields.add(field)
            continue
        if field == "date":
            raw_value = _text(correction.get("raw_value"))
            if raw_value is None:
                continue
            lines.append(f'✔ Date detected from "{raw_value}"')
            seen_fields.add(field)
            continue
        if field == "report_title":
            raw_value = _text(correction.get("raw_value"))
            if raw_value is None or raw_value == "attendance_structure_detected":
                continue
            lines.append(f"✔ Report title normalized: {raw_value} -> ATTENDANCE REPORT")
            seen_fields.add(field)
    return lines


def _qty_matches(left: float, right: float) -> bool:
    return abs(left - right) < 0.01


def _money_matches(left: float, right: float) -> bool:
    return abs(left - right) <= 0.01


def _format_qty_value(value: object) -> str:
    numeric = _float_or_none(value)
    if numeric is None:
        return "unknown"
    if abs(numeric - round(numeric)) < 0.01:
        return f"{int(round(numeric))} pcs"
    return f"{numeric:.2f} pcs"


def _format_money_value(value: object) -> str:
    numeric = _float_or_none(value)
    if numeric is None:
        return "unknown"
    return f"K{numeric:,.2f}"


def _format_plain_number(value: object) -> str:
    numeric = _float_or_none(value)
    if numeric is None:
        return "unknown"
    if abs(numeric - round(numeric)) < 0.01:
        return str(int(round(numeric)))
    return f"{numeric:.2f}"


def _parse_loose_money(raw_value: str) -> float | None:
    normalized = normalize_money(raw_value)
    if normalized.succeeded and normalized.normalized_value is not None:
        return _float_or_none(normalized.normalized_value)

    compact = re.sub(r",\s+", ",", raw_value)
    normalized = normalize_money(compact)
    if normalized.succeeded and normalized.normalized_value is not None:
        return _float_or_none(normalized.normalized_value)
    return None


def _parse_loose_number(raw_value: str) -> float | None:
    fragment_match = re.search(r"\d[\d,\s]*(?:\.\d+)?", raw_value)
    if fragment_match is None:
        return None
    fragment = fragment_match.group(0)
    compact = re.sub(r",\s+", ",", fragment)
    return _float_or_none(compact.replace(",", "").replace(" ", ""))


def _float_or_none(value: object) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _first_float(*values: object) -> float | None:
    for value in values:
        numeric = _float_or_none(value)
        if numeric is not None:
            return numeric
    return None


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _mapping_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            items.append(dict(item))
    return items


def _text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
