"""Deterministic TAOP feedback rendering for WhatsApp replies."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
import re
from typing import Any
import unicodedata

from packages.normalization.branches import CANONICAL_BRANCHES, normalize_branch
from packages.normalization.dates import normalize_report_date
from packages.report_policy import get_report_policy
from packages.validation import normalize_diagnostics

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
    "supervisor_control_summary": "Supervisor Control Summary",
    "store_monitoring": "Store Monitoring Report",
    "mixed": "Mixed Report",
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
_DETERMINISTIC_REVIEW_REASONS = frozenset(
    {
        "mixed_child_requires_review",
        "mixed_report_split_not_safe",
        "missing_branch",
        "missing_date",
        "supervisor_control_invalid_format",
    }
)


def build_report_feedback(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return structured TAOP feedback when the response can be upgraded safely."""

    response_type = _text(response_context.get("response_type"))
    if response_type is None:
        return None

    if response_type in {"duplicate_notice", "duplicate_ack"}:
        return build_duplicate_feedback(response_context)

    report_type = _canonical_report_type(_reported_type(response_context))
    if response_type in {"accepted_ack", "correction_accepted_ack"}:
        mixed_partial_feedback = _mixed_partial_success_feedback(response_context)
        if mixed_partial_feedback is not None:
            return {
                "response_text": mixed_partial_feedback,
            }
    if report_type == "bale_summary" and response_type in {"accepted_ack", "correction_accepted_ack"}:
        return build_bale_summary_feedback(response_context)
    if response_type in {"review_ack", "correction_review_ack"}:
        return _generic_review_feedback_payload(response_context)
    if response_type in {"rejected_fix_request", "correction_repeat_fix_request"}:
        return _generic_rejection_feedback_payload(response_context)
    return None


def build_review_feedback(response_context: Mapping[str, Any]) -> str:
    """Render deterministic TAOP review feedback when bale-specific details are unavailable."""

    feedback_context = _mapping(response_context.get("feedback_context"))
    diagnostics = _feedback_diagnostics(feedback_context)
    if diagnostics is not None:
        return _render_diagnostic_feedback(
            heading="⚠️ TAOP REVIEW REQUIRED",
            response_context=response_context,
            diagnostics=diagnostics,
        )
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
    review_reason = _resolved_review_reason(response_context)
    mixed_child_feedback = _mixed_child_review_feedback(
        response_context=response_context,
        feedback_context=feedback_context,
        response_reason=review_reason,
    )
    if mixed_child_feedback is not None:
        return mixed_child_feedback
    mixed_child_accountability = _mixed_child_accountability_feedback(
        response_context=response_context,
        feedback_context=feedback_context,
        response_reason=review_reason,
    )
    if mixed_child_accountability is not None:
        return mixed_child_accountability
    issues = _generic_review_issues(
        response_reason=review_reason,
        feedback_context=feedback_context,
        confidence=confidence,
        thresholds=thresholds,
    )
    validation = _generic_validation_results(
        report_label=report_label,
        branch=branch,
        report_date=report_date,
        response_reason=review_reason,
    )
    normalized_lines = _normalized_by_taop_lines(human_tolerance)
    action_line = _generic_review_action(
        response_reason=review_reason,
        report_label=report_label,
    )

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
            action_line,
        ]
    )
    if confidence is not None:
        lines.extend(["", "CONFIDENCE", f"Score: {confidence:.2f}"])
        auto_accept_min = _float_or_none(thresholds.get("auto_accept_min"))
        if auto_accept_min is not None:
            lines.append(f"Auto-accept threshold: {auto_accept_min:.2f}")
    return "\n".join(lines)


def _mixed_child_review_feedback(
    *,
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
    response_reason: str | None,
) -> str | None:
    """Render blocking mixed-child review details when they are available."""

    if response_reason != "mixed_child_requires_review":
        return None

    blocking_child = _blocking_transactional_mixed_child(feedback_context)
    if blocking_child is None:
        return None

    child_report_type = _mixed_child_report_type(blocking_child)
    report_label = _REPORT_LABELS.get(child_report_type or "", "Report")
    branch = _normalized_branch_from_any(
        blocking_child.get("branch"),
        _mapping(blocking_child.get("payload")).get("branch"),
        response_context.get("branch"),
        feedback_context.get("branch"),
        _branch_from_text(_raw_text(feedback_context)),
    )
    report_date = _normalized_report_date_from_any(
        blocking_child.get("report_date"),
        _mapping(blocking_child.get("payload")).get("report_date"),
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        _report_date_from_text(_raw_text(feedback_context)),
    )
    issue_lines = _mixed_child_issue_lines(
        blocking_child=blocking_child,
        report_type=child_report_type,
    )
    if not issue_lines:
        return None

    lines = [
        "⚠️ TAOP REVIEW REQUIRED",
        f"Report: {report_label}",
    ]
    if branch is not None:
        lines.append(f"Branch: {_upper_branch(branch)}")
    if report_date is not None:
        lines.append(f"Date: {_display_report_date(report_date)}")
    lines.extend(
        [
            "",
            "ISSUE",
            *issue_lines,
            "",
            "ACTION",
            _mixed_child_action_line(report_type=child_report_type, report_label=report_label),
        ]
    )
    return "\n".join(lines)


def _mixed_child_accountability_feedback(
    *,
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
    response_reason: str | None,
) -> str | None:
    """Render child-by-child accountability when a mixed review has explicit child detail."""

    if response_reason != "mixed_child_requires_review":
        return None

    child_entries = [
        entry
        for child in _mapping_list(feedback_context.get("mixed_children"))
        if (entry := _mixed_child_accountability_entry(child)) is not None
    ]
    if len(child_entries) < 2:
        return None

    reviewed_entries = [
        entry
        for entry in child_entries
        if entry["status"] not in {"accepted", "accepted_with_warning", "ready"}
    ]
    if not reviewed_entries:
        return None
    if any(
        entry["validation_error_code"] is None and entry["validation_error_message"] is None
        for entry in reviewed_entries
    ):
        return None

    branch = _normalized_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
        *(entry["branch"] for entry in child_entries),
        _branch_from_text(_raw_text(feedback_context)),
    )
    report_date = _normalized_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        *(entry["report_date"] for entry in child_entries),
        _report_date_from_text(_raw_text(feedback_context)),
    )

    lines = [
        "⚠️ TAOP REVIEW REQUIRED",
        "Report: Mixed Split Report",
    ]
    if branch is not None:
        lines.append(f"Branch: {_upper_branch(branch)}")
    if report_date is not None:
        lines.append(f"Date: {_display_report_date(report_date)}")

    lines.extend(["", "CHILD RESULTS"])
    for entry in child_entries:
        lines.append(f"child_{entry['child_index']}")
        lines.append(f"child_index: {entry['child_index']}")
        lines.append(f"report_type: {entry['report_type']}")
        lines.append(f"status: {entry['status']}")
        if entry["reason"] is not None:
            lines.append(f"reason: {entry['reason']}")
        if entry["validation_error_code"] is not None:
            lines.append(f"validation_error_code: {entry['validation_error_code']}")
        if entry["validation_error_message"] is not None:
            lines.append(f"validation_error_message: {entry['validation_error_message']}")
        lines.append("")

    if lines[-1] == "":
        lines.pop()

    lines.extend(
        [
            "",
            "ACTION",
            _mixed_child_accountability_action(reviewed_entries),
        ]
    )
    return "\n".join(lines)


def _mixed_partial_success_feedback(response_context: Mapping[str, Any]) -> str | None:
    """Render one accepted reply when a mixed split processed one or more child reports."""

    feedback_context = _mapping(response_context.get("feedback_context"))
    child_entries = [
        entry
        for child in _mapping_list(feedback_context.get("mixed_children"))
        if (entry := _mixed_child_accountability_entry(child)) is not None
    ]
    if len(child_entries) < 2:
        return None

    accepted_statuses = {"accepted", "accepted_with_warning", "ready"}
    processed_entries = [entry for entry in child_entries if entry["status"] in accepted_statuses]
    reviewed_entries = [entry for entry in child_entries if entry["status"] not in accepted_statuses]
    if not processed_entries:
        return None

    branch = _normalized_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
        *(entry["branch"] for entry in child_entries),
        _branch_from_text(_raw_text(feedback_context)),
    )
    report_date = _normalized_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        *(entry["report_date"] for entry in child_entries),
        _report_date_from_text(_raw_text(feedback_context)),
    )

    lines = ["✅ Mixed split reports received."]
    if branch is not None or report_date is not None:
        scope_parts: list[str] = []
        if branch is not None:
            scope_parts.append(_upper_branch(branch))
        if report_date is not None:
            scope_parts.append(_display_report_date(report_date) or report_date)
        if scope_parts:
            lines[0] = f"✅ Mixed split reports received for {', '.join(scope_parts)}."

    lines.append(f"Processed: {_mixed_entry_report_labels(processed_entries)}.")
    if not reviewed_entries:
        return "\n".join(lines)

    lines.append(f"Needs review: {_mixed_entry_report_labels(reviewed_entries)}.")
    partial_issue = _mixed_partial_success_issue(reviewed_entries)
    if partial_issue is not None:
        lines.append(f"Issue: {partial_issue}")
    lines.append(f"Action: {_mixed_partial_success_action(reviewed_entries)}")
    return "\n".join(lines)


def _mixed_child_accountability_entry(child: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return one normalized child-accountability entry when detail is available."""

    raw_child_index = child.get("child_index")
    if isinstance(raw_child_index, int) and raw_child_index > 0:
        child_index = raw_child_index
    else:
        lineage = _mapping(child.get("lineage"))
        lineage_child_index = child.get("lineage_child_index")
        if not isinstance(lineage_child_index, int):
            lineage_child_index = lineage.get("child_index")
        child_index = lineage_child_index + 1 if isinstance(lineage_child_index, int) and lineage_child_index >= 0 else None

    report_type = _text(child.get("response_report_type")) or _mixed_child_accountability_report_type(child)
    status = _text(child.get("response_status")) or _mixed_child_accountability_status(child)
    if child_index is None or report_type is None or status is None:
        return None

    reason = _text(child.get("reason"))
    validation_error_code = _text(child.get("validation_error_code"))
    validation_error_message = _text(child.get("validation_error_message"))
    if validation_error_code is None or validation_error_message is None:
        validation = _mapping(child.get("validation"))
        details = _mapping(validation.get("details"))
        validation_error_code = validation_error_code or _text(details.get("validation_error_code"))
        validation_error_message = validation_error_message or _text(details.get("validation_error_message"))
        rejections = validation.get("rejections")
        if isinstance(rejections, list):
            first_rejection = next((item for item in rejections if isinstance(item, Mapping)), None)
            if first_rejection is not None:
                validation_error_code = validation_error_code or _text(first_rejection.get("reason_code")) or _text(first_rejection.get("code"))
                validation_error_message = validation_error_message or _text(first_rejection.get("reason_detail")) or _text(first_rejection.get("message"))

    if reason is None and status in {"accepted", "accepted_with_warning"} and report_type == "day_end_sales":
        reason = "totals_reconciled"
    if status not in {"accepted", "accepted_with_warning", "ready"} and validation_error_message is not None:
        reason = None
    return {
        "child_index": child_index,
        "report_type": report_type,
        "status": status,
        "reason": reason,
        "validation_error_code": validation_error_code,
        "validation_error_message": validation_error_message,
        "branch": _text(child.get("branch")),
        "report_date": _text(child.get("report_date")),
    }


def _mixed_child_accountability_report_type(child: Mapping[str, Any]) -> str | None:
    """Return the response-facing report type for one mixed child."""

    report_type = _text(child.get("report_type")) or _text(child.get("report_family"))
    if report_type in {"sales_income", "sales", "day_end_sales"}:
        return "day_end_sales"
    if report_type == "supervisor_control":
        header_line = _text(child.get("header_line"))
        normalized_header = unicodedata.normalize("NFKC", header_line).casefold() if header_line is not None else None
        if normalized_header is not None and "summary" in normalized_header:
            return "supervisor_control_summary"
        return "supervisor_control"
    return report_type


def _mixed_child_accountability_status(child: Mapping[str, Any]) -> str | None:
    """Return the response-facing status for one mixed child."""

    status = _text(child.get("status"))
    if status == "needs_review":
        return "review"
    return status


def _mixed_child_accountability_action(reviewed_entries: list[dict[str, Any]]) -> str:
    """Return the specific resend action for the reviewed child set."""

    if len(reviewed_entries) == 1:
        entry = reviewed_entries[0]
        report_label = _REPORT_LABELS.get(entry["report_type"], "split report")
        return f"Resend only child_{entry['child_index']} as a complete {report_label}."
    return "Resend each reviewed split child as a complete single report."


def _mixed_partial_success_action(reviewed_entries: list[dict[str, Any]]) -> str:
    """Return the resend action for a partially processed mixed split."""

    if len(reviewed_entries) == 1:
        report_label = _REPORT_LABELS.get(reviewed_entries[0]["report_type"], "split report")
        return f"Resend only the {report_label} as one complete report."
    return "Resend each reviewed split report as a separate complete report."


def _mixed_partial_success_issue(reviewed_entries: list[dict[str, Any]]) -> str | None:
    """Return one concise surfaced issue for the reviewed split child set when available."""

    if len(reviewed_entries) != 1:
        return None
    reviewed_entry = reviewed_entries[0]
    return reviewed_entry["validation_error_message"] or reviewed_entry["reason"]


def _mixed_entry_report_labels(entries: list[dict[str, Any]]) -> str:
    """Return one human-readable report label list for mixed-child feedback."""

    labels: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        label = _REPORT_LABELS.get(entry["report_type"], "Report")
        if label not in seen:
            labels.append(label)
            seen.add(label)
    return ", ".join(labels) if labels else "Report"


def build_rejection_feedback(response_context: Mapping[str, Any]) -> str:
    """Render deterministic TAOP rejection feedback for unsupported or malformed inputs."""

    response_type = _text(response_context.get("response_type"))
    report_type = _reported_type(response_context)
    if report_type in {None, "unknown"}:
        return _unsupported_report_feedback()

    feedback_context = _mapping(response_context.get("feedback_context"))
    diagnostics = _feedback_diagnostics(feedback_context)
    if diagnostics is not None:
        return _render_diagnostic_feedback(
            heading="❌ TAOP REPORT REJECTED",
            response_context=response_context,
            diagnostics=diagnostics,
        )
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
    action_line = _generic_rejection_action(
        response_reason=_text(response_context.get("reason")),
        report_label=report_label,
        response_type=response_type,
    )

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


def _feedback_diagnostics(feedback_context: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return one normalized diagnostics payload when the response context carries it."""

    diagnostics = normalize_diagnostics(_mapping(feedback_context).get("diagnostics"))
    if diagnostics:
        return diagnostics
    return None


def _render_diagnostic_feedback(
    *,
    heading: str,
    response_context: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> str:
    """Render a concise diagnostics-led review or rejection message."""

    feedback_context = _mapping(response_context.get("feedback_context"))
    human_tolerance = _human_tolerance(feedback_context)
    response_report_type = _canonical_report_type(_reported_type(response_context))
    diagnostic_report_type = _duplicate_report_type(diagnostics.get("report_type"))
    if response_report_type in {None, "mixed", "routing", "report"} and diagnostic_report_type is not None:
        report_type = diagnostic_report_type
    else:
        report_type = response_report_type or diagnostic_report_type
    report_label = _REPORT_LABELS.get(report_type or "", "Report")
    branch = _normalized_branch_from_any(
        response_context.get("branch"),
        diagnostics.get("branch"),
    )
    report_date = _normalized_report_date_from_any(
        response_context.get("report_date"),
        diagnostics.get("date"),
    )
    lines = [
        heading,
        f"Report: {report_label}",
    ]
    if branch is not None:
        lines.append(f"Branch: {_upper_branch(branch)}")
    if report_date is not None:
        lines.append(f"Date: {_display_report_date(report_date)}")

    normalized_lines = _normalized_by_taop_lines(human_tolerance)
    if normalized_lines:
        lines.extend(["", "NORMALIZED BY TAOP", *normalized_lines])

    lines.extend(
        [
            "",
            "VALIDATION",
            *_render_diagnostic_checks(diagnostics, report_label=report_label),
            "",
            "FAILED CHECKS",
            *_render_failed_rules(diagnostics),
            "",
            "ACTION REQUIRED",
            _diagnostic_resend_action(
                diagnostics,
                report_label=report_label,
                response_type=_text(response_context.get("response_type")),
                response_reason=_text(response_context.get("reason")),
            ),
            "",
            "CONFIDENCE",
            _diagnostic_confidence_line(diagnostics),
        ]
    )
    return "\n".join(lines)


def _render_diagnostic_checks(
    diagnostics: Mapping[str, Any],
    *,
    report_label: str,
) -> list[str]:
    """Render validation status lines from one diagnostics payload."""

    rendered: list[str] = []
    failed_rule_fields = {
        _text(rule.get("field"))
        for rule in diagnostics.get("failed_rules", [])
        if isinstance(rule, Mapping)
    }
    for check in diagnostics.get("checks", []):
        if not isinstance(check, Mapping):
            continue
        name = _text(check.get("name"))
        passed = check.get("passed")
        if name is None or not isinstance(passed, bool):
            continue
        prefix = "✔" if passed else "❌"
        detail = _text(check.get("detail"))
        result = _text(check.get("result"))
        parser = _text(check.get("parser"))
        if name == "report_type_detected":
            rendered.append(
                f"{prefix} Report type {'detected' if passed else 'not detected'}: {detail or report_label}"
            )
        elif name == "branch_resolved":
            if not passed and "Branch" not in failed_rule_fields:
                rendered.append("ℹ Branch resolution not surfaced in reply metadata")
            else:
                rendered.append(
                    f"{prefix} Branch {'resolved' if passed else 'unresolved'}"
                    + (f": {_upper_branch(detail)}" if detail is not None else "")
                )
        elif name == "date_resolved":
            if not passed and "Date" not in failed_rule_fields:
                rendered.append("ℹ Date resolution not surfaced in reply metadata")
            else:
                rendered.append(
                    f"{prefix} Date {'resolved' if passed else 'unresolved'}"
                    + (f": {_display_report_date(detail)}" if detail is not None else "")
                )
        elif name == "mixed_content":
            if passed:
                suffix = {
                    "single": "single report detected",
                    "split": "split completed safely",
                }.get(result or "", detail or "single report detected")
                rendered.append(f"{prefix} Mixed content check: {suffix}")
            else:
                rendered.append(f"{prefix} Mixed content split failed: {detail or 'unsafe fan-out'}")
        elif name == "specialist_parser_stage":
            if result == "not_run":
                rendered.append(f"{prefix} Specialist parser not run: {detail or 'blocked upstream'}")
            elif passed:
                parser_label = parser or "specialist parser"
                rendered.append(f"{prefix} Specialist parser passed: {parser_label}")
            else:
                parser_label = parser or "specialist parser"
                rendered.append(f"{prefix} Specialist parser failed: {parser_label}")
        else:
            rendered.append(f"{prefix} {detail or name.replace('_', ' ')}")
    return rendered or ["ℹ Validation detail was not available."]


def _render_failed_rules(diagnostics: Mapping[str, Any]) -> list[str]:
    """Render numbered failed rules from one diagnostics payload."""

    rendered: list[str] = []
    rules = diagnostics.get("failed_rules")
    if not isinstance(rules, list) or not rules:
        return ["1. TAOP could not isolate an exact failed rule from the current payload."]

    unique_rules = _dedupe_rendered_failed_rules(rules)
    for index, rule in enumerate(unique_rules, start=1):
        if not isinstance(rule, Mapping):
            continue
        reason = _text(rule.get("reason")) or "Validation failed."
        rendered.append(f"{index}. {reason}")
        expected = _text(rule.get("expected"))
        received = _text(rule.get("received"))
        if expected is not None:
            rendered.append(f"- Expected: {expected}")
        if received is not None:
            rendered.append(f"- Received: {received}")
    return rendered or ["1. TAOP could not isolate an exact failed rule from the current payload."]


def _dedupe_rendered_failed_rules(rules: list[Any]) -> list[Mapping[str, Any]]:
    """Remove duplicate failed-rule blocks before rendering reply text."""

    unique: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
    for rule in rules:
        if not isinstance(rule, Mapping):
            continue
        key = (
            _text(rule.get("code")) or "",
            _text(rule.get("reason")) or "",
            _text(rule.get("expected")) or "",
            _text(rule.get("received")) or "",
        )
        if key not in unique:
            unique[key] = rule
    return list(unique.values())


def _diagnostic_resend_action(
    diagnostics: Mapping[str, Any],
    *,
    report_label: str,
    response_type: str | None = None,
    response_reason: str | None = None,
) -> str:
    """Return the operator resend instruction from diagnostics or a safe fallback."""

    if response_type == "correction_repeat_fix_request":
        return _generic_rejection_action(
            response_reason=response_reason,
            report_label=report_label,
            response_type=response_type,
        )
    resend_action = _text(diagnostics.get("resend_action"))
    if resend_action is not None:
        return resend_action
    return f"Correct only the failed checks above and resend the {report_label}."


def _diagnostic_confidence_line(diagnostics: Mapping[str, Any]) -> str:
    """Return the confidence line for the diagnostic feedback footer."""

    confidence = diagnostics.get("confidence")
    if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
        return f"Score: {float(confidence):.2f}"
    return "Score: not available"


def build_bale_summary_feedback(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return display-only TAOP feedback for bale-summary replies."""

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
    acceptance = _resolved_bale_acceptance(response_context, feedback_context)
    metrics = _resolved_bale_metrics(response_context, feedback_context)
    items = _resolved_bale_items(response_context, feedback_context)
    warnings = _resolved_bale_warnings(response_context, feedback_context)
    final_status = _resolved_feedback_status(response_context, feedback_context, acceptance)
    if final_status is None and not metrics and not items and not warnings:
        return None

    branch = _resolved_branch(response_context, feedback_context)
    report_date = _resolved_report_date(response_context, feedback_context)
    branch_display = _upper_branch(branch)
    date_display = _display_report_date(report_date)
    decision = _bale_feedback_decision(final_status)
    confidence = _first_float(
        acceptance.get("confidence"),
        response_context.get("confidence"),
        feedback_context.get("confidence"),
    )
    thresholds = _acceptance_thresholds(acceptance)

    all_issues = _bale_issues_from_context(
        warnings=warnings,
        response_reason=_resolved_bale_reason(response_context, feedback_context, acceptance),
        final_status=final_status,
    )
    blocking_issues, warning_issues = _split_display_issues(all_issues)
    issues_detected = warning_issues if _bale_display_status(final_status) == "accepted_with_warning" else all_issues
    action_required = _bale_action_required(final_status=final_status)
    normalized_summary = {
        "items": len(items),
        "total_qty": _float_or_none(metrics.get("total_qty")),
        "total_amount": _float_or_none(metrics.get("total_amount")),
    }

    response_text = _render_bale_summary_feedback_text(
        final_status=final_status,
        branch_display=branch_display,
        date_display=date_display,
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
        "status": final_status,
        "confidence": confidence,
        "auto_accept_threshold": thresholds.get("auto_accept_min"),
        "review_threshold": thresholds.get("review_min"),
        "validation_results": [],
        "issues_detected": issues_detected,
        "blocking_issues_detected": blocking_issues,
        "warning_issues_detected": warning_issues,
        "action_required": action_required,
        "normalized_summary": normalized_summary,
        "response_text": response_text,
    }


def build_duplicate_feedback(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return a more specific duplicate notice when scope can be inferred safely."""

    response_type = _text(response_context.get("response_type"))
    if response_type not in {"duplicate_notice", "duplicate_ack"}:
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
    if response_type == "duplicate_ack":
        lines = ["⚠️ DUPLICATE REPORT DETECTED"]
        if branch is not None:
            lines.append(f"Branch: {_upper_branch(branch)}")
        if report_date is not None:
            lines.append(f"Date: {_display_report_date(report_date)}")
        lines.append(f"Type: {report_label}")
        lines.append("This report was already received. No new record was created.")
        action_required = "No new record was created."
    else:
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
        action_required = "No new processing was applied."
    return {
        "report_type": report_type,
        "branch": branch,
        "report_date": report_date,
        "route": _feedback_route(response_context=response_context, feedback_context=feedback_context),
        "decision": "duplicate",
        "status": "duplicate",
        "reason_codes": _feedback_reason_codes(response_context=response_context, feedback_context=feedback_context),
        "confidence": None,
        "auto_accept_threshold": None,
        "review_threshold": None,
        "validation_results": [],
        "issues_detected": [],
        "action_required": action_required,
        "normalized_summary": None,
        "response_text": "\n".join(lines),
    }


def _generic_review_feedback_payload(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return structured review feedback for non-bale responses."""

    feedback_context = _mapping(response_context.get("feedback_context"))
    diagnostics = _feedback_diagnostics(feedback_context)
    report_type = _reported_type(response_context)
    if report_type in {None, "unknown"}:
        return None

    review_reason = _resolved_review_reason(response_context)
    response_context_with_reason = dict(response_context)
    if review_reason is not None:
        response_context_with_reason["reason"] = review_reason
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
    acceptance = _mapping(feedback_context.get("acceptance"))
    status = _resolved_feedback_status(response_context, feedback_context, acceptance) or "needs_review"
    action_required = _generic_review_action(
        response_reason=review_reason,
        report_label=report_label,
    )
    return {
        "report_type": report_type,
        "branch": branch,
        "report_date": report_date,
        "route": _feedback_route(response_context=response_context, feedback_context=feedback_context),
        "decision": "review",
        "status": status,
        "reason_codes": _feedback_reason_codes(
            response_context=response_context_with_reason,
            feedback_context=feedback_context,
        ),
        "confidence": _first_float(
            feedback_context.get("confidence"),
            acceptance.get("confidence"),
        ),
        "auto_accept_threshold": _float_or_none(_acceptance_thresholds(acceptance).get("auto_accept_min")),
        "review_threshold": _float_or_none(_acceptance_thresholds(acceptance).get("review_min")),
        "validation_results": _render_diagnostic_checks(diagnostics, report_label=report_label)
        if diagnostics is not None
        else _generic_validation_results(
            report_label=report_label,
            branch=branch,
            report_date=report_date,
            response_reason=review_reason,
        ),
        "issues_detected": _render_failed_rules(diagnostics)
        if diagnostics is not None
        else _generic_review_issues(
            response_reason=review_reason,
            feedback_context=feedback_context,
            confidence=_first_float(
                feedback_context.get("confidence"),
                acceptance.get("confidence"),
            ),
            thresholds=_acceptance_thresholds(acceptance),
        ),
        "action_required": _diagnostic_resend_action(
            diagnostics,
            report_label=report_label,
            response_type=_text(response_context_with_reason.get("response_type")),
            response_reason=review_reason,
        )
        if diagnostics is not None
        else action_required,
        **({"diagnostics": diagnostics} if diagnostics is not None else {}),
        "normalized_summary": None,
        "response_text": build_review_feedback(response_context_with_reason),
    }


def _generic_rejection_feedback_payload(response_context: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return structured rejection feedback for non-bale responses."""

    report_type = _reported_type(response_context)
    if report_type in {None, "unknown"}:
        return None

    feedback_context = _mapping(response_context.get("feedback_context"))
    diagnostics = _feedback_diagnostics(feedback_context)
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
    acceptance = _mapping(feedback_context.get("acceptance"))
    status = _resolved_feedback_status(response_context, feedback_context, acceptance) or "rejected"
    surfaced_reason = _generic_rejection_reason(_text(response_context.get("reason")))
    action_required = _generic_rejection_action(
        response_reason=_text(response_context.get("reason")),
        report_label=report_label,
        response_type=_text(response_context.get("response_type")),
    )
    return {
        "report_type": report_type,
        "branch": branch,
        "report_date": report_date,
        "route": _feedback_route(response_context=response_context, feedback_context=feedback_context),
        "decision": "rejected",
        "status": status,
        "reason_codes": _feedback_reason_codes(response_context=response_context, feedback_context=feedback_context),
        "confidence": _first_float(
            feedback_context.get("confidence"),
            acceptance.get("confidence"),
        ),
        "auto_accept_threshold": None,
        "review_threshold": None,
        "validation_results": _render_diagnostic_checks(diagnostics, report_label=report_label)
        if diagnostics is not None
        else _generic_validation_results(
            report_label=report_label,
            branch=branch,
            report_date=report_date,
            response_reason=_normalized_review_reason(_text(response_context.get("reason"))),
        ),
        "issues_detected": _render_failed_rules(diagnostics)
        if diagnostics is not None
        else [
            "1. " + surfaced_reason.rstrip(".") + ".",
            "2. TAOP could not approve this report with the current format.",
        ],
        "action_required": _diagnostic_resend_action(
            diagnostics,
            report_label=report_label,
            response_type=_text(response_context.get("response_type")),
            response_reason=_text(response_context.get("reason")),
        )
        if diagnostics is not None
        else action_required,
        **({"diagnostics": diagnostics} if diagnostics is not None else {}),
        "normalized_summary": None,
        "response_text": build_rejection_feedback(response_context),
    }


def _feedback_route(
    *,
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
) -> str | None:
    """Return the surfaced routing family when available."""

    return (
        _text(feedback_context.get("route"))
        or _text(response_context.get("report_type"))
        or _text(feedback_context.get("report_type"))
    )


def _feedback_reason_codes(
    *,
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
) -> list[str]:
    """Return surfaced outbound reason codes in stable order."""

    candidates: list[str] = []
    explicit_reason = _surface_reason_code(_text(response_context.get("reason")))
    if explicit_reason is not None:
        candidates.append(explicit_reason)

    validation_codes = sorted(
        {
            surfaced
            for surfaced in (
                _surface_reason_code(code)
                for code in _validation_reason_codes(
                    _mapping(feedback_context.get("validation")),
                    _mapping(feedback_context.get("candidate_validation")),
                )
            )
            if surfaced is not None
        }
    )
    candidates.extend(validation_codes)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        deduped.append(candidate)
        seen.add(candidate)
    return deduped


def _surface_reason_code(reason: str | None) -> str | None:
    """Map internal governance reasons onto outbound reason codes."""

    normalized = _normalized_review_reason(reason)
    if normalized is None:
        return None
    if normalized == "mixed_report_split_not_safe":
        return "mixed_report_signals"
    if normalized in {"validation_failed", "fallback_validation_failed", "invalid_input"}:
        return "validation_failed"
    return normalized


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
    response_reason: str | None,
) -> list[str]:
    results = [f"✔ Report type detected: {report_label}"]
    if branch is not None:
        results.append("✔ Branch resolved")
    elif response_reason == "missing_branch":
        results.append("❌ Branch is missing")
    else:
        results.append("ℹ Branch resolution not surfaced in reply metadata")
    if report_date is not None:
        results.append("✔ Date resolved")
    elif response_reason == "missing_date":
        results.append("❌ Date is missing")
    else:
        results.append("ℹ Date resolution not surfaced in reply metadata")
    return results


def _blocking_transactional_mixed_child(feedback_context: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return the first blocking mixed child that failed acceptance."""

    success_statuses = {"accepted", "accepted_with_warning", "accepted_split", "ready"}
    for child in _mapping_list(feedback_context.get("mixed_children")):
        if not _mixed_child_blocks_transactional(child):
            continue
        status = _text(child.get("status"))
        if status not in success_statuses:
            return child
    return None


def _mixed_child_blocks_transactional(child: Mapping[str, Any]) -> bool:
    """Return whether one mixed child should block parent acceptance."""

    blocks = child.get("blocks_transactional_processing")
    if isinstance(blocks, bool):
        return blocks
    report_family = _text(child.get("report_family_label")) or _text(child.get("report_family")) or _text(child.get("report_type"))
    return report_family not in {"intelligence", "supervisor_control"}


def _mixed_child_report_type(child: Mapping[str, Any]) -> str | None:
    """Return the concrete report type carried by one mixed child summary."""

    payload = _mapping(child.get("payload"))
    return _duplicate_report_type(
        child.get("report_type"),
        child.get("report_family"),
        payload.get("report_type"),
        payload.get("signal_type"),
    )


def _mixed_child_issue_lines(
    *,
    blocking_child: Mapping[str, Any],
    report_type: str | None,
) -> list[str]:
    """Return user-facing mixed child issue lines when details are available."""

    normalized_report_type = _canonical_report_type(report_type) or report_type
    if normalized_report_type == "sales_income":
        sales_issue_lines = _mixed_sales_issue_lines(blocking_child)
        if sales_issue_lines:
            return sales_issue_lines
    return _mixed_child_generic_issue_lines(blocking_child)


def _mixed_sales_issue_lines(blocking_child: Mapping[str, Any]) -> list[str]:
    """Return concrete totals guidance for one blocking sales child."""

    validation = _mixed_child_validation(blocking_child)
    payload_validation = _mapping(_mapping(blocking_child.get("payload")).get("validation"))
    structured_rejection = _structured_sales_totals_mismatch_rejection(
        validation,
        payload_validation,
    )
    if structured_rejection is not None:
        return _structured_sales_issue_lines(structured_rejection)

    warning_codes = {
        code
        for code in (
            _text(warning.get("code"))
            for warning in _mixed_child_warnings(blocking_child)
        )
        if code is not None
    }
    validation_codes = _validation_reason_codes(validation, payload_validation)
    if not ({"invalid_totals", "sales_totals_mismatch", "till_mismatch"} & (warning_codes | validation_codes)):
        return []

    metrics = _mixed_child_metrics(blocking_child)
    cash_sales = _float_or_none(metrics.get("cash_sales"))
    eftpos_sales = _float_or_none(metrics.get("eftpos_sales"))
    mobile_money_sales = _float_or_none(metrics.get("mobile_money_sales"))
    till_total = _float_or_none(metrics.get("till_total"))
    deposit_total = _float_or_none(metrics.get("deposit_total"))
    gross_sales = _float_or_none(metrics.get("gross_sales"))

    lines = ["Sales totals do not match till/payment totals."]

    calculated_till_cash = None
    if till_total is not None:
        calculated_till_cash = round(till_total + (deposit_total or 0.0), 2)
    if cash_sales is not None and calculated_till_cash is not None:
        lines.append(f"- Declared Total Cash: {_format_money_value(cash_sales)}")
        lines.append(f"- Calculated Till Cash: {_format_money_value(calculated_till_cash)}")

    payment_parts = [value for value in (cash_sales, eftpos_sales, mobile_money_sales) if value is not None]
    expected_total_sales = round(sum(payment_parts), 2) if len(payment_parts) >= 2 else None
    if gross_sales is not None and expected_total_sales is not None:
        lines.append(f"- Declared Total Sales: {_format_money_value(gross_sales)}")
        lines.append(f"- Expected Total Sales: {_format_money_value(expected_total_sales)}")

    if len(lines) == 1:
        generic_lines = _mixed_child_generic_issue_lines(blocking_child)
        if generic_lines:
            lines.extend(generic_lines)
    return lines


def _structured_sales_totals_mismatch_rejection(*validation_blocks: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the canonical sales totals mismatch rejection when present."""

    structured_fields = {
        "declared_total_cash",
        "expected_total_cash",
        "declared_total_card",
        "expected_total_card",
        "declared_total_sales",
        "expected_total_sales",
    }
    for validation in validation_blocks:
        rejections = validation.get("rejections")
        if not isinstance(rejections, list):
            continue
        for rejection in rejections:
            if not isinstance(rejection, Mapping):
                continue
            code = _text(rejection.get("reason_code")) or _text(rejection.get("code"))
            if code not in {"sales_totals_mismatch", "invalid_totals"}:
                continue
            if structured_fields & set(rejection.keys()):
                return dict(rejection)
    return None


def _structured_sales_issue_lines(rejection: Mapping[str, Any]) -> list[str]:
    """Return sales issue lines from structured validation detail."""

    message = (
        _text(rejection.get("reason_detail"))
        or _text(rejection.get("message"))
        or "Sales totals do not match till/payment totals."
    )
    lines = [message]
    for label, field_name in (
        ("Declared Total Cash", "declared_total_cash"),
        ("Expected Total Cash", "expected_total_cash"),
        ("Declared Total Card", "declared_total_card"),
        ("Expected Total Card", "expected_total_card"),
        ("Declared Total Sales", "declared_total_sales"),
        ("Expected Total Sales", "expected_total_sales"),
    ):
        lines.append(f"- {label}: {_format_money_value(rejection.get(field_name))}")
    return lines


def _mixed_child_generic_issue_lines(blocking_child: Mapping[str, Any]) -> list[str]:
    """Return generic but child-specific issue lines from warnings or validation."""

    issue_lines = _generic_issue_lines(
        feedback_context={
            "warnings": _mixed_child_warnings(blocking_child),
            "validation": _mixed_child_validation(blocking_child),
        }
    )
    if not issue_lines:
        return []
    summary, *details = issue_lines
    lines = [summary if summary.endswith(".") else f"{summary}."]
    lines.extend(f"- {detail}" for detail in details)
    return lines


def _mixed_child_action_line(*, report_type: str | None, report_label: str) -> str:
    """Return the most specific resend guidance for a blocking mixed child."""

    normalized_report_type = _canonical_report_type(report_type) or report_type
    if normalized_report_type == "sales_income":
        return f"Correct the TOTALS section and resend the {report_label}."
    if report_label == "Report":
        return "Correct the blocking report and resend it as one report per message."
    return f"Correct the issues in the {report_label} and resend it as one report per message."


def _mixed_child_warnings(blocking_child: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return normalized warning payloads for one mixed child summary."""

    explicit_warnings = blocking_child.get("warnings")
    if isinstance(explicit_warnings, list):
        return _mapping_list(explicit_warnings)
    return _mapping_list(_mapping(blocking_child.get("payload")).get("warnings"))


def _mixed_child_metrics(blocking_child: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return normalized metrics for one mixed child summary."""

    metrics = blocking_child.get("metrics")
    if isinstance(metrics, Mapping):
        return metrics
    return _mapping(_mapping(blocking_child.get("payload")).get("metrics"))


def _mixed_child_validation(blocking_child: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return normalized validation details for one mixed child summary."""

    validation = blocking_child.get("validation")
    if isinstance(validation, Mapping):
        return validation
    return _mapping(_mapping(blocking_child.get("payload")).get("validation"))


def _generic_review_issues(
    *,
    response_reason: str | None,
    feedback_context: Mapping[str, Any],
    confidence: float | None,
    thresholds: Mapping[str, Any],
) -> list[str]:
    known_reason_issues = _known_review_issue_lines(response_reason)
    if known_reason_issues:
        return [f"{index}. {issue}" for index, issue in enumerate(known_reason_issues, start=1)]

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
        "correction_request_requires_full_report": "Correction request detected, but full replacement report rows are required.",
        "validation_failed": "Report format failed SOP validation. Recheck required fields and totals.",
        "confidence_below_reject_threshold": "Confidence was below the configured acceptance threshold.",
        "fallback_validation_failed": "Report format failed SOP validation. Recheck required fields and totals.",
        "invalid_input": "Report format failed SOP validation. Recheck required fields and totals.",
        "unknown_report_type": "Format does not match any supported report type.",
    }
    if reason is None:
        return "Format does not match the expected SOP structure."
    return mapping.get(reason, reason.replace("_", " ").capitalize() + ".")


def _resolved_review_reason(response_context: Mapping[str, Any]) -> str | None:
    feedback_context = _mapping(response_context.get("feedback_context"))
    report_type = _reported_type(response_context)
    derived_reason = _derived_review_reason(feedback_context=feedback_context, report_type=report_type)
    if derived_reason in _DETERMINISTIC_REVIEW_REASONS:
        return derived_reason
    explicit_reason = _normalized_review_reason(_text(response_context.get("reason")))
    if explicit_reason is not None:
        return explicit_reason
    return derived_reason


def _derived_review_reason(
    *,
    feedback_context: Mapping[str, Any],
    report_type: str | None,
) -> str | None:
    codes = _validation_reason_codes(
        _mapping(feedback_context.get("validation")),
        _mapping(feedback_context.get("candidate_validation")),
    )
    if "missing_branch" in codes:
        return "missing_branch"
    if "missing_report_date" in codes or "missing_date" in codes:
        return "missing_date"

    normalized_report_type = _canonical_report_type(report_type) or report_type
    if normalized_report_type == "supervisor_control":
        warning_codes = {
            code
            for code in (
                _text(warning.get("code"))
                for warning in _mapping_list(feedback_context.get("warnings"))
            )
            if code is not None
        }
        if {"missing_fields", "parser_failure"} & (codes | warning_codes):
            return "supervisor_control_invalid_format"
    return None


def _validation_reason_codes(*validation_blocks: Mapping[str, Any]) -> set[str]:
    codes: set[str] = set()
    for validation in validation_blocks:
        reason_codes = validation.get("reason_codes")
        if isinstance(reason_codes, list):
            for item in reason_codes:
                normalized = _normalized_review_reason(_text(item))
                if normalized is not None:
                    codes.add(normalized)
        rejections = validation.get("rejections")
        if isinstance(rejections, list):
            for rejection in rejections:
                if not isinstance(rejection, Mapping):
                    continue
                normalized = _normalized_review_reason(
                    _text(rejection.get("reason_code")) or _text(rejection.get("code"))
                )
                if normalized is not None:
                    codes.add(normalized)
    return codes


def _normalized_review_reason(reason: str | None) -> str | None:
    if reason is None:
        return None
    if reason == "missing_report_date":
        return "missing_date"
    return reason


def _known_review_issue_lines(response_reason: str | None) -> list[str]:
    mapping = {
        "mixed_child_requires_review": [
            "TAOP split the message into multiple reports.",
            "One split report still needs review before final processing.",
        ],
        "mixed_report_split_not_safe": [
            "Mixed report types detected.",
            "Send each report separately or use approved split format.",
        ],
        "missing_branch": [
            "Branch missing or unclear.",
            "Add Branch: <branch>.",
        ],
        "missing_date": [
            "Date missing or unclear.",
            "Add Date: DD/MM/YY.",
        ],
        "supervisor_control_invalid_format": [
            "Supervisor Control Report format failed SOP validation.",
            "Recheck required fields and totals.",
        ],
    }
    issues = mapping.get(response_reason)
    return list(issues) if issues is not None else []


def _generic_review_action(*, response_reason: str | None, report_label: str) -> str:
    mapping = {
        "mixed_child_requires_review": "Please resend the report that still needs review as one report per message.",
        "mixed_report_split_not_safe": "Mixed report types detected. Send each report separately or use approved split format.",
        "missing_branch": "Branch missing or unclear. Add Branch: <branch>.",
        "missing_date": "Date missing or unclear. Add Date: DD/MM/YY.",
        "supervisor_control_invalid_format": "Report format failed SOP validation. Recheck required fields and totals.",
    }
    return mapping.get(response_reason, "Please correct the issues above and resend.")


def _generic_rejection_action(
    *,
    response_reason: str | None,
    report_label: str,
    response_type: str | None,
) -> str:
    if response_type == "correction_repeat_fix_request":
        return f"Resend using the exact SOP format for {report_label} with the missing corrections applied."
    if response_reason == "correction_request_requires_full_report":
        return "Resend the full replacement report with all bale item rows and totals."
    if response_reason in {"validation_failed", "fallback_validation_failed", "invalid_input"}:
        return f"Recheck required fields and totals, then resend using the exact SOP format for {report_label}."
    return f"Resend using the exact SOP format for {report_label}."


def _render_bale_summary_feedback_text(
    *,
    final_status: str | None,
    branch_display: str,
    date_display: str | None,
    issues_detected: list[dict[str, Any]],
    action_required: str,
    normalized_summary: dict[str, Any],
    confidence: float | None,
    thresholds: Mapping[str, Any],
) -> str:
    display_status = _bale_display_status(final_status)
    header = _bale_header(branch_display=branch_display, final_status=final_status)
    lines = [header]
    if date_display is not None:
        lines.append(f"Date: {date_display}")
    status_line = _bale_status_line(display_status)
    if status_line is not None:
        lines.extend(["", status_line])

    lines.extend(
        [
            "",
            "SUMMARY",
            f"Items: {int(normalized_summary.get('items') or 0)}",
            f"Total Qty: {_format_qty_value(normalized_summary.get('total_qty'))}",
            f"Total Amount: {_format_money_value(normalized_summary.get('total_amount'))}",
        ]
    )

    if display_status == "accepted_with_warning" and issues_detected:
        lines.extend(["", "WARNINGS"])
        lines.extend(_render_issues(issues_detected))
    elif display_status in {"needs_review", "rejected"}:
        lines.extend(["", "ISSUES"])
        if issues_detected:
            lines.extend(_render_issues(issues_detected))
        else:
            lines.extend(
                [
                    "1. Governance review required:",
                    "- The bale summary was held for review without specialist-agent issue detail.",
                ]
            )

    if display_status in {"accepted", "accepted_with_warning"}:
        lines.extend(["", action_required])
    else:
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

def _bale_issues_from_context(
    *,
    warnings: list[dict[str, Any]],
    response_reason: str | None,
    final_status: str | None,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for warning in warnings:
        code = _text(warning.get("code")) or "warning"
        message = _text(warning.get("message")) or "Specialist agent surfaced a warning."
        issues.append(
            {
                "code": code,
                "title": _warning_title(code),
                "details": [message],
                "severity": _text(warning.get("severity")) or "warning",
            }
        )

    if _bale_display_status(final_status) in {"needs_review", "rejected"}:
        known_codes = {str(issue.get("code") or "") for issue in issues}
        if response_reason is not None and response_reason not in known_codes:
            issues.append(
                {
                    "code": response_reason,
                    "title": "Governance reason",
                    "details": [_safe_reason_text(response_reason)],
                    "severity": "error",
                }
            )
    return _dedupe_issues(issues)


def _split_display_issues(issues: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Separate sourced TAOP display issues into blocking errors and warnings."""

    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    for issue in issues:
        if _text(issue.get("severity")) == "error":
            blocking_issues.append(issue)
        else:
            warning_issues.append(issue)
    return blocking_issues, warning_issues


def _bale_action_required(*, final_status: str | None) -> str:
    display_status = _bale_display_status(final_status)
    if display_status == "accepted":
        return "No correction required."
    if display_status == "accepted_with_warning":
        return "No resend required."
    if display_status == "needs_review":
        return "Follow the specialist-agent warnings and governance review outcome before resending."
    return "Correct the blocking specialist-agent issues and resend."


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
            lines.append("- Review the specialist-agent output for this bale summary.")
    return lines


def _bale_header(*, branch_display: str, final_status: str | None) -> str:
    display_status = _bale_display_status(final_status)
    if display_status == "accepted_with_warning":
        return f"✅ TAOP BALE SUMMARY ACCEPTED WITH WARNING – {branch_display}"
    if display_status == "accepted":
        return f"✅ TAOP BALE SUMMARY ACCEPTED – {branch_display}"
    if display_status == "needs_review":
        return f"📊 TAOP BALE SUMMARY REVIEW REQUIRED – {branch_display}"
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


def _resolved_bale_acceptance(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
) -> Mapping[str, Any]:
    acceptance = _mapping(response_context.get("acceptance"))
    if acceptance:
        return acceptance
    return _mapping(feedback_context.get("acceptance"))


def _resolved_bale_metrics(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
) -> Mapping[str, Any]:
    metrics = _mapping(response_context.get("metrics"))
    if metrics:
        return metrics
    return _mapping(feedback_context.get("metrics"))


def _resolved_bale_items(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    items = _mapping_list(response_context.get("items"))
    if items:
        return items
    return _mapping_list(feedback_context.get("items"))


def _resolved_bale_warnings(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    warnings = _mapping_list(response_context.get("warnings"))
    if warnings:
        return warnings
    return _mapping_list(feedback_context.get("warnings"))


def _resolved_feedback_status(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
    acceptance: Mapping[str, Any],
) -> str | None:
    governance = _mapping(response_context.get("governance")) or _mapping(feedback_context.get("governance"))
    return (
        _text(response_context.get("governance_status"))
        or _text(governance.get("status"))
        or _text(feedback_context.get("governance_status"))
        or _text(feedback_context.get("status"))
        or _status_from_acceptance(acceptance)
    )


def _status_from_acceptance(acceptance: Mapping[str, Any]) -> str | None:
    decision = _text(acceptance.get("decision")) or _text(acceptance.get("status"))
    if decision == "accept":
        return "accepted"
    if decision == "review":
        return "needs_review"
    if decision == "reject":
        return "rejected"
    return None


def _bale_feedback_decision(final_status: str | None) -> str:
    display_status = _bale_display_status(final_status)
    if display_status in {"accepted", "accepted_with_warning"}:
        return "accepted"
    if display_status == "needs_review":
        return "review"
    return "rejected"


def _bale_display_status(final_status: str | None) -> str | None:
    if final_status == "accepted_with_warning":
        return "accepted_with_warning"
    if final_status in {"accepted", "ready"}:
        return "accepted"
    if final_status in {"needs_review", "review", "conflict_blocked"}:
        return "needs_review"
    if final_status in {"rejected", "invalid_input"}:
        return "rejected"
    return None


def _bale_status_line(display_status: str | None) -> str | None:
    mapping = {
        "accepted": "STATUS: ✅ ACCEPTED",
        "accepted_with_warning": "STATUS: ⚠️ ACCEPTED WITH WARNING",
        "needs_review": "STATUS: ⚠️ REVIEW REQUIRED",
        "rejected": "STATUS: ❌ REJECTED",
    }
    return mapping.get(display_status)


def _acceptance_thresholds(acceptance: Mapping[str, Any]) -> dict[str, float | None]:
    thresholds = _mapping(acceptance.get("thresholds"))
    return {
        "auto_accept_min": _float_or_none(thresholds.get("auto_accept_min")),
        "review_min": _float_or_none(thresholds.get("review_min")),
        "reject_max": _float_or_none(thresholds.get("reject_max")),
    }


def _resolved_bale_reason(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
    acceptance: Mapping[str, Any],
) -> str | None:
    return (
        _text(response_context.get("reason"))
        or _text(_mapping(response_context.get("governance")).get("reason"))
        or _text(_mapping(feedback_context.get("governance")).get("reason"))
        or _text(acceptance.get("reason"))
    )


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
) -> str | None:
    return _normalized_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
    )


def _resolved_report_date(
    response_context: Mapping[str, Any],
    feedback_context: Mapping[str, Any],
) -> str | None:
    return _normalized_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
    )


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


def _warning_title(code: str) -> str:
    mapping = {
        "approval_backlog": "Approval backlog detected",
        "data_mismatch": "Totals or counts need review",
        "financial_anomaly": "Amount or quantity anomaly detected",
        "format_cleanup": "Format cleanup applied",
        "format_warning": "Format warning detected",
        "low_release_ratio": "Release ratio needs review",
        "missing_fields": "Required fields missing or incomplete",
        "missing_provenance": "Prepared By details are incomplete",
        "parser_failure": "Parser failure detected",
        "totals_inferred": "Totals inferred from bale rows",
    }
    return mapping.get(code, code.replace("_", " ").capitalize() if code else "Issue detected")


def _safe_reason_text(reason: str) -> str:
    if reason == "confidence_between_review_and_accept_thresholds":
        return "Confidence is below the auto-accept threshold and needs review."
    if reason == "validation_failed":
        return "The bale summary did not satisfy the required validation checks."
    if reason == "confidence_below_reject_threshold":
        return "Confidence was below the rejection threshold."
    if reason == "confidence_missing":
        return "Confidence was missing, so governance held the bale summary for review."
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
    if "supervisor control report" in normalized or "supervisor control summary" in normalized:
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


def _normalize_text(value: str | None) -> str | None:
    """Return a loose normalized text key for report-label comparisons."""

    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value).casefold()
    collapsed = " ".join(re.sub(r"[^a-z0-9]+", " ", normalized).split())
    return collapsed or None
