"""Shared diagnostics payloads for review and rejection feedback."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field as dataclass_field
from pathlib import Path
import re
from typing import Any

_REVIEW_RESPONSE_TYPES = {
    "review_ack",
    "correction_review_ack",
    "rejected_fix_request",
    "correction_repeat_fix_request",
}
_REPORT_LABELS = {
    "sales": "Day-End Sales Report",
    "sales_income": "Day-End Sales Report",
    "day_end_sales": "Day-End Sales Report",
    "staff_attendance": "Staff Attendance Report",
    "attendance": "Staff Attendance Report",
    "hr_attendance": "Staff Attendance Report",
    "hr_staffing": "Staff Attendance Report",
    "staff_performance": "Staff Performance Report",
    "hr_performance": "Staff Performance Report",
    "bale_summary": "Daily Bale Summary",
    "pricing_stock_release": "Daily Bale Summary",
    "bale_release": "Daily Bale Summary",
    "supervisor_control": "Supervisor Control Report",
    "supervisor_control_summary": "Supervisor Control Summary",
    "mixed": "Mixed Report",
}
_EXPECTED_TITLES = {
    "sales_income": "DAY-END SALES REPORT",
    "staff_attendance": "ATTENDANCE REPORT",
    "staff_performance": "STAFF PERFORMANCE REPORT",
    "bale_summary": "DAILY BALE SUMMARY - RELEASED TO RAIL",
    "supervisor_control": "SUPERVISOR CONTROL REPORT",
}
_SPECIALIST_PARSERS = {
    "sales_income": "sales_income_agent",
    "staff_attendance": "hr_agent",
    "staff_performance": "hr_agent",
    "bale_summary": "pricing_stock_release_agent",
    "supervisor_control": "supervisor_control_agent",
}
_SECTION_BOUNDARY_PATTERN = re.compile(
    r"^(?:totals?|customer count|additional information|branch|date|prepared by|checked by|thanks)\b",
    flags=re.IGNORECASE,
)
_TILL_HEADER_ANY_PATTERN = re.compile(r"^\s*till\b.*$", flags=re.IGNORECASE)
_TILL_HEADER_SUPPORTED_PATTERN = re.compile(r"^\s*till\s*#\s*\d+\s*(?::.*)?$", flags=re.IGNORECASE)
_CASHIER_PATTERN = re.compile(r"^\s*(cashier|served by)\b", flags=re.IGNORECASE)
_ASSISTANT_PATTERN = re.compile(r"^\s*(assistant|assistant cashier|monitor)\b", flags=re.IGNORECASE)
_TCASH_PATTERN = re.compile(r"^\s*t\s*/?\s*cash\b", flags=re.IGNORECASE)
_TCARD_PATTERN = re.compile(r"^\s*t\s*/?\s*card\b", flags=re.IGNORECASE)
_ZREADING_PATTERN = re.compile(r"^\s*z\s*/?\s*reading\b", flags=re.IGNORECASE)
_ADDITIONAL_INFO_PATTERN = re.compile(r"^\s*(additional information|additional info|notes)\b", flags=re.IGNORECASE)
_BALE_ITEM_HEADER_PATTERN = re.compile(r"^\s*#?\s*(\d+)\s*(?:\.\s*|\s+)(.+?)\s*$")
_BALE_QTY_PATTERN = re.compile(r"^\s*(?:qty|quantity)\b", flags=re.IGNORECASE)
_BALE_AMOUNT_PATTERN = re.compile(r"^\s*(?:amt|amount|value)\b", flags=re.IGNORECASE)
_TITLE_LINE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("sales_income", re.compile(r"^\s*sales\s+report\.?\s*$", flags=re.IGNORECASE)),
    ("staff_attendance", re.compile(r"^\s*staffs?\s+attendance(?:\s+report)?\.?\s*$", flags=re.IGNORECASE)),
    ("bale_summary", re.compile(r"^\s*daily\s+bale\s+summary\b", flags=re.IGNORECASE)),
    ("supervisor_control", re.compile(r"^\s*supervisor\s+control(?:\s+summary|\s+report)?\.?\s*$", flags=re.IGNORECASE)),
)
_MIXED_TITLE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("DAY-END SALES REPORT", re.compile(r"^\s*day[\s-]*end\s+sales\s+report\b", flags=re.IGNORECASE)),
    ("SUPERVISOR CONTROL REPORT", re.compile(r"^\s*supervisor\s+control\s+report\b", flags=re.IGNORECASE)),
    ("SUPERVISOR CONTROL SUMMARY", re.compile(r"^\s*supervisor\s+control\s+summary\b", flags=re.IGNORECASE)),
    ("ATTENDANCE REPORT", re.compile(r"^\s*(?:staffs?\s+)?attendance\s+report\b", flags=re.IGNORECASE)),
    ("STAFF PERFORMANCE REPORT", re.compile(r"^\s*staff\s+performance\s+report\b", flags=re.IGNORECASE)),
    ("DAILY BALE SUMMARY - RELEASED TO RAIL", re.compile(r"^\s*daily\s+bale\s+summary\b", flags=re.IGNORECASE)),
)
_CONFIDENCE_REASONS = {
    "confidence_between_review_and_accept_thresholds",
    "strict_candidate_below_reject_threshold",
    "confidence_below_reject_threshold",
    "confidence_missing",
}


@dataclass(slots=True, frozen=True)
class DiagnosticCheck:
    """One validation status line for operator-facing diagnostics."""

    name: str
    passed: bool
    result: str | None = None
    detail: str | None = None
    expected: str | None = None
    received: str | None = None
    stage: str | None = None
    parser: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "passed": self.passed,
        }
        for field_name in ("result", "detail", "expected", "received", "stage", "parser"):
            value = getattr(self, field_name)
            if value is not None:
                payload[field_name] = value
        return payload


@dataclass(slots=True, frozen=True)
class FailedRule:
    """One specific failed rule to surface back to the operator."""

    reason: str
    field: str | None = None
    expected: str | None = None
    received: str | None = None
    action: str | None = None
    stage: str | None = None
    code: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"reason": self.reason}
        for field_name in ("field", "expected", "received", "action", "stage", "code"):
            value = getattr(self, field_name)
            if value is not None:
                payload[field_name] = value
        return payload


@dataclass(slots=True, frozen=True)
class RejectionDiagnostics:
    """Normalized diagnostics attached to review and rejection feedback."""

    stage: str
    report_type: str | None
    branch: str | None
    date: str | None
    status: str
    checks: list[dict[str, Any]]
    failed_rules: list[dict[str, Any]]
    confidence: float | None = None
    specialist_parser: str | None = None
    resend_action: str | None = None
    details: dict[str, Any] = dataclass_field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "stage": self.stage,
            "report_type": self.report_type,
            "branch": self.branch,
            "date": self.date,
            "status": self.status,
            "checks": list(self.checks),
            "failed_rules": list(self.failed_rules),
            "confidence": self.confidence,
        }
        if self.specialist_parser is not None:
            payload["specialist_parser"] = self.specialist_parser
        if self.resend_action is not None:
            payload["resend_action"] = self.resend_action
        if self.details:
            payload["details"] = dict(self.details)
        return payload


def build_feedback_diagnostics(
    *,
    response_type: str | None,
    report_type: str | None,
    branch: str | None,
    report_date: str | None,
    reason: str | None,
    governance_status: str | None,
    feedback_context: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Build one operator-facing diagnostics payload for review/reject feedback."""

    if response_type not in _REVIEW_RESPONSE_TYPES:
        return None

    feedback = _mapping(feedback_context)
    subject = _subject_context(
        report_type=report_type,
        branch=branch,
        report_date=report_date,
        reason=reason,
        feedback_context=feedback,
    )
    subject_report_type = _canonical_report_type(_text(subject.get("report_type")))
    if subject_report_type is None:
        subject_report_type = _canonical_report_type(_text(report_type))
    subject_branch = _text(subject.get("branch")) or _text(branch)
    subject_date = _text(subject.get("report_date")) or _text(report_date)
    subject_reason = _text(subject.get("reason")) or _text(reason)
    subject_status = (
        _text(subject.get("status"))
        or _text(governance_status)
        or _text(feedback.get("status"))
        or "needs_review"
    )
    subject_validation = _mapping(subject.get("validation"))
    subject_warnings = _mapping_list(subject.get("warnings"))
    subject_metrics = _mapping(subject.get("metrics"))
    subject_items = _mapping_list(subject.get("items"))
    subject_validation_codes = _validation_codes(subject_validation)
    confidence = _first_float(
        subject.get("confidence"),
        feedback.get("confidence"),
        _mapping(feedback.get("acceptance")).get("confidence"),
    )
    specialist_parser = (
        _text(subject.get("agent_name"))
        or _text(subject.get("specialist_parser"))
        or _SPECIALIST_PARSERS.get(subject_report_type or "")
    )
    raw_text = _subject_raw_text(subject=subject, feedback_context=feedback)
    if subject_report_type is None:
        subject_report_type = _report_type_from_raw_text(raw_text)
    validation_codes = set(subject_validation_codes)
    warning_codes = _warning_codes(subject_warnings)

    checks = [
        _report_type_check(subject_report_type),
        _branch_check(subject_branch),
        _date_check(subject_date),
        _mixed_content_check(
            reason=subject_reason,
            feedback_context=feedback,
            subject=subject,
        ),
    ]

    failed_rules: list[dict[str, Any]] = []
    if not _suppress_scope_failed_rules(
        reason=subject_reason,
        branch=subject_branch,
        report_date=subject_date,
        validation_codes=subject_validation_codes,
    ):
        failed_rules.extend(
            _scope_failed_rules(
                branch=subject_branch,
                report_date=subject_date,
                reason=subject_reason,
            )
        )
    failed_rules.extend(
        _mixed_failed_rules(
            reason=subject_reason,
            feedback_context=feedback,
            raw_text=raw_text,
        )
    )
    failed_rules.extend(
        _validation_failed_rules(
            report_type=subject_report_type,
            validation=subject_validation,
            metrics=subject_metrics,
            items=subject_items,
        )
    )

    should_add_parser_heuristics = (
        not failed_rules
        or subject_reason in {"validation_failed", "fallback_validation_failed", "invalid_input", "parser_failure"}
        or "parser_failure" in validation_codes
        or "parser_failure" in warning_codes
        or subject_status in {"invalid_input", "rejected"}
    )
    if should_add_parser_heuristics:
        if subject_report_type == "sales_income":
            failed_rules.extend(
                _sales_parser_failed_rules(
                    raw_text=raw_text,
                    existing_rules=failed_rules,
                )
            )
        elif subject_report_type == "bale_summary":
            failed_rules.extend(
                _bale_parser_failed_rules(
                    raw_text=raw_text,
                    metrics=subject_metrics,
                    items=subject_items,
                    warnings=subject_warnings,
                    existing_rules=failed_rules,
                )
            )

    mixed_child_review_rules = _mixed_child_review_failed_rules(
        reason=subject_reason,
        subject=subject,
    )
    if mixed_child_review_rules:
        has_specific_child_review = any(
            _text(rule.get("code")) not in {None, "mixed_child_requires_review"}
            for rule in mixed_child_review_rules
        )
        if has_specific_child_review or not failed_rules:
            failed_rules.extend(mixed_child_review_rules)

    validation_detail_rules = _validation_detail_failed_rules(subject_validation)
    if validation_detail_rules:
        existing_codes = {
            code
            for code in (_text(rule.get("code")) for rule in failed_rules)
            if code is not None
        }
        for rule in validation_detail_rules:
            code = _text(rule.get("code"))
            if code is None or code not in existing_codes:
                failed_rules.append(rule)

    if not failed_rules:
        failed_rules.extend(
            _confidence_failed_rules(
                reason=subject_reason,
                confidence=confidence,
                feedback_context=feedback,
            )
        )

    failed_rules = _dedupe_failed_rules(failed_rules)
    parser_check = _specialist_parser_check(
        report_type=subject_report_type,
        reason=subject_reason,
        failed_rules=failed_rules,
        validation=subject_validation,
        warning_codes=warning_codes,
        specialist_parser=specialist_parser,
    )
    checks.append(parser_check)

    stage = _diagnostic_stage(failed_rules=failed_rules, reason=subject_reason, validation=subject_validation)
    resend_action = _recommended_resend_action(
        report_type=subject_report_type,
        reason=subject_reason,
        failed_rules=failed_rules,
    )

    return RejectionDiagnostics(
        stage=stage,
        report_type=subject_report_type,
        branch=subject_branch,
        date=subject_date,
        status=subject_status,
        checks=[check.to_payload() for check in checks],
        failed_rules=failed_rules[:5],
        confidence=confidence,
        specialist_parser=specialist_parser,
        resend_action=resend_action,
        details={
            "report_label": _REPORT_LABELS.get(subject_report_type or "", "Report"),
            "response_reason": subject_reason,
        },
    ).to_payload()


def normalize_diagnostics(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return one normalized diagnostics payload when present."""

    if not isinstance(value, Mapping):
        return None

    checks = []
    for check in value.get("checks", []):
        if not isinstance(check, Mapping):
            continue
        name = _text(check.get("name"))
        passed = check.get("passed")
        if name is None or not isinstance(passed, bool):
            continue
        checks.append(
            DiagnosticCheck(
                name=name,
                passed=passed,
                result=_text(check.get("result")),
                detail=_text(check.get("detail")),
                expected=_text(check.get("expected")),
                received=_text(check.get("received")),
                stage=_text(check.get("stage")),
                parser=_text(check.get("parser")),
            ).to_payload()
        )

    failed_rules = []
    for rule in value.get("failed_rules", []):
        if not isinstance(rule, Mapping):
            continue
        reason = _text(rule.get("reason"))
        if reason is None:
            continue
        failed_rules.append(
            FailedRule(
                reason=reason,
                field=_text(rule.get("field")),
                expected=_text(rule.get("expected")),
                received=_text(rule.get("received")),
                action=_text(rule.get("action")),
                stage=_text(rule.get("stage")),
                code=_text(rule.get("code")),
            ).to_payload()
        )

    confidence = value.get("confidence")
    normalized_confidence = float(confidence) if isinstance(confidence, (int, float)) and not isinstance(confidence, bool) else None
    return RejectionDiagnostics(
        stage=_text(value.get("stage")) or "orchestrator",
        report_type=_canonical_report_type(_text(value.get("report_type"))),
        branch=_text(value.get("branch")),
        date=_text(value.get("date")),
        status=_text(value.get("status")) or "needs_review",
        checks=checks,
        failed_rules=failed_rules,
        confidence=normalized_confidence,
        specialist_parser=_text(value.get("specialist_parser")),
        resend_action=_text(value.get("resend_action")),
        details=dict(_mapping(value.get("details"))),
    ).to_payload()


def _subject_context(
    *,
    report_type: str | None,
    branch: str | None,
    report_date: str | None,
    reason: str | None,
    feedback_context: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the diagnostics subject for direct or mixed-child feedback."""

    if reason == "mixed_child_requires_review":
        child = _blocking_mixed_child(feedback_context)
        if child:
            payload = _mapping(child.get("payload"))
            return {
                "report_type": _text(child.get("response_report_type"))
                or _text(child.get("report_type"))
                or _text(report_type),
                "branch": _text(child.get("branch")) or _text(payload.get("branch")) or _text(branch),
                "report_date": _text(child.get("report_date")) or _text(payload.get("report_date")) or _text(report_date),
                "reason": _text(reason),
                "child_reason": _text(child.get("reason")),
                "status": _text(child.get("status")),
                "validation": dict(_mapping(child.get("validation"))),
                "warnings": _mapping_list(child.get("warnings")),
                "metrics": dict(_mapping(child.get("metrics"))),
                "items": _mapping_list(payload.get("items")),
                "confidence": payload.get("confidence"),
                "agent_name": _text(child.get("agent_name")),
                "validation_error_code": _text(child.get("validation_error_code")),
                "validation_error_message": _text(child.get("validation_error_message")),
            }

    return {
        "report_type": _text(report_type) or _text(feedback_context.get("report_type")),
        "branch": _text(branch) or _text(feedback_context.get("branch")) or _text(feedback_context.get("branch_hint")),
        "report_date": (
            _text(report_date)
            or _text(feedback_context.get("report_date"))
            or _text(feedback_context.get("normalized_report_date"))
            or _text(feedback_context.get("raw_report_date"))
        ),
        "reason": _text(reason) or _text(feedback_context.get("reason")),
        "status": _text(feedback_context.get("governance_status")) or _text(feedback_context.get("status")),
        "validation": dict(_mapping(feedback_context.get("validation"))),
        "warnings": _mapping_list(feedback_context.get("warnings")),
        "metrics": dict(_mapping(feedback_context.get("metrics"))),
        "items": _mapping_list(feedback_context.get("items")),
        "confidence": feedback_context.get("confidence"),
        "agent_name": _text(feedback_context.get("agent_name")),
    }


def _subject_raw_text(*, subject: Mapping[str, Any], feedback_context: Mapping[str, Any]) -> str | None:
    """Return the best available raw text for diagnostics heuristics."""

    payload_text = _text(_mapping(subject.get("payload")).get("raw_text"))
    if payload_text is not None:
        return payload_text

    raw_text = _text(feedback_context.get("raw_text"))
    if raw_text is not None:
        return raw_text

    raw_txt_path = _text(feedback_context.get("raw_txt_path"))
    if raw_txt_path is None:
        return None

    try:
        loaded = Path(raw_txt_path).read_text(encoding="utf-8")
    except OSError:
        return None
    stripped = loaded.strip()
    return stripped or None


def _report_type_check(report_type: str | None) -> DiagnosticCheck:
    if report_type is None or report_type == "unknown":
        return DiagnosticCheck(
            name="report_type_detected",
            passed=False,
            result="not_detected",
            detail="Report type could not be resolved safely.",
            stage="orchestrator",
        )
    return DiagnosticCheck(
        name="report_type_detected",
        passed=True,
        result="detected",
        detail=_REPORT_LABELS.get(report_type, "Report"),
        stage="orchestrator",
    )


def _branch_check(branch: str | None) -> DiagnosticCheck:
    if branch is None:
        return DiagnosticCheck(
            name="branch_resolved",
            passed=False,
            result="unresolved",
            expected="Branch: <branch>",
            received="missing",
            stage="orchestrator",
        )
    return DiagnosticCheck(
        name="branch_resolved",
        passed=True,
        result="resolved",
        detail=branch,
        stage="orchestrator",
    )


def _date_check(report_date: str | None) -> DiagnosticCheck:
    if report_date is None:
        return DiagnosticCheck(
            name="date_resolved",
            passed=False,
            result="unresolved",
            expected="Date: DD/MM/YY",
            received="missing",
            stage="orchestrator",
        )
    return DiagnosticCheck(
        name="date_resolved",
        passed=True,
        result="resolved",
        detail=report_date,
        stage="orchestrator",
    )


def _mixed_content_check(
    *,
    reason: str | None,
    feedback_context: Mapping[str, Any],
    subject: Mapping[str, Any],
) -> DiagnosticCheck:
    if reason in {"mixed_report_split_not_safe", "mixed_report_rejected"}:
        return DiagnosticCheck(
            name="mixed_content",
            passed=False,
            result="unsafe",
            detail="Mixed content could not be safely split for fan-out.",
            expected="One report family per WhatsApp message.",
            received=_mixed_received_value(feedback_context, None),
            stage="splitter",
        )
    if reason == "mixed_child_requires_review":
        return DiagnosticCheck(
            name="mixed_content",
            passed=True,
            result="split",
            detail="Mixed content was split before child validation.",
            stage="splitter",
        )
    if _mapping(feedback_context.get("mixed_detection")):
        return DiagnosticCheck(
            name="mixed_content",
            passed=True,
            result="single",
            detail="Single report path after mixed-content analysis.",
            stage="splitter",
        )
    if _mapping_list(feedback_context.get("mixed_children")):
        return DiagnosticCheck(
            name="mixed_content",
            passed=True,
            result="split",
            detail="Split child diagnostics were available.",
            stage="splitter",
        )
    return DiagnosticCheck(
        name="mixed_content",
        passed=True,
        result="single",
        detail="Single report detected.",
        stage="splitter",
    )


def _specialist_parser_check(
    *,
    report_type: str | None,
    reason: str | None,
    failed_rules: list[dict[str, Any]],
    validation: Mapping[str, Any],
    warning_codes: set[str],
    specialist_parser: str | None,
) -> DiagnosticCheck:
    if reason in {"mixed_report_split_not_safe", "mixed_report_rejected"}:
        return DiagnosticCheck(
            name="specialist_parser_stage",
            passed=False,
            result="not_run",
            detail="Mixed fan-out stopped before specialist parsing.",
            stage="splitter",
            parser=specialist_parser,
        )

    validation_details = _mapping(validation.get("details"))
    if validation_details.get("parser_failure") is True or "parser_failure" in warning_codes:
        return DiagnosticCheck(
            name="specialist_parser_stage",
            passed=False,
            result="failed",
            detail="Specialist parser failed safely.",
            stage="specialist_parser",
            parser=specialist_parser,
        )

    if any(_text(rule.get("stage")) == "specialist_parser" for rule in failed_rules):
        return DiagnosticCheck(
            name="specialist_parser_stage",
            passed=False,
            result="failed",
            detail="Specialist parser could not recognize one or more required fields.",
            stage="specialist_parser",
            parser=specialist_parser,
        )

    return DiagnosticCheck(
        name="specialist_parser_stage",
        passed=True,
        result="passed",
        detail="Specialist parser completed.",
        stage="specialist_parser",
        parser=specialist_parser or _SPECIALIST_PARSERS.get(report_type or ""),
    )


def _scope_failed_rules(
    *,
    branch: str | None,
    report_date: str | None,
    reason: str | None,
) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    if branch is None:
        rules.append(
            FailedRule(
                field="Branch",
                reason="Branch could not be resolved.",
                expected="Branch: <branch>",
                received="missing",
                action="Add the branch line and resend.",
                stage="orchestrator",
                code="branch_unresolved",
            ).to_payload()
        )
    if report_date is None:
        rules.append(
            FailedRule(
                field="Date",
                reason="Report date could not be resolved.",
                expected="Date: DD/MM/YY",
                received="missing",
                action="Add the report date and resend.",
                stage="orchestrator",
                code="date_unresolved",
            ).to_payload()
        )
    if reason == "route_requires_review":
        rules.append(
            FailedRule(
                field="Routing",
                reason="The report could not be routed to a supported specialist safely.",
                expected="A supported report title, branch, and date.",
                received="routing requires review",
                action="Correct the header fields and resend.",
                stage="orchestrator",
                code="route_requires_review",
            ).to_payload()
        )
    return rules


def _mixed_failed_rules(
    *,
    reason: str | None,
    feedback_context: Mapping[str, Any],
    raw_text: str | None,
) -> list[dict[str, Any]]:
    if reason not in {"mixed_report_split_not_safe", "mixed_report_rejected"}:
        return []
    return [
        FailedRule(
            field="Mixed content",
            reason="Mixed content could not be safely split.",
            expected="One report family per WhatsApp message with a supported title, branch, and date.",
            received=_mixed_received_value(feedback_context, raw_text),
            action="Send one report family per WhatsApp message and resend.",
            stage="splitter",
            code=reason,
        ).to_payload()
    ]


def _mixed_child_review_failed_rules(
    *,
    reason: str | None,
    subject: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if reason != "mixed_child_requires_review":
        return []

    validation_error_message = _text(subject.get("validation_error_message"))
    validation_error_code = _text(subject.get("validation_error_code"))
    child_reason = _text(subject.get("child_reason"))
    if validation_error_message is not None or validation_error_code is not None:
        return [
            FailedRule(
                field="Split child review",
                reason=validation_error_message or child_reason or "One split child still needs review.",
                expected="One complete report per WhatsApp message for the reviewed child.",
                received=validation_error_code or "needs_review",
                action="Please resend the report that still needs review as one report per message.",
                stage="orchestrator",
                code=validation_error_code or "mixed_child_requires_review",
            ).to_payload()
        ]

    return [
        FailedRule(
            field="Split child review",
            reason="TAOP split the message into multiple reports, but one split report still needs review.",
            expected="One complete report per WhatsApp message for the reviewed child.",
            received="Blocking split child did not include enough validated detail to auto-accept.",
            action="Please resend the report that still needs review as one report per message.",
            stage="orchestrator",
            code="mixed_child_requires_review",
        ).to_payload()
    ]


def _validation_failed_rules(
    *,
    report_type: str | None,
    validation: Mapping[str, Any],
    metrics: Mapping[str, Any],
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    rejections = validation.get("rejections")
    if not isinstance(rejections, list):
        return rules

    for rejection in rejections:
        if not isinstance(rejection, Mapping):
            continue
        code = _text(rejection.get("reason_code")) or _text(rejection.get("code")) or "validation_failed"
        message = _text(rejection.get("reason_detail")) or _text(rejection.get("message")) or "Validation failed."
        field = _friendly_field(_text(rejection.get("field")))
        expected = None
        received = None
        stage = "validation"

        if code == "unsupported_report_title":
            field = "Report title"
            message = "Unsupported report title."
            expected = _text(rejection.get("expected_title")) or _EXPECTED_TITLES.get(report_type or "")
            received = _text(rejection.get("received_title")) or "missing"
        elif code == "sales_totals_mismatch":
            field = "TOTALS"
            message = "Sales totals do not match till/payment totals."
            expected = _sales_totals_triplet(rejection, prefix="expected")
            received = _sales_totals_triplet(rejection, prefix="declared")
        elif code in {"missing_branch", "branch_unresolved"}:
            continue
        elif code in {"missing_report_date", "date_unresolved"}:
            continue
        elif code == "invalid_report_date":
            field = "Date"
            expected = "Date: DD/MM/YY"
            received = _text(rejection.get("raw_report_date")) or "missing"
        elif code == "missing_items":
            field = "Item rows"
            message = "Bale item rows were not recognized."
            expected = "# 01. Item Name / Qty: 10 / Amt: K100"
            received = "Totals were present but no item rows were parsed."
            stage = "specialist_parser"
        elif code == "missing_required_field":
            stage = "specialist_parser"
            if field and field.endswith("Qty"):
                message = "Missing bale item quantity."
                expected = "Qty: <number>"
                received = "missing"
            elif field and field.endswith("Amount"):
                message = "Missing bale item amount."
                expected = "Amt: K<number>"
                received = "missing"
        elif code == "invalid_totals" and report_type == "bale_summary":
            if _text(rejection.get("field")) == "metrics.total_qty":
                field = "Total Qty"
                message = "Declared total quantity does not match parsed bale rows."
                expected = _format_qty(sum(_qty_or_zero(item.get("qty")) for item in items))
                received = _format_qty(_qty_or_zero(metrics.get("total_qty")))
            elif _text(rejection.get("field")) == "metrics.total_amount":
                field = "Total Amount"
                message = "Declared total amount does not match parsed bale rows."
                expected = _format_money(sum(_money_or_zero(item.get("amount")) for item in items))
                received = _format_money(_money_or_zero(metrics.get("total_amount")))

        rules.append(
            FailedRule(
                field=field,
                reason=message,
                expected=expected,
                received=received,
                action=None,
                stage=stage,
                code=code,
            ).to_payload()
        )
    return rules


def _sales_parser_failed_rules(
    *,
    raw_text: str | None,
    existing_rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return []

    rules: list[dict[str, Any]] = []
    existing_codes = {_text(rule.get("code")) for rule in existing_rules}
    existing_messages = {_text(rule.get("reason")) for rule in existing_rules}
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]

    if "unsupported_report_title" not in existing_codes:
        title_rule = _sales_title_rule(lines)
        if title_rule is not None:
            rules.append(title_rule)

    till_headers = _sales_till_headers(lines)
    if till_headers:
        for header in till_headers:
            if not header["supported"]:
                rules.append(
                    FailedRule(
                        field="Till header",
                        reason="Unsupported till header format.",
                        expected="Till#1 or Till#2",
                        received=header["line"],
                        action="Use Till#<number> for each till block.",
                        stage="specialist_parser",
                        code="unsupported_till_header",
                    ).to_payload()
                )
            segment_lines = header["segment_lines"]
            label = header["label"]
            if not any(_CASHIER_PATTERN.match(line) for line in segment_lines):
                rules.append(
                    FailedRule(
                        field="Cashier",
                        reason="Missing required till operator.",
                        expected="Cashier per till",
                        received=f"missing in {label}",
                        action="Add the cashier line for the till block.",
                        stage="specialist_parser",
                        code="missing_cashier",
                    ).to_payload()
                )
            if not any(_ASSISTANT_PATTERN.match(line) for line in segment_lines):
                rules.append(
                    FailedRule(
                        field="Assistant/Monitor",
                        reason="Missing required till support operator.",
                        expected="Assistant or Monitor per till",
                        received=f"missing in {label}",
                        action="Add the assistant or monitor line for the till block.",
                        stage="specialist_parser",
                        code="missing_assistant_or_monitor",
                    ).to_payload()
                )
            if not any(_TCASH_PATTERN.match(line) for line in segment_lines):
                rules.append(
                    FailedRule(
                        field="T/Cash",
                        reason="Missing till cash total.",
                        expected="T/Cash: <amount>",
                        received=f"missing in {label}",
                        action="Add the till cash line for the till block.",
                        stage="specialist_parser",
                        code="missing_t_cash",
                    ).to_payload()
                )
            if not any(_TCARD_PATTERN.match(line) for line in segment_lines):
                rules.append(
                    FailedRule(
                        field="T/Card",
                        reason="Missing till card total.",
                        expected="T/Card: <amount>",
                        received=f"missing in {label}",
                        action="Add the till card line for the till block.",
                        stage="specialist_parser",
                        code="missing_t_card",
                    ).to_payload()
                )
            if not any(_ZREADING_PATTERN.match(line) for line in segment_lines):
                rules.append(
                    FailedRule(
                        field="Z/Reading",
                        reason="Missing till Z/Reading.",
                        expected="Z/Reading: <amount>",
                        received=f"missing in {label}",
                        action="Add the Z/Reading line for the till block.",
                        stage="specialist_parser",
                        code="missing_z_reading",
                    ).to_payload()
                )
    elif any(
        pattern.match(line)
        for pattern in (_TCASH_PATTERN, _TCARD_PATTERN, _ZREADING_PATTERN)
        for line in lines
    ):
        rules.append(
            FailedRule(
                field="Till rows",
                reason="Till rows were not recognized.",
                expected="Till#1 or Till#2 blocks before till totals.",
                received="Till totals were present without supported till headers.",
                action="Send each till block with a supported Till# header.",
                stage="specialist_parser",
                code="missing_till_rows",
            ).to_payload()
        )

    customer_rule = _sales_customer_count_rule(lines)
    if customer_rule is not None:
        rules.append(customer_rule)

    has_till_context = bool(till_headers) or any(
        pattern.match(line)
        for pattern in (_TCASH_PATTERN, _TCARD_PATTERN, _ZREADING_PATTERN)
        for line in lines
    )
    if has_till_context and not any(_ADDITIONAL_INFO_PATTERN.match(line) for line in lines):
        rules.append(
            FailedRule(
                field="Additional Information",
                reason="Additional information section was not found.",
                expected="ADDITIONAL INFORMATION",
                received="missing",
                action="Add the additional information section and resend.",
                stage="specialist_parser",
                code="missing_additional_information",
            ).to_payload()
        )

    rules = _dedupe_failed_rules(rules)
    filtered_rules: list[dict[str, Any]] = []
    for rule in rules:
        reason = _text(rule.get("reason"))
        if reason is not None and reason in existing_messages:
            continue
        filtered_rules.append(rule)
    return filtered_rules[:5]


def _sales_title_rule(lines: list[str]) -> dict[str, Any] | None:
    for report_type, pattern in _TITLE_LINE_PATTERNS:
        if report_type != "sales_income":
            continue
        for line in lines[:8]:
            if pattern.match(line):
                return FailedRule(
                    field="Report title",
                    reason="Unsupported report title.",
                    expected=_EXPECTED_TITLES["sales_income"],
                    received=line,
                    action="Use the exact sales report title and resend.",
                    stage="validation",
                    code="unsupported_report_title",
                ).to_payload()
    return None


def _sales_till_headers(lines: list[str]) -> list[dict[str, Any]]:
    headers: list[dict[str, Any]] = []
    indices = [index for index, line in enumerate(lines) if _TILL_HEADER_ANY_PATTERN.match(line)]
    for position, index in enumerate(indices):
        next_index = indices[position + 1] if position + 1 < len(indices) else len(lines)
        segment_lines = [
            line
            for line in lines[index + 1 : next_index]
            if not _SECTION_BOUNDARY_PATTERN.match(line)
        ]
        line = lines[index]
        number_match = re.search(r"(\d+)", line)
        number = number_match.group(1) if number_match is not None else "?"
        headers.append(
            {
                "line": line,
                "supported": _TILL_HEADER_SUPPORTED_PATTERN.match(line) is not None,
                "label": f"Till#{number}",
                "segment_lines": segment_lines,
            }
        )
    return headers


def _sales_customer_count_rule(lines: list[str]) -> dict[str, Any] | None:
    normalized_lines = [line.casefold() for line in lines]
    has_main_door = any("main door" in line for line in normalized_lines)
    has_guest_served = any(
        token in line
        for line in normalized_lines
        for token in ("guest/customer serve", "guest customer serve", "customers served")
    )
    has_traffic = any("traffic" in line for line in normalized_lines)
    has_served = any(
        re.search(r"\b(served|customers served)\b", line) is not None
        for line in normalized_lines
    )
    if (has_main_door and has_guest_served) or (has_traffic and has_served):
        return None

    received_lines = [
        line
        for line in lines
        if any(token in line.casefold() for token in ("guest", "customer", "traffic", "served", "head count", "main door"))
    ]
    if not received_lines:
        return None
    return FailedRule(
        field="Customer Count",
        reason="Unsupported customer count labels.",
        expected="Main Door and Guest/customer serve OR Traffic and Served",
        received=" | ".join(received_lines[:2]),
        action="Use one supported customer count label pair.",
        stage="specialist_parser",
        code="unsupported_customer_count_labels",
    ).to_payload()


def _bale_parser_failed_rules(
    *,
    raw_text: str | None,
    metrics: Mapping[str, Any],
    items: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    existing_rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return []

    existing_codes = {_text(rule.get("code")) for rule in existing_rules}
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    rules: list[dict[str, Any]] = []
    headers = [index for index, line in enumerate(lines) if _BALE_ITEM_HEADER_PATTERN.match(line)]

    if not items and ("missing_items" not in existing_codes):
        if headers or metrics.get("total_qty") is not None or metrics.get("total_amount") is not None:
            rules.append(
                FailedRule(
                    field="Item rows",
                    reason="Bale item rows were not recognized.",
                    expected="# 01. Item Name / Qty: 10 / Amt: K100",
                    received="Totals were present but no bale item rows were parsed.",
                    action="Resend the bale rows using one supported item format.",
                    stage="specialist_parser",
                    code="no_item_rows_parsed",
                ).to_payload()
            )

    for header_index in headers[:3]:
        header_line = lines[header_index]
        detail_lines = []
        for line in lines[header_index + 1 : header_index + 4]:
            if _BALE_ITEM_HEADER_PATTERN.match(line) or _SECTION_BOUNDARY_PATTERN.match(line):
                break
            detail_lines.append(line)
        if not any(_BALE_QTY_PATTERN.match(line) for line in detail_lines):
            rules.append(
                FailedRule(
                    field="Qty",
                    reason="Missing quantity in bale row.",
                    expected="Qty: <number>",
                    received=f"missing after {header_line}",
                    action="Add the quantity line for the bale row.",
                    stage="specialist_parser",
                    code="missing_qty",
                ).to_payload()
            )
        if not any(_BALE_AMOUNT_PATTERN.match(line) for line in detail_lines):
            rules.append(
                FailedRule(
                    field="Amount",
                    reason="Missing amount in bale row.",
                    expected="Amt: K<number>",
                    received=f"missing after {header_line}",
                    action="Add the amount line for the bale row.",
                    stage="specialist_parser",
                    code="missing_amount",
                ).to_payload()
            )

    if any(_text(warning.get("code")) == "missing_provenance" for warning in warnings):
        rules.append(
            FailedRule(
                field="Prepared/Checked By",
                reason="Prepared By or Checked By fields are incomplete.",
                expected="Prepared By: <name> and Checked By: <name>",
                received="one or both provenance fields were missing",
                action="Add the prepared/check lines and resend.",
                stage="specialist_parser",
                code="missing_prepared_check_fields",
            ).to_payload()
        )

    return _dedupe_failed_rules(rules)[:5]


def _confidence_failed_rules(
    *,
    reason: str | None,
    confidence: float | None,
    feedback_context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if reason not in _CONFIDENCE_REASONS:
        return []
    acceptance = _mapping(feedback_context.get("acceptance"))
    thresholds = _mapping(acceptance.get("thresholds"))
    expected = None
    if reason == "confidence_between_review_and_accept_thresholds":
        auto_accept_min = _first_float(thresholds.get("auto_accept_min"))
        if auto_accept_min is not None:
            expected = f">= {auto_accept_min:.2f}"
    elif reason == "confidence_below_reject_threshold":
        reject_max = _first_float(thresholds.get("reject_max"))
        if reject_max is not None:
            expected = f"> {reject_max:.2f}"

    received = f"{confidence:.2f}" if confidence is not None else "not available"
    reason_detail = {
        "confidence_between_review_and_accept_thresholds": "Confidence is below the auto-accept threshold.",
        "strict_candidate_below_reject_threshold": "Confidence stayed below the strict candidate threshold.",
        "confidence_below_reject_threshold": "Confidence fell below the reject threshold.",
        "confidence_missing": "Confidence was not available for governance.",
    }.get(reason, "Confidence requires review.")
    return [
        FailedRule(
            field="Confidence",
            reason=reason_detail,
            expected=expected,
            received=received,
            action="Correct only the failed fields above and resend if the report values were wrong.",
            stage="governance",
            code=reason,
        ).to_payload()
    ]


def _validation_detail_failed_rules(validation: Mapping[str, Any]) -> list[dict[str, Any]]:
    details = _mapping(validation.get("details"))
    accountability = _mapping(details.get("accountability"))
    validation_error_code = (
        _text(details.get("validation_error_code"))
        or _text(accountability.get("validation_error_code"))
        or _text(accountability.get("failing_rule"))
    )
    validation_error_message = (
        _text(details.get("validation_error_message"))
        or _text(accountability.get("validation_error_message"))
        or _text(accountability.get("reason_detail"))
    )
    if validation_error_message is None and validation_error_code is None:
        return []

    field = None
    if validation_error_code == "declared_summary_total_mismatch":
        field = "Attendance summary"
    action = _text(accountability.get("recommended_correction"))
    return [
        FailedRule(
            field=field,
            reason=validation_error_message or "Validation detail requires review.",
            expected=None,
            received=validation_error_code,
            action=action,
            stage="validation",
            code=validation_error_code,
        ).to_payload()
    ]


def _suppress_scope_failed_rules(
    *,
    reason: str | None,
    branch: str | None,
    report_date: str | None,
    validation_codes: set[str],
) -> bool:
    if reason in {"mixed_report_split_not_safe", "mixed_report_rejected"}:
        return branch is None and report_date is None
    if reason != "mixed_child_requires_review":
        return False
    if branch is not None or report_date is not None:
        return False
    explicit_scope_codes = {
        "missing_branch",
        "branch_unresolved",
        "missing_report_date",
        "invalid_report_date",
        "date_unresolved",
    }
    return not bool(validation_codes & explicit_scope_codes)


def _blocking_mixed_child(feedback_context: Mapping[str, Any]) -> dict[str, Any] | None:
    children = _mapping_list(feedback_context.get("mixed_children"))
    accepted_statuses = {"accepted", "accepted_with_warning", "ready"}
    for child in children:
        blocks = child.get("blocks_transactional_processing")
        status = _text(child.get("status"))
        if blocks is not False and status not in accepted_statuses:
            return child
    for child in children:
        status = _text(child.get("status"))
        if status not in accepted_statuses:
            return child
    return None


def _validation_codes(validation: Mapping[str, Any]) -> set[str]:
    codes: set[str] = set()
    reason_codes = validation.get("reason_codes")
    if isinstance(reason_codes, list):
        for code in reason_codes:
            cleaned = _text(code)
            if cleaned is not None:
                codes.add(cleaned)
    rejections = validation.get("rejections")
    if isinstance(rejections, list):
        for rejection in rejections:
            if not isinstance(rejection, Mapping):
                continue
            cleaned = _text(rejection.get("reason_code")) or _text(rejection.get("code"))
            if cleaned is not None:
                codes.add(cleaned)
    return codes


def _warning_codes(warnings: list[dict[str, Any]]) -> set[str]:
    return {
        code
        for code in (_text(warning.get("code")) for warning in warnings)
        if code is not None
    }


def _diagnostic_stage(
    *,
    failed_rules: list[dict[str, Any]],
    reason: str | None,
    validation: Mapping[str, Any],
) -> str:
    for rule in failed_rules:
        stage = _text(rule.get("stage"))
        if stage is not None:
            return stage
    if reason in {"mixed_report_split_not_safe", "mixed_report_rejected"}:
        return "splitter"
    validation_details = _mapping(validation.get("details"))
    if validation_details.get("parser_failure") is True:
        return "specialist_parser"
    return "orchestrator"


def _recommended_resend_action(
    *,
    report_type: str | None,
    reason: str | None,
    failed_rules: list[dict[str, Any]],
) -> str:
    report_label = _REPORT_LABELS.get(report_type or "", "report")
    if reason in {"mixed_report_split_not_safe", "mixed_report_rejected"}:
        return "Send one report family per WhatsApp message and resend."
    failed_rule_codes = {
        code
        for code in (_text(rule.get("code")) for rule in failed_rules)
        if code is not None
    }
    if "sales_totals_mismatch" in failed_rule_codes:
        return f"Correct the TOTALS section and resend the {report_label}."
    if {"branch_unresolved", "missing_branch"} & failed_rule_codes:
        return "Branch missing or unclear. Add Branch: <branch>."
    if {"missing_report_date", "invalid_report_date", "date_unresolved"} & failed_rule_codes:
        return "Date missing or unclear. Add Date: DD/MM/YY."
    if any(_text(rule.get("code")) == "unsupported_report_title" for rule in failed_rules):
        expected_title = _EXPECTED_TITLES.get(report_type or "")
        if expected_title:
            return f"Use the exact title {expected_title} and resend the {report_label}."
    if {"missing_items", "no_item_rows_parsed"} & failed_rule_codes:
        return "Resend the bale rows using one supported item format."
    if report_type == "supervisor_control" and "missing_fields" in failed_rule_codes:
        return "Report format failed SOP validation. Recheck required fields and totals."
    if reason == "mixed_child_requires_review":
        return "Please resend the report that still needs review as one report per message."
    if len(failed_rules) == 1:
        only_rule_action = _text(failed_rules[0].get("action"))
        if only_rule_action is not None:
            return only_rule_action
    if report_type == "sales_income":
        return f"Correct only the failed fields above and resend the {report_label}."
    if report_type == "bale_summary":
        return f"Correct only the failed fields above and resend the {report_label}."
    if report_type == "supervisor_control":
        return f"Correct only the failed fields above and resend the {report_label}."
    return f"Correct only the failed fields above and resend the {report_label}."


def _mixed_received_value(feedback_context: Mapping[str, Any], raw_text: str | None) -> str:
    mixed_detection = _mapping(feedback_context.get("mixed_detection"))
    boundary_hints = mixed_detection.get("boundary_hints")
    titles: list[str] = []
    boundary_hint_text = None
    if isinstance(boundary_hints, list):
        for hint in boundary_hints:
            if not isinstance(hint, Mapping):
                continue
            raw_line = _text(hint.get("raw_line"))
            if raw_line is not None and raw_line not in titles:
                titles.append(raw_line)
            if boundary_hint_text is None:
                line_number = hint.get("line_number")
                if isinstance(line_number, int) and raw_line is not None:
                    boundary_hint_text = f"line {line_number}: {raw_line}"

    if isinstance(raw_text, str):
        for line in raw_text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            for _, pattern in _MIXED_TITLE_PATTERNS:
                if pattern.match(stripped):
                    if stripped not in titles:
                        titles.append(stripped)
                    break

    fragments: list[str] = []
    if titles:
        fragments.append("Detected titles: " + " | ".join(titles[:3]))
    if boundary_hint_text is not None:
        fragments.append("Unsafe boundary: " + boundary_hint_text)
    if not fragments:
        return "Mixed report content was detected."
    return ". ".join(fragments)


def _friendly_field(field: str | None) -> str | None:
    if field is None:
        return None
    mapping = {
        "branch": "Branch",
        "report_date": "Date",
        "metrics.gross_sales": "Gross Sales",
        "metrics.total_qty": "Total Qty",
        "metrics.total_amount": "Total Amount",
        "metrics.served": "Served",
        "items": "Item rows",
    }
    if field in mapping:
        return mapping[field]
    if field.endswith(".qty"):
        return "Qty"
    if field.endswith(".amount"):
        return "Amount"
    return field.replace("_", " ").replace(".", " ").title()


def _sales_totals_triplet(rejection: Mapping[str, Any], *, prefix: str) -> str | None:
    cash = rejection.get(f"{prefix}_total_cash")
    card = rejection.get(f"{prefix}_total_card")
    sales = rejection.get(f"{prefix}_total_sales")
    parts: list[str] = []
    if isinstance(cash, (int, float)) and not isinstance(cash, bool):
        parts.append(f"Cash {_format_money(float(cash))}")
    if isinstance(card, (int, float)) and not isinstance(card, bool):
        parts.append(f"Card {_format_money(float(card))}")
    if isinstance(sales, (int, float)) and not isinstance(sales, bool):
        parts.append(f"Sales {_format_money(float(sales))}")
    return " | ".join(parts) if parts else None


def _dedupe_failed_rules(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for rule in rules:
        reason = _text(rule.get("reason")) or ""
        field = _text(rule.get("field")) or ""
        expected = _text(rule.get("expected")) or ""
        received = _text(rule.get("received")) or ""
        key = (reason, field, expected, received)
        if key not in unique:
            unique[key] = rule
    return list(unique.values())


def _canonical_report_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().casefold()
    if normalized in {"report", "reports", "report_status", "route", "routing", "status"}:
        return None
    if normalized in {"sales", "sales_income", "day_end_sales"}:
        return "sales_income"
    if normalized in {"attendance", "staff_attendance", "hr_attendance", "hr_staffing"}:
        return "staff_attendance"
    if normalized in {"staff_performance", "hr_performance"}:
        return "staff_performance"
    if normalized in {"bale_summary", "pricing_stock_release", "bale_release"}:
        return "bale_summary"
    if normalized in {"supervisor_control", "supervisor_control_summary"}:
        return "supervisor_control"
    if normalized == "mixed":
        return "mixed"
    if normalized == "unknown":
        return "unknown"
    return normalized or None


def _report_type_from_raw_text(raw_text: str | None) -> str | None:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return None

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        for report_type, pattern in _TITLE_LINE_PATTERNS:
            if pattern.match(line):
                return report_type
        for title, pattern in _MIXED_TITLE_PATTERNS:
            if pattern.match(line):
                if title == "DAY-END SALES REPORT":
                    return "sales_income"
                if title == "SUPERVISOR CONTROL REPORT":
                    return "supervisor_control"
                if title == "SUPERVISOR CONTROL SUMMARY":
                    return "supervisor_control"
                if title == "ATTENDANCE REPORT":
                    return "staff_attendance"
                if title == "STAFF PERFORMANCE REPORT":
                    return "staff_performance"
                if title == "DAILY BALE SUMMARY - RELEASED TO RAIL":
                    return "bale_summary"
    return None


def _format_money(value: float | None) -> str:
    if value is None:
        return "missing"
    return f"K{value:,.2f}"


def _format_qty(value: int | float | None) -> str:
    if value is None:
        return "missing"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return f"{value}"


def _qty_or_zero(value: object) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _money_or_zero(value: object) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _mapping_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _first_float(*values: object) -> float | None:
    for value in values:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None
