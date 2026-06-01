"""Render deterministic WhatsApp-safe response text for Phase C1."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from difflib import SequenceMatcher
import hashlib
import logging
import os
from pathlib import Path
import re
from typing import Any

from apps.conversation_policy import ALLOWED_RESPONSE_TYPES, reason_text_for_response
from packages.llm_adapter.openai_client import refine_response_text
from packages.observability import record_conversation_llm_event
from packages.normalization.branches import normalize_branch
from packages.normalization.dates import normalize_report_date
from packages.taop_feedback import build_rejection_feedback, build_report_feedback, build_review_feedback
from packages.validation import build_feedback_diagnostics

CONVERSATION_LLM_ENABLED = False
CONVERSATION_LLM_MODE = "off"
_CONVERSATION_LLM_ENABLED_ENV = "TOPTOWN_CONVERSATION_LLM_ENABLED"
_CONVERSATION_LLM_MODE_ENV = "TOPTOWN_CONVERSATION_LLM_MODE"
_SUPPORTED_REPORT_LINES = (
    "DAY-END SALES REPORT",
    "ATTENDANCE REPORT",
    "STAFF PERFORMANCE REPORT",
    "DAILY BALE SUMMARY - RELEASED TO RAIL",
    "SUPERVISOR CONTROL REPORT",
)
_BRANCH_LINE_PATTERN = re.compile(r"^\s*branch\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE | re.MULTILINE)
_DATE_LINE_PATTERN = re.compile(r"^\s*date\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE | re.MULTILINE)
_LLM_REWRITE_CACHE: dict[str, str] = {}
logger = logging.getLogger(__name__)


def render_whatsapp_response(response_context: Mapping[str, Any]) -> dict[str, Any]:
    """Return one deterministic WhatsApp response envelope."""

    normalized_response_context = _with_feedback_diagnostics(response_context)
    channel = _string_or_none(response_context.get("channel")) or "whatsapp"
    is_replay = bool(response_context.get("is_replay") is True)
    should_reply = bool(response_context.get("should_reply") is True)
    response_type = _string_or_none(normalized_response_context.get("response_type"))
    if not should_reply or response_type is None:
        return {
            "response_type": None,
            "response_text": None,
            "should_send": False,
            "channel": channel,
            "is_replay": is_replay,
        }
    if response_type == "command_reply":
        return {
            "response_type": response_type,
            "response_text": _required_text(
                normalized_response_context.get("response_text"),
                field_name="response_text",
            ),
            "should_send": True,
            "channel": channel,
            "is_replay": is_replay,
        }
    if response_type == "operational_query_status":
        rendered = {
            "response_type": response_type,
            "response_text": _required_text(
                normalized_response_context.get("response_text"),
                field_name="response_text",
            ),
            "should_send": True,
            "channel": channel,
            "is_replay": is_replay,
        }
        feedback = normalized_response_context.get("feedback")
        if isinstance(feedback, Mapping):
            rendered["feedback"] = dict(feedback)
        return rendered
    if response_type not in ALLOWED_RESPONSE_TYPES:
        raise ValueError(f"Unsupported response type `{response_type}`.")

    feedback_payload = build_report_feedback(normalized_response_context)

    structured_feedback: Mapping[str, Any] | None = None
    if feedback_payload is not None:
        structured_feedback = dict(feedback_payload)
        response_text = _required_text(feedback_payload.get("response_text"), field_name="feedback.response_text")
        feedback = {key: value for key, value in feedback_payload.items() if key != "response_text"}
        if not feedback:
            feedback = None
    elif response_type == "accepted_ack":
        response_text = _render_accepted_ack(normalized_response_context)
        feedback = None
    elif response_type == "correction_accepted_ack":
        response_text = _render_correction_accepted_ack(normalized_response_context)
        feedback = None
    elif response_type == "correction_review_ack":
        response_text = _render_correction_review_ack(normalized_response_context)
        feedback = None
    elif response_type == "correction_repeat_fix_request":
        response_text = _render_correction_repeat_fix_request(normalized_response_context)
        feedback = None
    elif response_type == "review_ack":
        response_text = _render_review_ack(normalized_response_context)
        feedback = None
    elif response_type == "rejected_fix_request":
        response_text = _render_rejected_fix_request(normalized_response_context)
        feedback = None
    elif response_type in {"duplicate_ack", "duplicate_notice"}:
        response_text = _render_duplicate_notice(normalized_response_context)
        feedback = None
    else:
        response_text = _render_unknown_message_guidance()
        feedback = None

    response_text = _maybe_refine_response_text(
        base_text=response_text,
        response_context=normalized_response_context,
        structured_feedback=structured_feedback,
    )
    rendered = {
        "response_type": response_type,
        "response_text": response_text,
        "should_send": True,
        "channel": channel,
        "is_replay": is_replay,
    }
    if feedback is not None:
        rendered["feedback"] = feedback
    return rendered


def _with_feedback_diagnostics(response_context: Mapping[str, Any]) -> Mapping[str, Any]:
    """Attach diagnostics when direct callers bypass conversation routing."""

    feedback_context = response_context.get("feedback_context")
    if not isinstance(feedback_context, Mapping) or isinstance(feedback_context.get("diagnostics"), Mapping):
        return response_context

    diagnostics = build_feedback_diagnostics(
        response_type=_string_or_none(response_context.get("response_type")),
        report_type=_string_or_none(response_context.get("report_type")),
        branch=_string_or_none(response_context.get("branch")),
        report_date=_string_or_none(response_context.get("report_date")),
        reason=_string_or_none(response_context.get("reason")),
        governance_status=_string_or_none(response_context.get("governance_status")),
        feedback_context=feedback_context,
    )
    if diagnostics is None:
        return response_context

    updated_feedback_context = dict(feedback_context)
    updated_feedback_context["diagnostics"] = diagnostics
    updated_response_context = dict(response_context)
    updated_response_context["feedback_context"] = updated_feedback_context
    return updated_response_context


def _render_accepted_ack(response_context: Mapping[str, Any]) -> str:
    header = f"✅ {_report_label(response_context)} received{_scope_suffix(response_context)}."
    return "\n".join([header, "Processed successfully."])


def _render_correction_accepted_ack(response_context: Mapping[str, Any]) -> str:
    header = f"✅ Corrected {_report_label(response_context)} received{_scope_suffix(response_context)}."
    return "\n".join([header, "Processed successfully."])


def _render_correction_review_ack(response_context: Mapping[str, Any]) -> str:
    return build_review_feedback(response_context)


def _render_correction_repeat_fix_request(response_context: Mapping[str, Any]) -> str:
    return build_rejection_feedback(response_context)


def _render_review_ack(response_context: Mapping[str, Any]) -> str:
    return build_review_feedback(response_context)


def _render_rejected_fix_request(response_context: Mapping[str, Any]) -> str:
    return build_rejection_feedback(response_context)


def _render_duplicate_notice(response_context: Mapping[str, Any]) -> str:
    feedback = build_report_feedback(response_context)
    if isinstance(feedback, Mapping):
        response_text = _string_or_none(feedback.get("response_text"))
        if response_text is not None:
            return response_text
    return "\n".join(
        [
            "ℹ️ This report was already received and processed earlier.",
            "No new processing was applied.",
        ]
    )


def _render_unknown_message_guidance() -> str:
    return build_rejection_feedback({"report_type": "unknown"})


def _report_label(response_context: Mapping[str, Any]) -> str:
    report_type = _string_or_none(response_context.get("report_type"))
    label = _REPORT_LABELS.get(report_type or "")
    if label is not None:
        return label
    return "REPORT"


def _branch_label(value: object) -> str | None:
    text = _string_or_none(value)
    if text is None or text == "unknown":
        return None
    return " ".join(part.capitalize() for part in text.replace("_", " ").split())


def _format_report_date(value: object) -> str | None:
    text = _string_or_none(value)
    if text is None:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        year = text[2:4]
        month = text[5:7]
        day = text[8:10]
        if day.isdigit() and month.isdigit() and year.isdigit():
            return f"{day}/{month}/{year}"
    return None


def _scope_suffix(response_context: Mapping[str, Any]) -> str:
    branch = _branch_label(response_context.get("branch"))
    report_date = _format_report_date(response_context.get("report_date"))
    if branch is not None and report_date is not None:
        return f" for {branch}, {report_date}"
    if branch is not None:
        return f" for {branch}"
    if report_date is not None:
        return f" for {report_date}"
    return ""


def _safe_reason_text(response_context: Mapping[str, Any], *, fallback: str) -> str:
    response_type = _required_text(response_context.get("response_type"), field_name="response_type")
    reason = _string_or_none(response_context.get("reason"))
    if reason is None:
        return fallback
    rendered = reason_text_for_response(response_type, reason)
    if rendered is not None:
        return rendered
    return fallback


def _maybe_refine_response_text(
    *,
    base_text: str,
    response_context: Mapping[str, Any],
    structured_feedback: Mapping[str, Any] | None = None,
) -> str:
    original_text = base_text
    response_type = _string_or_none(response_context.get("response_type"))
    channel = _string_or_none(response_context.get("channel")) or "whatsapp"
    reason = _string_or_none(response_context.get("reason"))
    is_replay = bool(response_context.get("is_replay") is True)
    observability = _conversation_llm_metadata(response_context)
    report_date = observability["report_date"] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    branch = observability["branch"]
    report_type = observability["report_type"]

    skip_reason = _llm_skip_reason(
        base_text=base_text,
        response_context=response_context,
        structured_feedback=structured_feedback,
    )
    if skip_reason is not None:
        record_conversation_llm_event(
            report_date=report_date,
            outcome="skipped",
            response_type=response_type,
            channel=channel,
            branch=branch,
            report_type=report_type,
            event_report_date=observability["report_date"],
            replay_suppressed=is_replay,
            reason=skip_reason,
        )
        return original_text

    cache_key = _cache_key(base_text)
    cached = _LLM_REWRITE_CACHE.get(cache_key)
    if cached is not None:
        record_conversation_llm_event(
            report_date=report_date,
            outcome="skipped",
            response_type=response_type,
            channel=channel,
            branch=branch,
            report_type=report_type,
            event_report_date=observability["report_date"],
            replay_suppressed=False,
            reason="cache_hit",
        )
        return cached

    llm_context = {
        "response_type": response_type,
        "channel": channel,
        "reason": reason,
        "report_type": report_type,
        "branch": branch,
    }
    record_conversation_llm_event(
        report_date=report_date,
        outcome="called",
        response_type=response_type,
        channel=channel,
        branch=branch,
        report_type=report_type,
        event_report_date=observability["report_date"],
        replay_suppressed=False,
    )
    try:
        refined_text = refine_response_text(original_text, llm_context)
        logger.warning(f"LLM RETURN VALUE: {refined_text!r}")
    except Exception:
        logger.warning("LLM RETURN VALUE: None")
        logger.warning(f"FINAL TEXT BEFORE GUARD: {original_text}")
        logger.warning("VALIDATION GUARD ACTIVE: False")
        logger.warning("OUTCOME BEFORE FINAL SET: adapter_failure")
        record_conversation_llm_event(
            report_date=report_date,
            outcome="failed",
            response_type=response_type,
            channel=channel,
            branch=branch,
            report_type=report_type,
            event_report_date=observability["report_date"],
            replay_suppressed=False,
            reason="adapter_failure",
        )
        record_conversation_llm_event(
            report_date=report_date,
            outcome="fallback",
            response_type=response_type,
            channel=channel,
            branch=branch,
            report_type=report_type,
            event_report_date=observability["report_date"],
            replay_suppressed=False,
            reason="adapter_failure",
        )
        return original_text

    if isinstance(refined_text, str) and refined_text.strip():
        final_text = refined_text.strip().strip('"').strip("'")
        logger.warning(f"FINAL TEXT BEFORE GUARD: {final_text}")
        logger.warning("VALIDATION GUARD ACTIVE: False")
        logger.warning("OUTCOME BEFORE FINAL SET: success")
        record_conversation_llm_event(
            report_date=report_date,
            outcome="success",
            response_type=response_type,
            channel=channel,
            branch=branch,
            report_type=report_type,
            event_report_date=observability["report_date"],
            replay_suppressed=False,
        )
        _LLM_REWRITE_CACHE[cache_key] = final_text
        return final_text

    logger.warning(f"FINAL TEXT BEFORE GUARD: {original_text}")
    logger.warning("VALIDATION GUARD ACTIVE: False")
    logger.warning("OUTCOME BEFORE FINAL SET: fallback")
    record_conversation_llm_event(
        report_date=report_date,
        outcome="fallback",
        response_type=response_type,
        channel=channel,
        branch=branch,
        report_type=report_type,
        event_report_date=observability["report_date"],
        replay_suppressed=False,
    )
    return original_text


def _llm_skip_reason(
    *,
    base_text: str,
    response_context: Mapping[str, Any],
    structured_feedback: Mapping[str, Any] | None = None,
) -> str | None:
    if _string_or_none(response_context.get("response_type")) == "command_reply":
        return "command_reply_skipped"
    if isinstance(structured_feedback, Mapping):
        return "structured_feedback"
    if not _conversation_llm_enabled():
        return "llm_disabled"
    if _conversation_llm_mode() != "rewrite_only":
        return "llm_mode_off"
    if bool(response_context.get("is_replay") is True):
        return "replay"
    if (_string_or_none(response_context.get("channel")) or "whatsapp") != "whatsapp":
        return "channel_not_supported"
    if len(base_text) < 40:
        return "text_too_short"
    if _string_or_none(response_context.get("response_type")) in {"duplicate_ack", "duplicate_notice"}:
        return "duplicate_notice_skipped"
    return None


def _validated_refined_text(
    *,
    base_text: str,
    refined_text: str,
    response_context: Mapping[str, Any],
) -> str | None:
    cleaned = refined_text.strip().strip('"').strip("'")
    if len(cleaned) < 10 or len(cleaned) > 300:
        return None

    if SequenceMatcher(a=base_text, b=cleaned).ratio() < 0.55:
        return None

    lowered = cleaned.casefold()
    for keyword_group in _required_keyword_groups(response_context):
        if not any(keyword in lowered for keyword in keyword_group):
            return None

    if not _reason_preserved(base_text=base_text, refined_text=cleaned, response_context=response_context):
        return None

    if _string_or_none(response_context.get("response_type")) == "unknown_message_guidance":
        for line in _SUPPORTED_REPORT_LINES:
            if line.casefold() not in lowered:
                return None

    return cleaned


def _required_keyword_groups(response_context: Mapping[str, Any]) -> tuple[tuple[str, ...], ...]:
    response_type = _string_or_none(response_context.get("response_type"))
    if response_type == "accepted_ack":
        return (("received",), ("processed", "successfully"))
    if response_type == "correction_accepted_ack":
        return (("corrected",), ("processed", "successfully"))
    if response_type == "correction_review_ack":
        return (("corrected",), ("review",))
    if response_type == "correction_repeat_fix_request":
        return (("processed",), ("resend", "corrections"))
    if response_type == "review_ack":
        return (("received",), ("review",))
    if response_type == "rejected_fix_request":
        return (("processed",), ("resend",))
    if response_type == "duplicate_ack":
        return (("duplicate",), ("received", "record"))
    if response_type == "unknown_message_guidance":
        return (("supported",), ("report",))
    return ()


def _reason_preserved(
    *,
    base_text: str,
    refined_text: str,
    response_context: Mapping[str, Any],
) -> bool:
    if "Reason:" not in base_text:
        return True
    base_reason = base_text.split("Reason:", 1)[1].splitlines()[0].strip().rstrip(".")
    if not base_reason:
        return True
    lowered = refined_text.casefold()
    if base_reason.casefold() in lowered:
        return True
    significant_words = [word for word in base_reason.casefold().split() if len(word) >= 5]
    return bool(significant_words) and all(word in lowered for word in significant_words)


def _conversation_llm_enabled() -> bool:
    import os

    value = os.environ.get("CONVERSATION_LLM_ENABLED", "")
    return value.strip().lower() in {"true", "1", "yes", "on"}


def _conversation_llm_mode() -> str:
    import os

    return os.environ.get("CONVERSATION_LLM_MODE", "").strip().lower()


def _observability_report_date(response_context: Mapping[str, Any]) -> str:
    report_date = _conversation_llm_metadata(response_context)["report_date"]
    if report_date is not None:
        return report_date
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _format_observability_date(value: str | None) -> str | None:
    if value is None:
        return None
    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]
    return None


def _conversation_llm_metadata(response_context: Mapping[str, Any]) -> dict[str, str | None]:
    """Return the best available report metadata for LLM observability events."""

    feedback_context = _mapping(response_context.get("feedback_context"))
    human_tolerance = _mapping(feedback_context.get("human_tolerance"))
    normalized_fields = _mapping(human_tolerance.get("normalized_fields"))
    raw_text = _feedback_raw_text(feedback_context)

    branch = _resolved_branch_from_any(
        response_context.get("branch"),
        feedback_context.get("branch"),
        normalized_fields.get("branch"),
        _branch_from_raw_text(raw_text),
    )
    report_date = _resolved_report_date_from_any(
        response_context.get("report_date"),
        feedback_context.get("report_date"),
        normalized_fields.get("date"),
        _report_date_from_raw_text(raw_text),
    )
    report_type = _resolved_report_type_from_any(
        response_context.get("report_type"),
        response_context.get("signal_subtype"),
        response_context.get("signal_type"),
        feedback_context.get("report_type"),
        feedback_context.get("signal_subtype"),
        feedback_context.get("signal_type"),
        human_tolerance.get("report_type_hint"),
        _report_type_from_raw_text(raw_text),
    )
    return {
        "branch": branch,
        "report_date": report_date,
        "report_type": report_type,
    }


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _feedback_raw_text(feedback_context: Mapping[str, Any]) -> str | None:
    text = _string_or_none(feedback_context.get("raw_text"))
    if text is not None:
        return text
    raw_txt_path = _string_or_none(feedback_context.get("raw_txt_path"))
    if raw_txt_path is None:
        return None
    try:
        loaded = Path(raw_txt_path).read_text(encoding="utf-8")
    except OSError:
        return None
    stripped = loaded.strip()
    return stripped or None


def _branch_from_raw_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    match = _BRANCH_LINE_PATTERN.search(raw_text)
    if match is None:
        return None
    return _string_or_none(match.group(1))


def _report_date_from_raw_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    match = _DATE_LINE_PATTERN.search(raw_text)
    if match is None:
        return None
    return _string_or_none(match.group(1))


def _resolved_branch_from_any(*values: object) -> str | None:
    for value in values:
        text = _string_or_none(value)
        if text is None or text == "unknown":
            continue
        normalized = normalize_branch(text).normalized_value or text.strip()
        if normalized:
            return normalized
    return None


def _resolved_report_date_from_any(*values: object) -> str | None:
    for value in values:
        text = _string_or_none(value)
        if text is None:
            continue
        normalized = normalize_report_date(text).normalized_value or _format_observability_date(text)
        if normalized is not None:
            return normalized
    return None


def _resolved_report_type_from_any(*values: object) -> str | None:
    for value in values:
        text = _string_or_none(value)
        if text is None:
            continue
        normalized = text.strip().casefold().replace("-", "_").replace(" ", "_")
        if normalized in {"report", "reports", "routing", "unknown"}:
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
        if normalized == "store_monitoring":
            return "store_monitoring"
        return normalized
    return None


def _report_type_from_raw_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    normalized = raw_text.casefold()
    if "supervisor control report" in normalized or "supervisor control summary" in normalized:
        return "supervisor_control"
    if "daily bale summary" in normalized or "pricing stock release" in normalized:
        return "bale_summary"
    if "staff performance report" in normalized:
        return "staff_performance"
    if "attendance report" in normalized or "staff attendance" in normalized or "staffs attendance" in normalized:
        return "staff_attendance"
    if "day-end sales report" in normalized or "day end sales report" in normalized:
        return "sales_income"
    return None


def _cache_key(base_text: str) -> str:
    return hashlib.sha256(base_text.encode("utf-8")).hexdigest()


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _required_text(value: object, *, field_name: str) -> str:
    cleaned = _string_or_none(value)
    if cleaned is None:
        raise ValueError(f"Response context field `{field_name}` must be a non-empty string.")
    return cleaned


_REPORT_LABELS: dict[str, str] = {
    "sales": "DAY-END SALES REPORT",
    "sales_income": "DAY-END SALES REPORT",
    "staff_attendance": "ATTENDANCE REPORT",
    "hr_attendance": "ATTENDANCE REPORT",
    "staff_performance": "STAFF PERFORMANCE REPORT",
    "hr_performance": "STAFF PERFORMANCE REPORT",
    "bale_summary": "DAILY BALE SUMMARY",
    "pricing_stock_release": "DAILY BALE SUMMARY",
    "supervisor_control": "SUPERVISOR CONTROL REPORT",
}
