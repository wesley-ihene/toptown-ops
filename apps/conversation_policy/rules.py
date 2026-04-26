"""Central response policy for Phase C1 WhatsApp outcomes."""

from __future__ import annotations

from typing import Final

ALLOWED_RESPONSE_TYPES: Final[set[str]] = {
    "operational_query_status",
    "accepted_ack",
    "correction_accepted_ack",
    "correction_review_ack",
    "correction_repeat_fix_request",
    "review_ack",
    "rejected_fix_request",
    "duplicate_notice",
    "unknown_message_guidance",
}

_RESPONSE_REASON_ALLOWLIST: Final[dict[str, set[str]]] = {
    "operational_query_status": set(),
    "accepted_ack": set(),
    "correction_accepted_ack": set(),
    "correction_review_ack": {
        "confidence_missing",
        "confidence_between_review_and_accept_thresholds",
        "strict_candidate_below_reject_threshold",
        "route_requires_review",
        "unknown_report_family",
        "mixed_report_split_not_safe",
        "mixed_child_requires_review",
        "missing_branch",
        "missing_date",
        "supervisor_control_invalid_format",
    },
    "correction_repeat_fix_request": {
        "validation_failed",
        "confidence_below_reject_threshold",
        "fallback_validation_failed",
        "mixed_report",
        "mixed_report_rejected",
        "mixed_reports_rejected_upstream",
        "invalid_input",
    },
    "review_ack": {
        "confidence_missing",
        "confidence_between_review_and_accept_thresholds",
        "strict_candidate_below_reject_threshold",
        "route_requires_review",
        "unknown_report_family",
        "mixed_report_split_not_safe",
        "mixed_child_requires_review",
        "missing_branch",
        "missing_date",
        "supervisor_control_invalid_format",
    },
    "rejected_fix_request": {
        "validation_failed",
        "confidence_below_reject_threshold",
        "fallback_validation_failed",
        "mixed_report",
        "mixed_report_rejected",
        "mixed_reports_rejected_upstream",
        "invalid_input",
    },
    "duplicate_notice": {
        "duplicate_message",
        "duplicate_message_id",
        "duplicate_raw_sha256",
        "duplicate_semantic",
    },
    "unknown_message_guidance": {
        "unknown_report_type",
        "unsupported_payload_kind",
        "empty_input",
        "missing_raw_text",
    },
}

_REASON_TEXT: Final[dict[str, str]] = {
    "confidence_missing": "confidence details were incomplete",
    "confidence_between_review_and_accept_thresholds": "confidence needs operator review",
    "strict_candidate_below_reject_threshold": "confidence needs operator review",
    "route_requires_review": "routing needs operator review",
    "unknown_report_family": "the message could not be routed safely",
    "mixed_report_split_not_safe": "send one report per message",
    "mixed_child_requires_review": "one split item still needs review",
    "missing_branch": "branch is missing from the report",
    "missing_date": "report date is missing from the report",
    "supervisor_control_invalid_format": "supervisor control format needs correction",
    "validation_failed": "required report fields were missing or invalid",
    "confidence_below_reject_threshold": "confidence was below the acceptance threshold",
    "fallback_validation_failed": "required report fields were missing or invalid",
    "mixed_report": "send one report per message",
    "mixed_report_rejected": "send one report per message",
    "mixed_reports_rejected_upstream": "send one report per message",
    "invalid_input": "required report fields were missing or invalid",
    "duplicate_message": "this message matches a report already received",
    "duplicate_message_id": "this message matches a report already received",
    "duplicate_raw_sha256": "this message matches a report already received",
    "duplicate_semantic": "a matching report for this branch and date is already on file",
    "unknown_report_type": "the message format was not recognized as a supported report",
    "unsupported_payload_kind": "send the report as a text message",
    "empty_input": "no report text was detected",
    "missing_raw_text": "no report text was detected",
}

_FALLBACK_REASON_TEXT: Final[dict[str, str]] = {
    "operational_query_status": "deterministic operational status reply generated",
    "correction_review_ack": "operator review is required before final processing",
    "correction_repeat_fix_request": "the report needs correction before it can be processed",
    "review_ack": "operator review is required before final processing",
    "rejected_fix_request": "the report needs correction before it can be processed",
    "duplicate_notice": "this report was already received",
    "unknown_message_guidance": "send one supported report in text format only",
}


def normalize_response_reason(response_type: str, reason: str | None) -> str | None:
    """Return one allowed reason code for the given response type."""

    if response_type not in ALLOWED_RESPONSE_TYPES:
        raise ValueError(f"Unsupported response type `{response_type}`.")
    if not isinstance(reason, str):
        return None
    cleaned = reason.strip()
    if not cleaned:
        return None
    if cleaned not in _RESPONSE_REASON_ALLOWLIST[response_type]:
        return None
    return cleaned


def reason_text_for_response(response_type: str, reason: str | None) -> str | None:
    """Return surfaced reason text only when policy allows it."""

    normalized = normalize_response_reason(response_type, reason)
    if normalized is not None:
        return _REASON_TEXT.get(normalized)
    return _FALLBACK_REASON_TEXT.get(response_type)
