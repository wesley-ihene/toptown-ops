"""Resolve deterministic continuity flags for multi-message correction flows."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_REJECTION_RESPONSE_TYPES = {"rejected_fix_request", "correction_repeat_fix_request"}
_SUCCESS_RESPONSE_TYPES = {"accepted_ack", "review_ack"}
_CORRECTION_ELIGIBLE_RESPONSE_TYPES = _REJECTION_RESPONSE_TYPES | _SUCCESS_RESPONSE_TYPES
_REPORT_TYPE_ALIASES = {
    "sales": "sales",
    "sales_income": "sales",
    "staff_attendance": "staff_attendance",
    "hr_attendance": "staff_attendance",
    "staff_performance": "staff_performance",
    "hr_performance": "staff_performance",
    "bale_summary": "bale_summary",
    "supervisor_control": "supervisor_control",
    "unknown": "unknown",
}


def resolve_context_flags(
    *,
    current_response_context: Mapping[str, Any],
    stored_context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return conservative continuity flags for one current response."""

    current_response_type = _text_or_none(current_response_context.get("response_type"))
    previous = _interaction(stored_context)
    previous_response_type = _text_or_none(previous.get("response_type"))
    if bool(current_response_context.get("is_replay") is True):
        return _flags(previous_response_type=previous_response_type)
    if previous_response_type not in _REJECTION_RESPONSE_TYPES:
        return _flags(previous_response_type=previous_response_type)
    if current_response_type not in _CORRECTION_ELIGIBLE_RESPONSE_TYPES:
        return _flags(previous_response_type=previous_response_type)
    if not _matches_previous_scope(previous, current_response_context):
        return _flags(previous_response_type=previous_response_type)

    correction_attempt = True
    repeat_failure = current_response_type in _REJECTION_RESPONSE_TYPES
    correction_success = current_response_type in _SUCCESS_RESPONSE_TYPES
    return _flags(
        previous_response_type=previous_response_type,
        correction_attempt=correction_attempt,
        repeat_failure=repeat_failure,
        correction_success=correction_success,
    )


def _matches_previous_scope(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
) -> bool:
    previous_family = _report_family(previous.get("report_type"))
    current_family = _report_family(current.get("report_type"))
    if previous_family is not None and current_family is not None and previous_family != current_family:
        return False

    previous_branch = _text_or_none(previous.get("branch"))
    current_branch = _text_or_none(current.get("branch"))
    if previous_branch is not None and current_branch is not None and previous_branch != current_branch:
        return False

    previous_date = _text_or_none(previous.get("report_date"))
    current_date = _text_or_none(current.get("report_date"))
    if previous_date is not None and current_date is not None and previous_date != current_date:
        return False

    if (
        previous_family is not None
        and current_family is not None
        and previous_branch is not None
        and current_branch is not None
        and previous_date is not None
        and current_date is not None
    ):
        return True
    if previous_family is not None and current_family is not None:
        return True
    return False


def _interaction(stored_context: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(stored_context, Mapping):
        return {}
    interaction = stored_context.get("last_interaction")
    if isinstance(interaction, Mapping):
        return interaction
    return {}


def _report_family(value: object) -> str | None:
    report_type = _text_or_none(value)
    if report_type is None:
        return None
    return _REPORT_TYPE_ALIASES.get(report_type, report_type)


def _flags(
    *,
    previous_response_type: str | None,
    correction_attempt: bool = False,
    repeat_failure: bool = False,
    correction_success: bool = False,
) -> dict[str, Any]:
    return {
        "previous_response_type": previous_response_type,
        "correction_attempt": correction_attempt,
        "repeat_failure": repeat_failure,
        "correction_success": correction_success,
    }


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
