"""Controlled natural-language intent mapping for existing WhatsApp commands."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import copy
import hashlib
import re
from typing import Any

from apps.command_router.worker import route_whatsapp_command
from packages.llm_adapter.openai_client import classify_user_intent_json
from packages.observability import record_nl_intent_event

_ALLOWED_INTENTS = {
    "help",
    "status",
    "format_sales",
    "format_attendance",
    "why_rejected",
}
_CONFIDENCE_THRESHOLD = 0.75
_MIN_MESSAGE_LENGTH = 5
_CLASSIFICATION_CACHE: dict[str, dict[str, Any]] = {}
_TOK_PISIN_REPLACEMENTS = (
    ("why em reject", "why rejected"),
    ("format bilong attendance", "format attendance"),
)
_WHITESPACE_RE = re.compile(r"\s+")
_FORMAT_SIGNAL_WORDS = {"format", "show", "template"}


def normalize_text(message_text: str) -> str:
    """Return one normalized message string before any LLM use."""

    if not isinstance(message_text, str):
        return ""
    normalized = _WHITESPACE_RE.sub(" ", message_text.casefold().strip())
    for source, target in _TOK_PISIN_REPLACEMENTS:
        normalized = normalized.replace(source, target)
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def classify_intent(message_text: str, is_replay: bool) -> dict[str, Any]:
    """Return one strict validity result for NL intent classification."""

    details = _evaluate_intent(message_text, is_replay=is_replay)
    return {
        "intent": details["intent"],
        "confidence": details["confidence"],
        "valid": details["valid"],
    }


def route_natural_language_command(
    message_text: str | None,
    *,
    source_message_id: str | None = None,
    sender_phone: str | None = None,
    channel: str = "whatsapp",
    is_replay: bool = False,
    received_at: str | None = None,
    output_root: str | None = None,
) -> dict[str, Any] | None:
    """Return one deterministic command payload when NL intent is safely mapped."""

    details = _evaluate_intent(message_text, is_replay=is_replay)
    report_date = _report_date(received_at)
    record_nl_intent_event(
        report_date=report_date,
        outcome=details["outcome"],
        message_id=_text_or_none(source_message_id),
        sender_phone=_text_or_none(sender_phone),
        normalized_message=details["normalized_message"],
        intent=details["intent"],
        confidence=details["confidence"],
        reason=details["reason"],
        output_root=output_root,
    )

    if details["valid"] is not True:
        return None

    command_text = _map_intent_to_command_text(details["intent"])
    if command_text is None:
        return None

    command = route_whatsapp_command(
        command_text,
        source_message_id=source_message_id,
        sender_phone=sender_phone,
        channel=channel,
        is_replay=is_replay,
    )
    if command.get("is_command") is not True:
        record_nl_intent_event(
            report_date=report_date,
            outcome="mismatch",
            message_id=_text_or_none(source_message_id),
            sender_phone=_text_or_none(sender_phone),
            normalized_message=details["normalized_message"],
            intent=details["intent"],
            confidence=details["confidence"],
            reason="mapped_command_unrecognized",
            output_root=output_root,
        )
        return None

    command["command_text"] = command_text
    command["command_source"] = "nl_intent"
    command["nl_intent"] = {
        "intent": details["intent"],
        "confidence": details["confidence"],
        "normalized_message": details["normalized_message"],
    }
    return command


def _evaluate_intent(message_text: str | None, *, is_replay: bool) -> dict[str, Any]:
    normalized_message = normalize_text(message_text or "")
    if is_replay:
        return _result(
            normalized_message=normalized_message,
            outcome="skipped",
            reason="replay",
        )
    if len(normalized_message) < _MIN_MESSAGE_LENGTH:
        return _result(
            normalized_message=normalized_message,
            outcome="rejected",
            reason="message_too_short",
        )

    hint_matches = _hint_matches(normalized_message)
    if len(hint_matches) > 1:
        return _result(
            normalized_message=normalized_message,
            outcome="mismatch",
            reason="ambiguous_intent_patterns",
        )

    cached = _cached_result(normalized_message)
    if cached is not None:
        cached["normalized_message"] = normalized_message
        return cached

    raw_response = classify_user_intent_json(normalized_message)
    if raw_response is None:
        result = _result(
            normalized_message=normalized_message,
            outcome="rejected",
            reason="llm_unavailable",
        )
        _store_cache(normalized_message, result)
        return result

    parsed = _parse_classification(raw_response)
    if parsed is None:
        result = _result(
            normalized_message=normalized_message,
            outcome="rejected",
            reason="invalid_json",
        )
        _store_cache(normalized_message, result)
        return result

    intent = _text_or_none(parsed.get("intent"))
    confidence = _confidence(parsed.get("confidence"))
    if intent not in _ALLOWED_INTENTS:
        result = _result(
            normalized_message=normalized_message,
            outcome="rejected",
            reason="unsupported_intent",
            intent=intent,
            confidence=confidence,
        )
        _store_cache(normalized_message, result)
        return result
    if confidence < _CONFIDENCE_THRESHOLD:
        result = _result(
            normalized_message=normalized_message,
            outcome="low_confidence",
            reason="confidence_below_threshold",
            intent=intent,
            confidence=confidence,
        )
        _store_cache(normalized_message, result)
        return result
    if len(hint_matches) != 1:
        result = _result(
            normalized_message=normalized_message,
            outcome="mismatch",
            reason="no_intent_pattern_match",
            intent=intent,
            confidence=confidence,
        )
        _store_cache(normalized_message, result)
        return result

    hinted_intent = next(iter(hint_matches))
    if hinted_intent != intent:
        result = _result(
            normalized_message=normalized_message,
            outcome="mismatch",
            reason="hint_intent_mismatch",
            intent=intent,
            confidence=confidence,
        )
        _store_cache(normalized_message, result)
        return result

    result = _result(
        normalized_message=normalized_message,
        outcome="success",
        reason="intent_confirmed",
        intent=intent,
        confidence=confidence,
        valid=True,
    )
    _store_cache(normalized_message, result)
    return result


def _hint_matches(normalized_message: str) -> set[str]:
    matches: set[str] = set()
    words = set(normalized_message.split())
    if "help" in words:
        matches.add("help")
    if _looks_like_status(normalized_message):
        matches.add("status")
    if _looks_like_why_rejected(normalized_message):
        matches.add("why_rejected")
    if _looks_like_format(normalized_message, report_word="sales"):
        matches.add("format_sales")
    if _looks_like_format(normalized_message, report_word="attendance"):
        matches.add("format_attendance")
    return matches


def _looks_like_status(normalized_message: str) -> bool:
    if normalized_message == "status":
        return True
    if "report" in normalized_message and "go through" in normalized_message:
        return True
    return "report status" in normalized_message


def _looks_like_why_rejected(normalized_message: str) -> bool:
    if normalized_message == "why rejected":
        return True
    if "wrong" in normalized_message and "report" in normalized_message:
        return True
    return "reject reason" in normalized_message


def _looks_like_format(normalized_message: str, *, report_word: str) -> bool:
    words = set(normalized_message.split())
    return report_word in words and bool(words & _FORMAT_SIGNAL_WORDS)


def _map_intent_to_command_text(intent: str | None) -> str | None:
    return {
        "help": "help",
        "status": "status",
        "format_sales": "format sales",
        "format_attendance": "format attendance",
        "why_rejected": "why rejected",
    }.get(intent)


def _parse_classification(raw_response: str) -> dict[str, Any] | None:
    import json

    try:
        payload = json.loads(raw_response)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _cached_result(normalized_message: str) -> dict[str, Any] | None:
    payload = _CLASSIFICATION_CACHE.get(_cache_key(normalized_message))
    if payload is None:
        return None
    return copy.deepcopy(payload)


def _store_cache(normalized_message: str, payload: Mapping[str, Any]) -> None:
    _CLASSIFICATION_CACHE[_cache_key(normalized_message)] = dict(payload)


def _cache_key(normalized_message: str) -> str:
    return hashlib.sha256(normalized_message.encode("utf-8")).hexdigest()


def _result(
    *,
    normalized_message: str,
    outcome: str,
    reason: str,
    intent: str | None = None,
    confidence: float = 0.0,
    valid: bool = False,
) -> dict[str, Any]:
    return {
        "intent": intent,
        "confidence": confidence,
        "valid": valid,
        "normalized_message": normalized_message,
        "outcome": outcome,
        "reason": reason,
    }


def _confidence(value: object) -> float:
    if isinstance(value, (int, float)):
        bounded = float(value)
    elif isinstance(value, str):
        try:
            bounded = float(value.strip())
        except ValueError:
            return 0.0
    else:
        return 0.0
    if bounded < 0.0:
        return 0.0
    if bounded > 1.0:
        return 1.0
    return bounded


def _report_date(value: str | None) -> str:
    cleaned = _text_or_none(value)
    if cleaned is not None and len(cleaned) >= 10 and cleaned[4] == "-" and cleaned[7] == "-":
        return cleaned[:10]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
