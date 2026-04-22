"""Deterministic pre-ingestion validation for live inbound text."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

_VALIDATOR_VERSION = "v1"
_ZERO_WIDTH_AND_CONTROL_CODE = "removed_control_characters"
_TRIMMED_CODE = "trimmed_whitespace"
_NORMALIZED_LINES_CODE = "normalized_line_endings"
_COLLAPSED_BLANKS_CODE = "collapsed_blank_lines"

_STRONG_HEADER_FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("sales", ("day-end sales report", "day end sales report")),
    ("bale_summary", ("bale summary report", "bale summary", "pricing stock release report", "pricing stock release")),
    ("staff_attendance", ("staff attendance report",)),
    ("staff_performance", ("staff performance report",)),
    ("supervisor_control", ("supervisor control report",)),
)


def validate_inbound_text(
    text: str,
    *,
    payload_kind: str = "text",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate inbound text shape conservatively before orchestration."""

    del metadata

    original_text = text if isinstance(text, str) else ""
    normalized_payload_kind = payload_kind.strip() if isinstance(payload_kind, str) else ""
    reasons: list[dict[str, str]] = []
    warnings: list[str] = []
    detected_risks: list[str] = []

    cleaned_text = original_text
    cleaned_text, changed_line_endings = _normalize_line_endings(cleaned_text)
    if changed_line_endings:
        reasons.append(_reason(_NORMALIZED_LINES_CODE, "Normalized CRLF/CR line endings to LF."))

    cleaned_text, removed_controls = _remove_control_characters(cleaned_text)
    if removed_controls:
        reasons.append(_reason(_ZERO_WIDTH_AND_CONTROL_CODE, "Removed zero-width and control characters."))

    trimmed_text = cleaned_text.strip()
    if trimmed_text != cleaned_text:
        cleaned_text = trimmed_text
        reasons.append(_reason(_TRIMMED_CODE, "Trimmed leading and trailing whitespace."))
    else:
        cleaned_text = trimmed_text

    cleaned_text, collapsed_blank_lines = _collapse_blank_lines(cleaned_text)
    if collapsed_blank_lines:
        reasons.append(_reason(_COLLAPSED_BLANKS_CODE, "Collapsed repeated blank lines."))

    if normalized_payload_kind != "text":
        return _result(
            status="rejected",
            cleaned_text=cleaned_text,
            reasons=reasons + [_reason("unsupported_payload_kind", "Only text payloads are supported for pre-ingestion validation.")],
            warnings=warnings,
            detected_risks=detected_risks,
            suggested_report_family=None,
        )

    if not cleaned_text.strip():
        return _result(
            status="rejected",
            cleaned_text=cleaned_text,
            reasons=reasons + [_reason("empty_input", "Inbound text is empty after safe cleanup.")],
            warnings=warnings,
            detected_risks=detected_risks,
            suggested_report_family=None,
        )

    matched_families = _detect_strong_headers(cleaned_text)
    if len(matched_families) >= 2:
        detected_risks.append("mixed_report_risk")
        warnings.append("Multiple strong report headers were detected in one message.")

    status = "cleaned" if cleaned_text != original_text else "accepted"
    return _result(
        status=status,
        cleaned_text=cleaned_text,
        reasons=reasons,
        warnings=warnings,
        detected_risks=detected_risks,
        suggested_report_family=matched_families[0] if len(matched_families) == 1 else None,
    )


def _normalize_line_endings(text: str) -> tuple[str, bool]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return normalized, normalized != text


def _remove_control_characters(text: str) -> tuple[str, bool]:
    changed = False
    characters: list[str] = []
    for character in text:
        if character in {"\n", "\t"}:
            characters.append(character)
            continue
        if unicodedata.category(character).startswith("C"):
            changed = True
            continue
        characters.append(character)
    return "".join(characters), changed


def _collapse_blank_lines(text: str) -> tuple[str, bool]:
    if not text:
        return text, False

    lines = text.split("\n")
    collapsed: list[str] = []
    previous_blank = False
    changed = False
    for line in lines:
        is_blank = not line.strip()
        normalized_line = "" if is_blank else line
        if is_blank and previous_blank:
            changed = True
            continue
        if line != normalized_line:
            changed = True
        collapsed.append(normalized_line)
        previous_blank = is_blank
    result = "\n".join(collapsed)
    return result, changed or result != text


def _detect_strong_headers(text: str) -> list[str]:
    normalized_lines = [_normalized_header_line(line) for line in text.split("\n")]
    matched: list[str] = []
    for family, patterns in _STRONG_HEADER_FAMILIES:
        if any(line in patterns for line in normalized_lines):
            matched.append(family)
    return matched


def _normalized_header_line(line: str) -> str:
    normalized = unicodedata.normalize("NFKC", line).casefold()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return normalized.strip()


def _reason(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _result(
    *,
    status: str,
    cleaned_text: str,
    reasons: list[dict[str, str]],
    warnings: list[str],
    detected_risks: list[str],
    suggested_report_family: str | None,
) -> dict[str, Any]:
    return {
        "status": status,
        "cleaned_text": cleaned_text,
        "reasons": reasons,
        "warnings": warnings,
        "detected_risks": detected_risks,
        "suggested_report_family": suggested_report_family,
        "validator_version": _VALIDATOR_VERSION,
    }
