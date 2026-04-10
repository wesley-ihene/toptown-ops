"""Shared report-date normalization helpers."""

from __future__ import annotations

from datetime import datetime
import re

from .types import AppliedRule, NormalizedValue

_WEEKDAY_NAMES = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)
_EMBEDDED_DATE_PATTERN = re.compile(
    rf"(?P<full>(?:(?:date)\s*[:=-]\s*)?"
    rf"(?:(?:{'|'.join(_WEEKDAY_NAMES)})\s*,?\s*)?"
    r"(?P<date>\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}|\d{4}-\d{2}-\d{2}))",
    flags=re.IGNORECASE,
)
_LEADING_WEEKDAY_PATTERN = re.compile(
    rf"^(?:{'|'.join(_WEEKDAY_NAMES)})\s*,?\s*",
    flags=re.IGNORECASE,
)
_DATE_LABEL_PREFIX_PATTERN = re.compile(r"^date\s*[:=-]\s*", flags=re.IGNORECASE)


def normalize_report_date(raw_value: str) -> NormalizedValue:
    """Return an ISO date only when the input is recoverable."""

    raw = raw_value
    cleaned = raw_value.strip()
    result = NormalizedValue(raw_value=raw, metadata={"value_type": "date"})
    if not cleaned:
        result.hard_errors.append("empty_date_value")
        return result

    candidate = cleaned
    match = _EMBEDDED_DATE_PATTERN.search(cleaned)
    if match is not None:
        candidate = match.group("full").strip()
        if candidate != cleaned:
            result.applied_rules.append(
                AppliedRule(
                    name="embedded_date_substring",
                    raw_value=cleaned,
                    normalized_value=candidate,
                )
            )

    label_stripped = _DATE_LABEL_PREFIX_PATTERN.sub("", candidate).strip()
    if label_stripped != candidate:
        result.applied_rules.append(
            AppliedRule(
                name="date_label_prefix_removed",
                raw_value=candidate,
                normalized_value=label_stripped,
            )
        )
        candidate = label_stripped

    weekday_stripped = _LEADING_WEEKDAY_PATTERN.sub("", candidate).strip()
    if weekday_stripped != candidate:
        result.applied_rules.append(
            AppliedRule(
                name="weekday_prefix_removed",
                raw_value=candidate,
                normalized_value=weekday_stripped,
            )
        )
        candidate = weekday_stripped

    collapsed = re.sub(r"\s*([/-])\s*", r"\1", candidate)
    if collapsed != candidate:
        result.applied_rules.append(
            AppliedRule(
                name="date_separator_spacing_collapsed",
                raw_value=candidate,
                normalized_value=collapsed,
            )
        )
        candidate = collapsed

    for pattern in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y", "%d-%m-%y", "%d-%m-%Y"):
        try:
            iso_date = datetime.strptime(candidate, pattern).date().isoformat()
        except ValueError:
            continue
        result.normalized_value = iso_date
        result.confidence = 1.0
        result.metadata["pattern"] = pattern
        if candidate != iso_date:
            result.applied_rules.append(
                AppliedRule(
                    name="date_canonicalized_to_iso",
                    raw_value=candidate,
                    normalized_value=iso_date,
                    details={"pattern": pattern},
                )
            )
        return result

    result.hard_errors.append("unrecoverable_date")
    return result
