"""Normalization helpers for HR parsing."""

from __future__ import annotations

import re

from packages.common.normalizer import parse_count
from packages.normalization.labels import normalize_label

STATUS_ALIASES: dict[str, tuple[str, ...]] = {
    "present": ("p", "present"),
    "off": ("off", "off duty"),
    "annual_leave": ("anual leave", "annual leave", "leave"),
    "suspended": ("suspend", "suspended"),
    "absent": ("absent",),
    "sick": ("sick",),
}


def normalize_text(value: str) -> str:
    """Normalize free-form text for case-insensitive comparisons."""

    return " ".join(value.casefold().replace("_", " ").split())


def clean_name(raw_value: str) -> str | None:
    """Return a cleaned person-name candidate or `None`."""

    cleaned = re.sub(r"\s+", " ", raw_value).strip(" -:/|")
    return cleaned or None


def normalize_status(raw_value: str) -> tuple[str | None, str | None]:
    """Return canonical and raw attendance status values when recognized."""

    label_result = normalize_label(raw_value, report_family="attendance")
    if label_result.succeeded and label_result.normalized_value is not None:
        canonical = {
            "P": "present",
            "OFF": "off",
            "LEAVE": "leave",
            "ABSENT": "absent",
            "SICK": "absent",
            "SUSPENDED": "absent",
        }.get(label_result.normalized_value)
        if canonical is not None:
            return canonical, label_result.normalized_value

    normalized = normalize_text(raw_value)
    matched_aliases: list[tuple[int, str, str]] = []
    for canonical, aliases in STATUS_ALIASES.items():
        for alias in aliases:
            alias_normalized = normalize_text(alias)
            if re.search(rf"\b{re.escape(alias_normalized)}\b", normalized):
                matched_aliases.append((len(alias_normalized), canonical, alias))
    if not matched_aliases:
        return None, None

    _, canonical, alias = max(matched_aliases, key=lambda item: item[0])
    return canonical, alias
