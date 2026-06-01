"""Normalization helpers for HR parsing."""

from __future__ import annotations

import re

from packages.common.normalizer import parse_count
from packages.normalization.labels import normalize_label

_CHECKMARK_PATTERN = re.compile(r"[✓✔✅☑]")
_NOISE_PATTERN = re.compile(r"\s*/=\s*")
_WRAPPER_PATTERN = re.compile(r"^[*=]+|[*=]+$")
_KEY_TEXT_PATTERN = re.compile(r"[^a-z0-9]+")
_HALF_DAY_PRESENT_ALIASES = {
    "p 1 2 day",
    "p half day",
    "present half day",
}
STATUS_ALIASES: dict[str, tuple[str, ...]] = {
    "present": ("p", "present", "press", "pres", "press/", "check"),
    "present_half": ("p half", "p half day", "present half day"),
    "off": ("off", "off duty", "day off"),
    "leave": ("anual leave", "annual leave", "leave", "on leave", "leave break", "leavebreak"),
    "suspend": ("suspend", "suspended"),
    "absent": ("absent", "x", "no show"),
    "sick": ("sick",),
    "awn": ("awn", "absent with notice"),
    "awon": ("awon", "absent without notice", "absent without"),
    "lay_off": ("lay off", "layoff"),
    "non_active": (
        "resign",
        "resigned",
        "resignation pending",
        "resign pending",
        "decision pending",
        "decission pending",
        "terminated",
        "termination pending",
    ),
    "transfer": ("transfer",),
    "late": ("late",),
    "nil": ("nil", "nill"),
}
STATUS_BUCKETS: dict[str, str] = {
    "present": "present",
    "present_half": "present_half",
    "off": "off",
    "leave": "leave",
    "absent": "absent",
    "sick": "absent",
    "suspend": "absent",
    "awn": "absent",
    "awon": "absent",
    "late": "absent",
    "lay_off": "off",
    "non_active": "non_active",
    "transfer": "off",
    "nil": "off",
}
_CANONICAL_STATUS_LABELS: dict[str, str] = {
    "present": "PRESENT",
    "present_half": "P_HALF",
    "off": "OFF",
    "leave": "LEAVE",
    "suspend": "SUSPEND",
    "absent": "ABSENT",
    "sick": "SICK",
    "awn": "AWN",
    "awon": "AWON",
    "lay_off": "LAY_OFF",
    "non_active": "NON_ACTIVE",
    "transfer": "TRANSFER",
    "late": "LATE",
    "nil": "NIL",
}


def normalize_text(value: str) -> str:
    """Normalize free-form text for case-insensitive comparisons."""

    return " ".join(strip_formatting_noise(value).casefold().replace("_", " ").split())


def normalize_key_text(value: str) -> str:
    """Return one comparison-friendly key with formatting noise removed."""

    return " ".join(_KEY_TEXT_PATTERN.sub(" ", normalize_text(value)).split())


def strip_formatting_noise(value: str) -> str:
    """Remove light WhatsApp formatting noise without changing meaning."""

    cleaned = value.replace("\u200b", " ").replace("\u2060", " ").replace("\ufeff", " ")
    cleaned = cleaned.replace("✔️", "✔").replace("✓️", "✓")
    cleaned = _NOISE_PATTERN.sub(" ", cleaned)
    cleaned = _WRAPPER_PATTERN.sub("", cleaned.strip())
    return " ".join(cleaned.split())


def clean_name(raw_value: str) -> str | None:
    """Return a cleaned person-name candidate or `None`."""

    cleaned = re.sub(r"\s+", " ", strip_formatting_noise(raw_value)).strip(" -:/|")
    return cleaned or None


def normalize_status(raw_value: str) -> tuple[str | None, str | None]:
    """Return canonical and raw attendance status values when recognized."""

    cleaned = strip_formatting_noise(raw_value)
    if normalize_key_text(cleaned) in _HALF_DAY_PRESENT_ALIASES:
        return "present_half", _CANONICAL_STATUS_LABELS["present_half"]
    if _CHECKMARK_PATTERN.search(cleaned):
        return "present", _CANONICAL_STATUS_LABELS["present"]

    label_result = normalize_label(cleaned, report_family="attendance")
    if label_result.succeeded and label_result.normalized_value is not None:
        canonical = {
            "P": "present",
            "P_HALF": "present_half",
            "OFF": "off",
            "LEAVE": "leave",
            "ABSENT": "absent",
            "SICK": "sick",
            "SUSPENDED": "suspend",
        }.get(label_result.normalized_value)
        if canonical is not None:
            return canonical, _CANONICAL_STATUS_LABELS.get(canonical, label_result.normalized_value)

    normalized = normalize_text(cleaned)
    matched_aliases: list[tuple[int, str, str]] = []
    for canonical, aliases in STATUS_ALIASES.items():
        for alias in aliases:
            alias_normalized = normalize_text(alias)
            if re.search(rf"\b{re.escape(alias_normalized)}\b", normalized):
                matched_aliases.append((len(alias_normalized), canonical, alias))
    if not matched_aliases:
        return None, None

    _, canonical, alias = max(matched_aliases, key=lambda item: item[0])
    return canonical, _CANONICAL_STATUS_LABELS.get(canonical, alias.upper())
