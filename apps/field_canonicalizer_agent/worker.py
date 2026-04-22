"""Canonicalize free-form staff performance field lines."""

from __future__ import annotations

from dataclasses import dataclass
import re

_ITEM_KEY_PATTERN = re.compile(
    r"^\s*(?:[^\w]*)?(?:total\s+)?(?:items?\s+sold(?:\s+assisting)?|item\s+sold(?:\s+assist(?:ing)?)?|items?\s+moved|items?\s+assist(?:ing)?\s+section|items?|item)\s*[:=.\->]?\s*(.*)$",
    flags=re.IGNORECASE,
)
_ASSIST_KEY_PATTERN = re.compile(
    r"^\s*(?:[^\w]*)?(?:customers?\s+assist(?:ing)?|customer\s+assists?|item\s+assist(?:ing)?|asss?ists?|asss?ist)\s*[:=.\->]?\s*(.*)$",
    flags=re.IGNORECASE,
)
_SECTION_KEY_PATTERN = re.compile(
    r"^\s*(?:[^\w]*)?section(?:\s*\(([^)]*)\))?\s*(?:[.,:=>\-]+)?\s*(.*)$",
    flags=re.IGNORECASE,
)


@dataclass(slots=True)
class CanonicalField:
    """One canonicalized key/value line."""

    key: str
    raw_value: str | None
    normalized_value: str | None
    annotation: str | None = None


def canonicalize_null_token(value: str | None) -> str | None:
    """Normalize blank and dash-like placeholders to null."""

    if value is None:
        return None
    cleaned = value.strip()
    cleaned = cleaned.strip("()").strip()
    if cleaned in {"", "-", "—", "–"}:
        return None
    return cleaned


def canonicalize_field_line(line: str) -> CanonicalField | None:
    """Return a canonical field interpretation when one is recognized."""

    assist_match = _ASSIST_KEY_PATTERN.match(line)
    if assist_match is not None:
        normalized_value = canonicalize_null_token(assist_match.group(1))
        return CanonicalField(
            key="assist_count",
            raw_value=assist_match.group(1).strip() or None,
            normalized_value=normalized_value,
        )

    section_match = _SECTION_KEY_PATTERN.match(line)
    if section_match is not None:
        raw_value = section_match.group(2).strip() or None
        normalized_value = canonicalize_null_token(raw_value)
        return CanonicalField(
            key="section",
            raw_value=raw_value,
            normalized_value=normalized_value,
            annotation=canonicalize_null_token(section_match.group(1)),
        )

    item_match = _ITEM_KEY_PATTERN.match(line)
    if item_match is not None:
        normalized_value = canonicalize_null_token(item_match.group(1))
        return CanonicalField(
            key="items_moved",
            raw_value=item_match.group(1).strip() or None,
            normalized_value=normalized_value,
        )

    return None
