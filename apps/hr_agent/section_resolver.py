"""Section normalization helpers for HR records."""

from __future__ import annotations

from apps.hr_agent.field_mapper import canonical_section_name


def resolve_section(raw_value: str | None) -> tuple[str | None, str | None]:
    """Return canonical and preserved raw section values."""

    if raw_value is None:
        return None, None

    cleaned = raw_value.strip()
    if not cleaned:
        return None, None

    return canonical_section_name(cleaned), cleaned
