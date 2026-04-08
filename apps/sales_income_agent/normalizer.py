"""Normalization helpers for sales report numeric fields."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re


def parse_money(raw_value: str) -> float | None:
    """Parse a money value from free-form WhatsApp text."""

    cleaned = _numeric_text(raw_value)
    if not cleaned:
        return None
    try:
        return float(Decimal(cleaned))
    except InvalidOperation:
        return None


def parse_count(raw_value: str) -> int | None:
    """Parse an integer count from free-form text."""

    amount = parse_money(raw_value)
    return int(amount) if amount is not None else None


def parse_percent(raw_value: str) -> float | None:
    """Parse a percent value, normalizing `80` and `80%` to `0.8`."""

    cleaned = raw_value.strip().replace("%", "")
    amount = parse_money(cleaned)
    if amount is None:
        return None
    if amount > 1:
        return round(amount / 100.0, 4)
    return round(amount, 4)


def parse_hours(raw_value: str) -> float | None:
    """Parse a labor-hour figure from free-form text."""

    return parse_money(raw_value)


def _numeric_text(raw_value: str) -> str:
    """Return only the numeric content from a free-form string."""

    cleaned = raw_value.strip()
    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.replace("$", "")
    cleaned = cleaned.replace("K", "")
    cleaned = cleaned.replace("k", "")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    return cleaned
