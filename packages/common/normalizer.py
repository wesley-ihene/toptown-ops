"""Normalization helpers for numeric fields."""

from __future__ import annotations

from packages.normalization.currency import normalize_money
from packages.normalization.numbers import normalize_decimal, normalize_int


def parse_money(raw_value: str) -> float | None:
    """Parse a money value from free-form WhatsApp text."""

    normalized = normalize_money(raw_value)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    return float(normalized.normalized_value)


def parse_count(raw_value: str) -> int | None:
    """Parse an integer count from free-form text."""

    normalized = normalize_int(raw_value)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    return int(normalized.normalized_value)


def parse_percent(raw_value: str) -> float | None:
    """Parse a percent-like value without silently scaling it."""

    normalized = normalize_decimal(raw_value, allow_percent=True)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    return float(normalized.normalized_value)


def parse_hours(raw_value: str) -> float | None:
    """Parse a labor-hour figure from free-form text."""

    normalized = normalize_decimal(raw_value)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    return float(normalized.normalized_value)
