"""Focused tests for shared numeric normalization."""

from __future__ import annotations

from packages.normalization.numbers import normalize_decimal, normalize_int


def test_normalize_decimal_recovers_messy_whatsapp_number_shapes() -> None:
    assert normalize_decimal("K3,489. 00").normalized_value == "3489.00"
    assert normalize_decimal("1 236.00").normalized_value == "1236.00"
    assert normalize_int("(5)").normalized_value == "5"
    assert normalize_decimal("43.86%", allow_percent=True).normalized_value == "43.86"
    assert normalize_int("score: 5/5", scalar_from_ratio=True).normalized_value == "5"


def test_normalize_decimal_rejects_ambiguous_broken_values() -> None:
    result = normalize_decimal("1,23,6")

    assert result.normalized_value is None
    assert result.hard_errors == ["ambiguous_numeric_value"]
