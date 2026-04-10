"""Focused tests for shared currency normalization."""

from __future__ import annotations

from packages.normalization.currency import normalize_money


def test_normalize_money_recovers_supported_whatsapp_money_shapes() -> None:
    assert normalize_money("K3,489.00").to_payload()["normalized_value"] == "3489.00"
    assert normalize_money("K3,489.00").metadata["currency_code"] == "PGK"
    assert normalize_money("PGK 3489").normalized_value == "3489.00"
    assert normalize_money("3,489.00").metadata["currency_code"] is None
    assert normalize_money("K 3 489.00").normalized_value == "3489.00"


def test_normalize_money_rejects_broken_values() -> None:
    result = normalize_money("PGK K 3,,00")

    assert result.normalized_value is None
    assert result.hard_errors == ["ambiguous_numeric_value"]
