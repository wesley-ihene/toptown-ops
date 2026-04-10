"""Focused tests for shared date normalization."""

from __future__ import annotations

from packages.normalization.dates import normalize_report_date


def test_normalize_report_date_recovers_live_whatsapp_sales_date() -> None:
    result = normalize_report_date("Date: Friday, 10/04 /26")

    assert result.normalized_value == "2026-04-10"
    assert result.succeeded is True
    applied_rule_names = [rule.name for rule in result.applied_rules]
    assert "date_label_prefix_removed" in applied_rule_names
    assert "weekday_prefix_removed" in applied_rule_names
    assert "date_separator_spacing_collapsed" in applied_rule_names
    assert "date_canonicalized_to_iso" in applied_rule_names


def test_normalize_report_date_is_idempotent_for_iso_values() -> None:
    result = normalize_report_date("2026-04-10")

    assert result.normalized_value == "2026-04-10"
    assert result.succeeded is True


def test_normalize_report_date_rejects_unrecoverable_input() -> None:
    result = normalize_report_date("Friday / / 26")

    assert result.normalized_value is None
    assert result.hard_errors == ["unrecoverable_date"]
