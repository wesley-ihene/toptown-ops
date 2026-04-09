"""Tests for Phase 1 sales SOP validation."""

from packages.sop_validation.sales import validate_sales


def test_validate_sales_accepts_balanced_payload() -> None:
    result = validate_sales(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 600.0,
                "eftpos_sales": 500.0,
                "mobile_money_sales": 100.0,
                "traffic": 15,
                "served": 12,
            },
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_sales_rejects_invalid_totals_and_served_over_traffic() -> None:
    result = validate_sales(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 600.0,
                "eftpos_sales": 500.0,
                "mobile_money_sales": 50.0,
                "traffic": 10,
                "served": 12,
            },
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["invalid_totals", "invalid_numeric_value"]
