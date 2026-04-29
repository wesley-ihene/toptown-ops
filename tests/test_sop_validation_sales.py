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


def test_validate_sales_allows_partial_payment_breakdown_without_total_mismatch_rejection() -> None:
    result = validate_sales(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 600.0,
                "eftpos_sales": None,
                "mobile_money_sales": None,
            },
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_sales_accepts_payload_with_derived_gross_sales_total() -> None:
    result = validate_sales(
        {
            "branch": "lae_5th_street",
            "report_date": "2026-04-25",
            "metrics": {
                "gross_sales": 5616.0,
                "cash_sales": 4047.0,
                "eftpos_sales": 1569.0,
                "mobile_money_sales": None,
                "traffic": 322,
                "served": 162,
            },
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_sales_surfaces_canonical_declared_and_expected_totals() -> None:
    result = validate_sales(
        {
            "branch": "bena_road",
            "report_date": "2026-04-28",
            "raw_text": "\n".join(
                [
                    "Branch: Bena Road Branch",
                    "Date: 28/04/2026",
                    "",
                    "DAY-END SALES REPORT",
                    "Till #1: Main Shop",
                    "T/Cash: 2205",
                    "T/Card: 345",
                    "Z/Reading: 2550",
                    "",
                    "Till #3: Side Counter",
                    "T/Cash: 435",
                    "T/Card: 25",
                    "Z/Reading: 460",
                    "",
                    "TOTALS",
                    "Total Cash: 2205",
                    "Total Card: 370",
                    "Total Sales: 2575",
                ]
            ),
            "metrics": {
                "gross_sales": 2575.0,
                "cash_sales": 2205.0,
                "eftpos_sales": 370.0,
                "mobile_money_sales": None,
                "traffic": 20,
                "served": 18,
            },
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["sales_totals_mismatch"]

    rejection = result.to_payload()["rejections"][0]
    assert rejection["declared_total_cash"] == 2205.0
    assert rejection["expected_total_cash"] == 2640.0
    assert rejection["declared_total_card"] == 370.0
    assert rejection["expected_total_card"] == 370.0
    assert rejection["declared_total_sales"] == 2575.0
    assert rejection["expected_total_sales"] == 3010.0
