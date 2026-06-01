"""Tests for Phase 1 sales SOP validation."""

from packages.report_acceptance import decide_acceptance
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


def test_validate_sales_accepts_cash_over_reconciliation() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T/Cash: 100",
                "T/Card: 50",
                "C/over: 10",
                "Total Sales: 160",
                "Z/Reading: 140",
            ]
        ),
        "metrics": {
            "gross_sales": 160.0,
            "cash_sales": 100.0,
            "eftpos_sales": 50.0,
        },
    }

    result = validate_sales(payload)

    assert result.accepted is True
    assert result.rejection_codes == []
    assert payload["reconciliation"]["expected_z_reading"] == 140.0
    assert payload["reconciliation"]["expected_total_sales"] == 160.0
    assert payload["reconciliation"]["variance_explained_by_cash_over"] == 10.0
    assert payload["reconciliation"]["unexplained_variance"] == 0.0


def test_validate_sales_accepts_cash_down_reconciliation() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T/Cash: 100",
                "T/Card: 50",
                "C/down: 5",
                "Total Sales: 145",
                "Z/Reading: 155",
            ]
        ),
        "metrics": {
            "gross_sales": 145.0,
            "cash_sales": 100.0,
            "eftpos_sales": 50.0,
        },
    }

    result = validate_sales(payload)

    assert result.accepted is True
    assert result.rejection_codes == []
    assert payload["reconciliation"]["expected_z_reading"] == 155.0
    assert payload["reconciliation"]["expected_total_sales"] == 145.0
    assert payload["reconciliation"]["variance_explained_by_cash_down"] == 5.0
    assert payload["reconciliation"]["unexplained_variance"] == 0.0


def test_validate_sales_accepts_item_return_adjustment() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T/Cash: 100",
                "T/Card: 50",
                "Item Return: 5",
                "Total Sales: 145",
                "Z/Reading: 150",
            ]
        ),
        "metrics": {
            "gross_sales": 145.0,
            "cash_sales": 100.0,
            "eftpos_sales": 50.0,
        },
    }

    result = validate_sales(payload)

    assert result.accepted is True
    assert result.rejection_codes == []
    assert payload["reconciliation"]["item_return_adjustment"] == -5.0
    assert payload["reconciliation"]["expected_total_sales"] == 145.0
    assert payload["reconciliation"]["expected_z_reading"] == 150.0


def test_validate_sales_accepts_waigani_returns_when_total_sales_is_net_sales() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-05-20",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 20/05/2026",
                "Till 1:",
                "Z/Reading = K3,766.20",
                "Item returns = K25.00",
                "Till 2:",
                "Z/Reading = K2,755.00",
                "Item returns = K21.00",
                "Gross Z total: K6,521.20",
                "Item returns total: K46.00",
                "Cash Sales: K4,868.20",
                "Card Sales: K1,607.00",
                "Net Sales: K6,475.20",
                "Total Sales: K6,475.20",
            ]
        ),
        "metrics": {
            "gross_sales": 6521.20,
            "net_sales": 6475.20,
            "cash_sales": 4868.20,
            "eftpos_sales": 1607.00,
            "item_returns_total": 46.00,
            "total_returns": 46.00,
            "z_reading": 6521.20,
            "total_sales": 6475.20,
        },
    }

    result = validate_sales(payload)

    assert result.accepted is True
    assert result.rejection_codes == []
    assert payload["reconciliation"]["expected_total_sales"] == 6475.20
    assert payload["reconciliation"]["expected_z_reading"] == 6521.20
    assert payload["reconciliation"]["item_return_adjustment"] == -46.0


def test_validate_sales_accepts_eftpos_card_split_reconciliation() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T/Cash: 100",
                "T/Card: 50",
                "Card Reconciliation: -2",
                "Total Sales: 148",
                "Z/Reading: 150",
            ]
        ),
        "metrics": {
            "gross_sales": 148.0,
            "cash_sales": 100.0,
            "eftpos_sales": 30.0,
            "mobile_money_sales": 20.0,
        },
    }

    result = validate_sales(payload)

    assert result.accepted is True
    assert result.rejection_codes == []
    assert payload["reconciliation"]["eftpos_reconciliation_adjustment"] == -2.0
    assert payload["reconciliation"]["expected_total_sales"] == 148.0
    assert payload["reconciliation"]["expected_total_card"] == 50.0


def test_validate_sales_supports_legacy_reconciliation_field_names() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T_Cash: 80",
                "T_Card: 20",
                "Cash_Over: 5",
                "Cash_Down: 2",
                "Total_Sales: 103",
                "Z_Reading: 97",
            ]
        ),
        "metrics": {
            "gross_sales": 103.0,
            "cash_sales": 80.0,
            "eftpos_sales": 20.0,
        },
    }

    result = validate_sales(payload)

    assert result.accepted is True
    assert result.rejection_codes == []
    assert payload["reconciliation"]["cash_over"] == 5.0
    assert payload["reconciliation"]["cash_down"] == 2.0
    assert payload["reconciliation"]["expected_total_sales"] == 103.0
    assert payload["reconciliation"]["expected_z_reading"] == 97.0


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


def test_validate_sales_unexplained_variance_routes_to_review_when_confident() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T/Cash: 100",
                "T/Card: 50",
                "Total Sales: 155",
                "Z/Reading: 150",
            ]
        ),
        "metrics": {
            "gross_sales": 155.0,
            "cash_sales": 100.0,
            "eftpos_sales": 50.0,
        },
    }

    validation_result = validate_sales(payload)
    acceptance = decide_acceptance(
        "sales",
        validation_result=validation_result,
        work_item_payload={"confidence": 0.8, "status": "accepted"},
    )

    assert validation_result.accepted is False
    assert validation_result.rejection_codes == ["sales_totals_mismatch"]
    assert acceptance.decision == "review"
    assert acceptance.reason == "sales_unexplained_variance_requires_review"


def test_validate_sales_unexplained_variance_rejects_when_confidence_is_low() -> None:
    payload = {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "raw_text": "\n".join(
            [
                "DAY-END SALES REPORT",
                "T/Cash: 100",
                "T/Card: 50",
                "Total Sales: 155",
                "Z/Reading: 150",
            ]
        ),
        "metrics": {
            "gross_sales": 155.0,
            "cash_sales": 100.0,
            "eftpos_sales": 50.0,
        },
    }

    validation_result = validate_sales(payload)
    acceptance = decide_acceptance(
        "sales",
        validation_result=validation_result,
        work_item_payload={"confidence": 0.3, "status": "accepted"},
    )

    assert validation_result.accepted is False
    assert validation_result.rejection_codes == ["sales_totals_mismatch"]
    assert acceptance.decision == "reject"
    assert acceptance.reason == "sales_unexplained_variance_exceeds_threshold"


def test_validate_sales_rejects_weak_legacy_title_alias() -> None:
    result = validate_sales(
        {
            "branch": "waigani",
            "report_date": "2026-04-24",
            "raw_text": "\n".join(
                [
                    "Sales report.",
                    "Branch: Waigani",
                    "Date: 24/04/26",
                    "Gross Sales: 1200",
                    "Cash Sales: 700",
                    "Eftpos Sales: 500",
                ]
            ),
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 700.0,
                "eftpos_sales": 500.0,
                "mobile_money_sales": None,
            },
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["unsupported_report_title"]

    rejection = result.to_payload()["rejections"][0]
    assert rejection["expected_title"] == "DAY-END SALES REPORT"
    assert rejection["received_title"] == "Sales report."
