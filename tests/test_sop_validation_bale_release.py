"""Tests for Phase 1 bale release SOP validation."""

from packages.sop_validation.bale_release import validate_bale_release


def test_validate_bale_release_accepts_consistent_payload() -> None:
    result = validate_bale_release(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "bales_processed": 2,
                "bales_released": 2,
                "total_qty": 10,
                "total_amount": 100.0,
            },
            "items": [
                {"bale_id": "01", "item_name": "OSH", "qty": 4, "amount": 40.0},
                {"bale_id": "02", "item_name": "Jeans", "qty": 6, "amount": 60.0},
            ],
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def test_validate_bale_release_rejects_release_over_processed_and_bad_totals() -> None:
    result = validate_bale_release(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "bales_processed": 1,
                "bales_released": 2,
                "total_qty": 12,
                "total_amount": 90.0,
            },
            "items": [
                {"bale_id": "01", "item_name": "OSH", "qty": 4, "amount": 40.0},
                {"bale_id": "02", "item_name": "Jeans", "qty": 6, "amount": 60.0},
            ],
        }
    )

    assert result.accepted is False
    assert result.rejection_codes == ["invalid_count_mismatch", "invalid_totals", "invalid_totals"]
