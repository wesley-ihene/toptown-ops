"""Tests for validation metadata and metric helpers."""

from __future__ import annotations

from packages.validation import (
    ValidationMetadata,
    build_pipeline_health,
    build_rejection,
    merge_consistency_snapshot,
    normalize_rejections,
)


def test_rejection_helpers_normalize_aliases_and_reason_codes() -> None:
    rejection = build_rejection(reason_code=" invalid_totals ", reason_detail=" Totals do not match. ")
    normalized = normalize_rejections(
        [
            {"code": "missing_field", "message": "Date is required.", "field": "report_date"},
            rejection,
        ]
    )
    metadata = ValidationMetadata(
        stage="validator",
        status="rejected",
        accepted=False,
        rejections=normalized,
    )

    assert normalized == [
        {
            "reason_code": "missing_field",
            "reason_detail": "Date is required.",
            "code": "missing_field",
            "message": "Date is required.",
            "field": "report_date",
        },
        {
            "reason_code": "invalid_totals",
            "reason_detail": "Totals do not match.",
            "code": "invalid_totals",
            "message": "Totals do not match.",
        },
    ]
    assert metadata.to_payload()["reason_codes"] == ["missing_field", "invalid_totals"]


def test_pipeline_health_and_consistency_helpers_build_expected_summaries() -> None:
    pipeline_health = build_pipeline_health(
        "2026-04-07",
        {
            "summary": {
                "intake_volume": 4,
                "accept_count": 1,
                "review_count": 1,
                "reject_count": 2,
                "fallback_activation_count": 1,
                "fallback_activation_rate": 0.25,
            },
            "exports": {
                "success_count": 1,
                "failure_count": 1,
            },
        },
    )
    consistency = merge_consistency_snapshot(
        None,
        report_date="2026-04-07",
        branch="waigani",
        snapshot={"issues": [{"reason_code": "invalid_count_mismatch"}]},
    )

    assert pipeline_health["status"] == "critical"
    assert pipeline_health["rates"]["reject_rate"] == 0.5
    assert pipeline_health["rates"]["export_failure_rate"] == 0.5
    assert consistency["summary"] == {
        "branch_count": 1,
        "issue_count": 1,
        "status": "warn",
    }
