"""Tests for deterministic threshold recommendation summaries."""

from __future__ import annotations

import json
from pathlib import Path

from apps.threshold_recommendation_engine import generate_threshold_recommendations
from packages.learning_store import write_action_effectiveness, write_review_summary


def test_recommendation_generation_logic_is_deterministic(tmp_path: Path) -> None:
    write_review_summary(
        "2026-04-07",
        {
            "artifact_type": "review_summary",
            "report_date": "2026-04-07",
            "review_volume_by_report_type": [
                {"report_type": "sales", "count": 6},
                {"report_type": "hr_attendance", "count": 4},
            ],
            "confidence_accuracy_analysis": {
                "false_low_confidence_candidates": {
                    "count": 2,
                    "items": [
                        {"report_type": "sales"},
                        {"report_type": "sales"},
                    ],
                },
                "false_high_confidence_candidates": {
                    "count": 2,
                    "items": [
                        {"report_type": "hr_attendance"},
                        {"report_type": "hr_attendance"},
                    ],
                },
            },
        },
        output_root=tmp_path,
    )
    write_action_effectiveness(
        "2026-04-07",
        {
            "artifact_type": "action_effectiveness",
            "report_date": "2026-04-07",
            "summary": {
                "dismissed_rate": 0.25,
                "stale_pending_rate": 0.15,
            },
            "noisy_rules_candidates": [
                {"rule_code": "low_conversion_rate"},
            ],
        },
        output_root=tmp_path,
    )

    result = generate_threshold_recommendations(
        "2026-04-07",
        output_root=tmp_path,
        generated_at="2026-04-07T12:00:00Z",
    )

    summary = result["summary"]
    assert result["output_path"] == str(
        tmp_path / "records" / "learning" / "threshold_recommendations" / "2026-04-07.json"
    )
    assert summary["artifact_type"] == "threshold_recommendations"
    assert summary["recommendation_count"] == 2
    assert summary["recommendations"][0]["recommendation_id"] == "attendance__auto_accept_min__increase"
    assert summary["recommendations"][0]["target_area"] == "report_policy.attendance.confidence_thresholds.auto_accept_min"
    assert summary["recommendations"][0]["current_threshold"] == 0.9
    assert summary["recommendations"][0]["proposed_threshold"] == 0.92
    assert summary["recommendations"][0]["evidence"]["report_type"] == "attendance"
    assert summary["recommendations"][0]["evidence"]["review_volume"] == 4
    assert summary["recommendations"][0]["requires_human_approval"] is True
    assert summary["recommendations"][0]["priority"] == "medium"
    assert summary["recommendations"][0]["simulation_fields"] == {
        "report_date": "2026-04-07",
        "comparison_mode": "confidence_threshold_delta",
        "threshold_field": "auto_accept_min",
    }
    assert "confidence" in summary["recommendations"][0]
    assert "impact_score" in summary["recommendations"][0]

    assert summary["recommendations"][1]["recommendation_id"] == "sales__auto_accept_min__decrease"
    assert summary["recommendations"][1]["target_area"] == "report_policy.sales.confidence_thresholds.auto_accept_min"
    assert summary["recommendations"][1]["current_threshold"] == 0.9
    assert summary["recommendations"][1]["proposed_threshold"] == 0.88
    assert summary["recommendations"][1]["evidence"]["report_type"] == "sales"
    assert summary["recommendations"][1]["evidence"]["review_volume"] == 6
    assert summary["recommendations"][1]["evidence"]["false_low_confidence_share"] == 0.3333
    assert summary["recommendations"][1]["requires_human_approval"] is True
    assert summary["recommendations"][1]["priority"] == "medium"
    assert summary["recommendations"][1]["simulation_fields"] == {
        "report_date": "2026-04-07",
        "comparison_mode": "confidence_threshold_delta",
        "threshold_field": "auto_accept_min",
    }
    assert "confidence" in summary["recommendations"][1]
    assert "impact_score" in summary["recommendations"][1]


def test_requires_human_approval_and_priority_fields_always_exist(tmp_path: Path) -> None:
    write_review_summary(
        "2026-04-07",
        {
            "artifact_type": "review_summary",
            "report_date": "2026-04-07",
            "review_volume_by_report_type": [
                {"report_type": "sales", "count": 5},
            ],
            "confidence_accuracy_analysis": {
                "false_low_confidence_candidates": {
                    "count": 2,
                    "items": [{"report_type": "sales"}, {"report_type": "sales"}],
                },
                "false_high_confidence_candidates": {"count": 0, "items": []},
            },
        },
        output_root=tmp_path,
    )

    recommendations = generate_threshold_recommendations("2026-04-07", output_root=tmp_path)["summary"]["recommendations"]

    assert len(recommendations) == 1
    assert recommendations[0]["requires_human_approval"] is True
    assert "impact_score" in recommendations[0]
    assert "priority" in recommendations[0]


def test_no_live_threshold_mutation_occurs(tmp_path: Path) -> None:
    config_path = Path("/home/clawadmin/.openclaw/workspace/toptown-ops/config/report_policy.json")
    config_before = config_path.read_text(encoding="utf-8")

    write_review_summary(
        "2026-04-07",
        {
            "artifact_type": "review_summary",
            "report_date": "2026-04-07",
            "review_volume_by_report_type": [
                {"report_type": "sales", "count": 5},
            ],
            "confidence_accuracy_analysis": {
                "false_low_confidence_candidates": {
                    "count": 2,
                    "items": [{"report_type": "sales"}, {"report_type": "sales"}],
                },
                "false_high_confidence_candidates": {"count": 0, "items": []},
            },
        },
        output_root=tmp_path,
    )

    generate_threshold_recommendations("2026-04-07", output_root=tmp_path)

    assert config_path.read_text(encoding="utf-8") == config_before


def test_safe_noop_when_insufficient_evidence(tmp_path: Path) -> None:
    write_review_summary(
        "2026-04-07",
        {
            "artifact_type": "review_summary",
            "report_date": "2026-04-07",
            "review_volume_by_report_type": [
                {"report_type": "sales", "count": 4},
            ],
            "confidence_accuracy_analysis": {
                "false_low_confidence_candidates": {
                    "count": 1,
                    "items": [{"report_type": "sales"}],
                },
                "false_high_confidence_candidates": {"count": 0, "items": []},
            },
        },
        output_root=tmp_path,
    )

    summary = generate_threshold_recommendations("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["recommendation_count"] == 0
    assert summary["recommendations"] == []
    assert summary["source_summary_status"]["review_summary_path"] is not None


def test_latest_available_learning_summary_before_date_is_used(tmp_path: Path) -> None:
    write_review_summary(
        "2026-04-06",
        {
            "artifact_type": "review_summary",
            "report_date": "2026-04-06",
            "review_volume_by_report_type": [
                {"report_type": "sales", "count": 5},
            ],
            "confidence_accuracy_analysis": {
                "false_low_confidence_candidates": {
                    "count": 2,
                    "items": [{"report_type": "sales"}, {"report_type": "sales"}],
                },
                "false_high_confidence_candidates": {"count": 0, "items": []},
            },
        },
        output_root=tmp_path,
    )

    summary = generate_threshold_recommendations("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["recommendation_count"] == 1
    assert summary["source_summary_status"]["review_summary_path"] == str(
        tmp_path / "records" / "learning" / "review_summary" / "2026-04-06.json"
    )
