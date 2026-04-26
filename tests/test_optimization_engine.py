"""Tests for deterministic optimization proposal generation."""

from __future__ import annotations

import json
from pathlib import Path

from apps.optimization_engine.worker import build_optimization_proposals, generate_optimization_proposals
from packages.learning_store import write_action_effectiveness, write_review_summary


def test_build_optimization_proposals_is_deterministic_and_side_effect_free(tmp_path: Path) -> None:
    _write_learning_inputs(tmp_path)
    _write_branch_comparison(tmp_path)

    proposals = build_optimization_proposals("2026-04-23", output_root=tmp_path)

    assert len(proposals) == 1
    proposal = proposals[0]
    assert proposal["proposal_type"] == "branch_optimization_action"
    assert proposal["branch"] == "waigani"
    assert proposal["report_type"] == "sales"
    assert proposal["approval_status"] == "pending"
    assert proposal["apply_status"] == "not_applied"
    assert proposal["evidence"]["analytics_snapshot"]["operational_score"] == 72.0
    assert proposal["evidence"]["learning_snapshot"]["branch_action_dismissed_rate"] == 0.25
    assert proposal["proposed_action"]["action_type"] == "report_quality_coaching"
    assert not (tmp_path / "records" / "proposals").exists()


def test_generate_optimization_proposals_persists_records(tmp_path: Path) -> None:
    _write_learning_inputs(tmp_path)
    _write_branch_comparison(tmp_path)

    result = generate_optimization_proposals("2026-04-23", output_root=tmp_path)

    assert result["status"] == "written"
    assert result["proposal_count"] == 1
    assert len(result["output_paths"]) == 1
    persisted = json.loads(Path(result["output_paths"][0]).read_text(encoding="utf-8"))
    assert persisted["branch"] == "waigani"
    assert persisted["status"] == "pending_review"
    assert persisted["proposed_action"]["rule_code"] == "report_quality_coaching"


def _write_learning_inputs(root: Path) -> None:
    write_review_summary(
        "2026-04-23",
        {
            "artifact_type": "review_summary",
            "report_date": "2026-04-23",
            "branch_review_heatmap": [
                {
                    "branch": "waigani",
                    "total_reviews": 3,
                    "by_report_type": [{"report_type": "sales", "count": 3}],
                    "by_reason": [{"reason": "conflicting_record_same_scope", "count": 2}],
                },
                {
                    "branch": "lae_malaita",
                    "total_reviews": 1,
                    "by_report_type": [{"report_type": "sales", "count": 1}],
                    "by_reason": [{"reason": "low_confidence", "count": 1}],
                },
            ],
        },
        output_root=root,
    )
    write_action_effectiveness(
        "2026-04-23",
        {
            "artifact_type": "action_effectiveness",
            "report_date": "2026-04-23",
            "action_response_by_branch": [
                {
                    "branch": "waigani",
                    "total_actions": 4,
                    "dismissed_rate": 0.25,
                    "stale_pending_rate": 0.0,
                }
            ],
        },
        output_root=root,
    )


def _write_branch_comparison(root: Path) -> None:
    path = root / "analytics" / "branch_comparison" / "2026-04-23.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "report_date": "2026-04-23",
                "branch_scorecards": [
                    {
                        "branch": "waigani",
                        "operational_score": 72,
                        "conversion_rate": 0.45,
                        "warning_count": 1,
                    },
                    {
                        "branch": "lae_malaita",
                        "operational_score": 88,
                        "conversion_rate": 0.62,
                        "warning_count": 0,
                    },
                ],
                "ranked_branches_by_sales": [],
                "ranked_branches_by_conversion": [],
                "ranked_branches_by_staff_productivity": [],
                "ranked_branches_by_operational_score": [],
                "warnings": [],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
