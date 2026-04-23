"""Tests for deterministic review learning summaries."""

from __future__ import annotations

import json
from pathlib import Path

from apps.review_learning_engine import analyze_review_queue


def test_grouped_review_summary_and_artifact_write_are_correct(tmp_path: Path) -> None:
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/waigani/sales/hash-1.json",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-07",
            "reason": "confidence_between_review_and_accept_thresholds",
            "confidence": 0.66,
            "acceptance": {"decision": "accept", "reason": "manual_review_passed"},
            "resolution_status": "accepted_after_review",
        },
    )
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/lae/sales/hash-2.json",
        payload={
            "report_type": "sales",
            "branch": "lae",
            "date": "2026-04-07",
            "reason": "conflicting_record_same_scope",
            "confidence": 0.92,
            "acceptance": {"decision": "reject", "reason": "manual_review_rejected"},
            "resolution_status": "rejected_after_review",
        },
    )
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_06/waigani/hr_attendance/hash-3.json",
        payload={
            "report_type": "hr_attendance",
            "branch": "waigani",
            "date": "2026-04-06",
            "reason": "conflicting_record_same_scope",
            "confidence": 0.55,
            "acceptance": {"status": "review", "reason": "conflicting_record_same_scope"},
            "resolution_status": "open",
        },
    )

    result = analyze_review_queue(
        "2026-04-07",
        window_days=2,
        output_root=tmp_path,
        generated_at="2026-04-07T12:00:00Z",
    )

    summary = result["summary"]
    assert result["output_path"] == str(
        tmp_path / "records" / "learning" / "review_summary" / "2026-04-07.json"
    )
    assert summary["artifact_type"] == "review_summary"
    assert summary["report_date"] == "2026-04-07"
    assert summary["total_review_items"] == 3
    assert summary["review_volume_by_report_type"] == [
        {"report_type": "sales", "count": 2},
        {"report_type": "hr_attendance", "count": 1},
    ]
    assert summary["grouped_review_counts"]["by_branch"] == [
        {"branch": "waigani", "count": 2},
        {"branch": "lae", "count": 1},
    ]
    assert summary["grouped_review_counts"]["by_date"] == [
        {"date": "2026-04-07", "count": 2},
        {"date": "2026-04-06", "count": 1},
    ]

    persisted = json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))
    assert persisted["generated_at"] == "2026-04-07T12:00:00Z"
    assert persisted["analysis_window"] == {
        "start_date": "2026-04-06",
        "end_date": "2026-04-07",
        "window_days": 2,
    }
    assert persisted["source_paths"] == sorted(
        [
            str(tmp_path / "records" / "review" / "2026_04_06" / "waigani" / "hr_attendance" / "hash-3.json"),
            str(tmp_path / "records" / "review" / "2026_04_07" / "lae" / "sales" / "hash-2.json"),
            str(tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales" / "hash-1.json"),
        ]
    )


def test_recurring_reasons_and_outcome_tracking_are_aggregated(tmp_path: Path) -> None:
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/waigani/sales/hash-1.json",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-07",
            "reason": "confidence_between_review_and_accept_thresholds",
            "confidence": 0.65,
            "acceptance": {"decision": "accept"},
            "resolution_status": "accepted_after_review",
        },
    )
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/lae/sales/hash-2.json",
        payload={
            "report_type": "sales",
            "branch": "lae",
            "date": "2026-04-07",
            "governance": {"reasons": ["conflicting_record_same_scope"]},
            "acceptance": {"decision": "reject"},
            "resolution_status": "rejected_after_review",
        },
    )
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/waigani/hr_attendance/hash-3.json",
        payload={
            "report_type": "hr_attendance",
            "branch": "waigani",
            "date": "2026-04-07",
            "reason": "conflicting_record_same_scope",
            "confidence": 0.72,
            "acceptance": {"status": "review"},
            "resolution_status": "open",
        },
    )

    summary = analyze_review_queue("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["review_reasons_summary"] == [
        {
            "reason": "conflicting_record_same_scope",
            "count": 2,
            "report_types": ["hr_attendance", "sales"],
            "branches": ["lae", "waigani"],
            "dates": ["2026-04-07"],
        },
        {
            "reason": "confidence_between_review_and_accept_thresholds",
            "count": 1,
            "report_types": ["sales"],
            "branches": ["waigani"],
            "dates": ["2026-04-07"],
        },
    ]
    assert summary["recurring_review_causes"] == [
        {
            "reason": "conflicting_record_same_scope",
            "count": 2,
            "report_types": ["hr_attendance", "sales"],
            "branches": ["lae", "waigani"],
            "dates": ["2026-04-07"],
        }
    ]
    assert summary["review_outcomes"] == {
        "accepted_after_review": 1,
        "rejected_after_review": 1,
        "still_pending": 1,
    }


def test_confidence_accuracy_analysis_is_present_and_deterministic(tmp_path: Path) -> None:
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/waigani/sales/a-item.json",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-07",
            "reason": "confidence_between_review_and_accept_thresholds",
            "confidence": 0.64,
            "acceptance": {"decision": "accept"},
            "resolution_status": "accepted_after_review",
        },
    )
    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/lae/sales/b-item.json",
        payload={
            "report_type": "sales",
            "branch": "lae",
            "date": "2026-04-07",
            "reason": "conflicting_record_same_scope",
            "confidence": 0.95,
            "acceptance": {"decision": "reject"},
            "resolution_status": "rejected_after_review",
        },
    )

    confidence = analyze_review_queue("2026-04-07", output_root=tmp_path)["summary"]["confidence_accuracy_analysis"]

    assert confidence["false_low_confidence_candidates"]["count"] == 1
    assert confidence["false_low_confidence_candidates"]["items"] == [
        {
            "path": str(tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales" / "a-item.json"),
            "date": "2026-04-07",
            "report_type": "sales",
            "branch": "waigani",
            "reason": "confidence_between_review_and_accept_thresholds",
            "confidence": 0.64,
        }
    ]
    assert confidence["false_high_confidence_candidates"]["count"] == 1
    assert confidence["false_high_confidence_candidates"]["items"] == [
        {
            "path": str(tmp_path / "records" / "review" / "2026_04_07" / "lae" / "sales" / "b-item.json"),
            "date": "2026-04-07",
            "report_type": "sales",
            "branch": "lae",
            "reason": "conflicting_record_same_scope",
            "confidence": 0.95,
        }
    ]


def test_missing_review_directory_is_handled_safely(tmp_path: Path) -> None:
    result = analyze_review_queue("2026-04-07", output_root=tmp_path)

    assert result["status"] == "written"
    assert result["summary"]["total_review_items"] == 0
    assert result["summary"]["review_volume_by_report_type"] == []
    assert result["summary"]["review_reasons_summary"] == []
    assert result["summary"]["branch_review_heatmap"] == []
    assert result["summary"]["recurring_review_causes"] == []
    assert result["summary"]["review_outcomes"] == {
        "accepted_after_review": 0,
        "rejected_after_review": 0,
        "still_pending": 0,
    }
    assert result["summary"]["source_paths"] == []


def test_invalid_review_json_files_are_ignored(tmp_path: Path) -> None:
    invalid_path = tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales" / "bad.json"
    invalid_path.parent.mkdir(parents=True, exist_ok=True)
    invalid_path.write_text("{not-json", encoding="utf-8")

    _write_review_item(
        tmp_path,
        relative_path="records/review/2026_04_07/waigani/sales/good.json",
        payload={
            "report_type": "sales",
            "branch": "waigani",
            "date": "2026-04-07",
            "reason": "confidence_between_review_and_accept_thresholds",
            "acceptance": {"status": "review"},
            "resolution_status": "open",
        },
    )

    summary = analyze_review_queue("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["total_review_items"] == 1
    assert summary["source_paths"] == [
        str(tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales" / "good.json")
    ]


def _write_review_item(root: Path, *, relative_path: str, payload: dict[str, object]) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
