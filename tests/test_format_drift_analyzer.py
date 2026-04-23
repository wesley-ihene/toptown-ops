"""Tests for deterministic format drift summaries."""

from __future__ import annotations

import json
from pathlib import Path

from apps.format_drift_analyzer import analyze_format_drift


def test_issue_aggregation_and_artifact_write_are_deterministic(tmp_path: Path) -> None:
    raw_one = _write_raw(
        tmp_path,
        report_type="sales",
        filename="2026-04-07__waigani__sales.txt",
        text="\n".join(
            [
                "DAY-END SALES REPORT",
                "Gross Sales: 1200",
                "Total Sales = 1200",
            ]
        ),
    )
    raw_two = _write_raw(
        tmp_path,
        report_type="sales",
        filename="2026-04-06__waigani__sales.txt",
        text="\n".join(
            [
                "DAY-END SALES REPORT",
                "gross sales: 1400",
                "Total Sales - 1400",
            ]
        ),
    )
    _write_review(
        tmp_path,
        report_type="sales",
        branch="waigani",
        raw_path=raw_one,
        reason="parser_failure",
        filename="hash-1.json",
    )
    _write_rejected_meta(
        tmp_path,
        report_type="sales",
        branch="waigani",
        rejection_reason="invalid_totals",
        received_at="2026-04-07T11:00:00Z",
        text_path=raw_two,
        filename="sales-reject.meta.json",
    )

    result = analyze_format_drift(
        "2026-04-07",
        output_root=tmp_path,
        generated_at="2026-04-07T12:00:00Z",
    )

    summary = result["summary"]
    assert result["output_path"] == str(
        tmp_path / "records" / "learning" / "format_drift" / "2026-04-07.json"
    )
    assert summary["artifact_type"] == "format_drift"
    assert summary["total_raw_records"] == 2
    assert summary["total_review_records"] == 1
    assert summary["total_rejection_records"] == 1
    assert summary["frequent_format_issues"] == [
        {
            "issue_code": "casing_drift",
            "count": 2,
            "report_types": ["sales"],
            "branches": ["waigani"],
            "examples": ["Gross Sales", "gross sales"],
            "related_review_reasons": [{"reason": "parser_failure", "count": 1}],
            "related_rejection_reasons": [{"reason": "invalid_totals", "count": 1}],
        },
        {
            "issue_code": "inconsistent_totals_syntax",
            "count": 2,
            "report_types": ["sales"],
            "branches": ["waigani"],
            "examples": ["Total Sales - 1400", "Total Sales = 1200"],
            "related_review_reasons": [{"reason": "parser_failure", "count": 1}],
            "related_rejection_reasons": [{"reason": "invalid_totals", "count": 1}],
        },
        {
            "issue_code": "label_spelling_variant",
            "count": 2,
            "report_types": ["sales"],
            "branches": ["waigani"],
            "examples": ["Gross Sales", "gross sales"],
            "related_review_reasons": [{"reason": "parser_failure", "count": 1}],
            "related_rejection_reasons": [{"reason": "invalid_totals", "count": 1}],
        },
    ]
    assert summary["template_improvement_candidates"] == [
        {
            "candidate_id": "sales__casing_drift",
            "report_type": "sales",
            "issue_code": "casing_drift",
            "reason": "Observed repeated casing drift across label lines.",
            "evidence": {"issue_count": 2, "branches": ["waigani"]},
            "proposed_template_note": "Show one consistent label-casing example in the reporting template.",
        },
        {
            "candidate_id": "sales__inconsistent_totals_syntax",
            "report_type": "sales",
            "issue_code": "inconsistent_totals_syntax",
            "reason": "Observed repeated totals syntax drift.",
            "evidence": {"issue_count": 2, "branches": ["waigani"]},
            "proposed_template_note": "Show totals using `Label: value` consistently.",
        },
        {
            "candidate_id": "sales__label_spelling_variant",
            "report_type": "sales",
            "issue_code": "label_spelling_variant",
            "reason": "Observed repeated label spelling variants for one report family.",
            "evidence": {"issue_count": 2, "branches": ["waigani"]},
            "proposed_template_note": "Show one canonical label example in the reporting template.",
        },
    ]
    assert summary["branch_training_notes"] == [
        {
            "branch": "waigani",
            "note": "Reinforce consistent separators and label formatting for waigani.",
            "issue_codes": ["casing_drift", "inconsistent_totals_syntax"],
            "affected_report_types": ["sales"],
            "supporting_reviews": 1,
            "supporting_rejections": 1,
        }
    ]

    persisted = json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))
    assert persisted["generated_at"] == "2026-04-07T12:00:00Z"
    assert persisted["analysis_window"] == {
        "start_date": "2026-04-01",
        "end_date": "2026-04-07",
        "window_days": 7,
    }


def test_branch_and_report_family_summaries_are_written(tmp_path: Path) -> None:
    raw_one = _write_raw(
        tmp_path,
        report_type="sales",
        filename="2026-04-07__waigani__sales.txt",
        text="Gross Sales: 1200\nTotal Sales = 1200\n",
    )
    raw_two = _write_raw(
        tmp_path,
        report_type="attendance",
        filename="2026-04-07__lae__attendance.txt",
        text="STAFF ON DUTY: 5\nTOTAL STAFF: 5\n",
    )
    _write_review(tmp_path, report_type="sales", branch="waigani", raw_path=raw_one, reason="parser_failure", filename="review-sales.json")
    _write_review(tmp_path, report_type="attendance", branch="lae", raw_path=raw_two, reason="needs_review", filename="review-attendance.json")

    summary = analyze_format_drift("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["report_family_summaries"] == [
        {
            "report_type": "attendance",
            "raw_records": 1,
            "issue_counts": [],
            "branch_counts": [{"branch": "lae", "count": 1}],
        },
        {
            "report_type": "sales",
            "raw_records": 1,
            "issue_counts": [
                {"issue_code": "inconsistent_totals_syntax", "count": 1},
                {"issue_code": "punctuation_equals_separator", "count": 1},
            ],
            "branch_counts": [{"branch": "waigani", "count": 1}],
        },
    ]
    assert summary["branch_summaries"] == [
        {
            "branch": "lae",
            "raw_records": 1,
            "issue_counts": [],
            "report_type_counts": [{"report_type": "attendance", "count": 1}],
        },
        {
            "branch": "waigani",
            "raw_records": 1,
            "issue_counts": [
                {"issue_code": "inconsistent_totals_syntax", "count": 1},
                {"issue_code": "punctuation_equals_separator", "count": 1},
            ],
            "report_type_counts": [{"report_type": "sales", "count": 1}],
        },
    ]


def test_sparse_data_is_handled_safely(tmp_path: Path) -> None:
    review_path = tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales" / "orphan.json"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(
        json.dumps({"report_type": "sales", "branch": "waigani", "date": "2026-04-07", "reason": "parser_failure"}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    summary = analyze_format_drift("2026-04-07", output_root=tmp_path)["summary"]

    assert summary["total_raw_records"] == 0
    assert summary["total_review_records"] == 0
    assert summary["total_rejection_records"] == 0
    assert summary["frequent_format_issues"] == []
    assert summary["template_improvement_candidates"] == []
    assert summary["branch_training_notes"] == []
    assert summary["report_family_summaries"] == []
    assert summary["branch_summaries"] == []
    assert summary["source_paths"] == []


def _write_raw(root: Path, *, report_type: str, filename: str, text: str) -> str:
    path = root / "records" / "raw" / "whatsapp" / report_type / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


def _write_review(
    root: Path,
    *,
    report_type: str,
    branch: str,
    raw_path: str,
    reason: str,
    filename: str,
) -> None:
    path = root / "records" / "review" / "2026_04_07" / branch / report_type / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "report_type": report_type,
        "branch": branch,
        "date": "2026-04-07",
        "reason": reason,
        "provenance": {"source_record_path": raw_path},
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_rejected_meta(
    root: Path,
    *,
    report_type: str,
    branch: str,
    rejection_reason: str,
    received_at: str,
    text_path: str,
    filename: str,
) -> None:
    path = root / "records" / "rejected" / "whatsapp" / report_type / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "attempted_report_type": report_type,
        "branch_hint": branch,
        "rejection_reason": rejection_reason,
        "received_at": received_at,
        "original_rejected_path": text_path,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
