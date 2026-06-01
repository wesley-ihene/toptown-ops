"""Tests for response artifact persistence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from packages.response_store import write_response_artifacts


def test_response_artifacts_are_written_deterministically(tmp_path: Path) -> None:
    result = write_response_artifacts(
        {
            "source_message_id": "wamid.accepted-1",
            "sender_phone": "67570000000",
            "response_type": "accepted_ack",
            "governance_status": "accepted",
            "report_type": "sales_income",
            "branch": "waigani",
            "reason": None,
            "generated_at": "2026-04-07T12:00:00Z",
            "response_text": "Accepted: Sales Income report for Waigani on 2026-04-07 received.",
            "dispatch_status": "written_only",
        },
        output_root=tmp_path,
    )

    json_path = Path(result["json_path"])
    text_path = Path(result["text_path"])

    assert json_path == tmp_path / "records" / "responses" / "whatsapp" / "2026-04-07" / f"{result['response_id']}.json"
    assert text_path == tmp_path / "records" / "responses" / "whatsapp" / "2026-04-07" / f"{result['response_id']}.txt"
    assert text_path.read_text(encoding="utf-8").endswith("\n")
    assert json.loads(json_path.read_text(encoding="utf-8"))["dispatch_status"] == "written_only"


def test_response_artifacts_allow_idempotent_same_content_write(tmp_path: Path) -> None:
    payload = {
        "source_message_id": "wamid.review-1",
        "sender_phone": "67570000000",
        "response_type": "review_ack",
        "governance_status": "needs_review",
        "report_type": "sales_income",
        "branch": "waigani",
        "reason": "confidence_between_review_and_accept_thresholds",
        "generated_at": "2026-04-07T12:00:00Z",
        "response_text": "Received: Sales Income report for Waigani on 2026-04-07 queued for review.",
        "dispatch_status": "written_only",
    }

    first = write_response_artifacts(payload, output_root=tmp_path)
    second = write_response_artifacts(payload, output_root=tmp_path)

    assert first["response_id"] == second["response_id"]
    assert first["json_path"] == second["json_path"]


def test_response_artifacts_reject_unsafe_overwrite(tmp_path: Path) -> None:
    write_response_artifacts(
        {
            "source_message_id": "wamid.reject-1",
            "sender_phone": "67570000000",
            "response_type": "rejected_fix_request",
            "governance_status": "rejected",
            "report_type": "sales_income",
            "branch": "waigani",
            "reason": "validation_failed",
            "generated_at": "2026-04-07T12:00:00Z",
            "response_text": "Not accepted: your report needs correction. Please resend one corrected report.",
            "dispatch_status": "written_only",
        },
        output_root=tmp_path,
    )

    with pytest.raises(FileExistsError):
        write_response_artifacts(
            {
                "source_message_id": "wamid.reject-1",
                "sender_phone": "67570000000",
                "response_type": "rejected_fix_request",
                "governance_status": "rejected",
                "report_type": "sales_income",
                "branch": "waigani",
                "reason": "validation_failed",
                "generated_at": "2026-04-07T12:00:00Z",
                "response_text": "Changed text.",
                "dispatch_status": "written_only",
            },
            output_root=tmp_path,
        )


def test_response_artifacts_persist_structured_feedback(tmp_path: Path) -> None:
    result = write_response_artifacts(
        {
            "source_message_id": "wamid.feedback-1",
            "sender_phone": "67570000000",
            "response_type": "review_ack",
            "governance_status": "needs_review",
            "report_type": "bale_summary",
            "branch": "lae_5th_street",
            "report_date": "2026-04-25",
            "reason": "confidence_between_review_and_accept_thresholds",
            "generated_at": "2026-04-25T12:00:00Z",
            "response_text": "📊 TAOP BALE SUMMARY REVIEW REQUIRED – LAE 5TH STREET",
            "dispatch_status": "written_only",
            "feedback": {
                "report_type": "bale_summary",
                "branch": "lae_5th_street",
                "report_date": "2026-04-25",
                "decision": "review",
                "status": "needs_review",
                "confidence": 0.76,
                "auto_accept_threshold": 0.88,
                "review_threshold": 0.58,
                "validation_results": [],
                "issues_detected": [
                    {
                        "code": "confidence_between_review_and_accept_thresholds",
                        "title": "Governance reason",
                        "details": ["Confidence is below the auto-accept threshold and needs review."],
                    }
                ],
                "action_required": "Follow the specialist-agent warnings and governance review outcome before resending.",
                "normalized_summary": {"items": 2, "total_qty": 486, "total_amount": 2335.0},
            },
        },
        output_root=tmp_path,
    )

    payload = json.loads(Path(result["json_path"]).read_text(encoding="utf-8"))

    assert payload["report_date"] == "2026-04-25"
    assert payload["feedback"]["decision"] == "review"
    assert payload["feedback"]["normalized_summary"]["total_amount"] == 2335.0
