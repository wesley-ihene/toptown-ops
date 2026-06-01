"""Structured review feedback regressions."""

from __future__ import annotations

from apps.response_engine.worker import render_whatsapp_response


def test_review_ack_feedback_contains_reason_codes_and_action_required() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-28",
            "reason": "mixed_report_split_not_safe",
            "feedback_context": {
                "route": "mixed",
                "validation": {
                    "reason_codes": ["mixed_report_split_not_safe"],
                    "rejections": [
                        {
                            "reason_code": "mixed_report_split_not_safe",
                            "reason_detail": "Mixed content could not be split safely.",
                        }
                    ],
                },
            },
        }
    )

    feedback = rendered["feedback"]

    assert feedback["report_type"] == "sales_income"
    assert feedback["branch"] == "waigani"
    assert feedback["report_date"] == "2026-04-28"
    assert feedback["route"] == "mixed"
    assert feedback["status"] == "needs_review"
    assert feedback["reason_codes"] == ["mixed_report_signals"]
    assert feedback["action_required"] == "Send one report family per WhatsApp message and resend."
    assert "Mixed content could not be safely split." in rendered["response_text"]
    assert "Send one report family per WhatsApp message and resend." in rendered["response_text"]


def test_rejected_fix_request_feedback_surfaces_validation_failed_action() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "rejected_fix_request",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "staff_attendance",
            "branch": "waigani",
            "report_date": "2026-04-28",
            "reason": "validation_failed",
        }
    )

    feedback = rendered["feedback"]

    assert feedback["route"] == "staff_attendance"
    assert feedback["status"] == "rejected"
    assert feedback["reason_codes"] == ["validation_failed"]
    assert (
        feedback["action_required"]
        == "Recheck required fields and totals, then resend using the exact SOP format for Staff Attendance Report."
    )
