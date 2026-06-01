"""Tests for deterministic WhatsApp response rendering."""

from __future__ import annotations

import pytest

from apps.response_engine import render_whatsapp_response
from apps.response_engine import worker


@pytest.fixture(autouse=True)
def _disable_llm_observability_side_effects(monkeypatch) -> None:
    monkeypatch.setattr(worker, "record_conversation_llm_event", lambda **kwargs: None)
    worker._LLM_REWRITE_CACHE.clear()


def test_accepted_ack_renders_correctly_with_full_metadata() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "accepted_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-23",
        }
    )

    assert rendered == {
        "response_type": "accepted_ack",
        "response_text": "✅ DAY-END SALES REPORT received for Waigani, 23/04/26.\nProcessed successfully.",
        "should_send": True,
        "channel": "whatsapp",
        "is_replay": False,
    }


def test_review_ack_renders_correctly_with_safe_reason() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "bena_road",
            "report_date": "2026-04-05",
            "reason": "confidence_between_review_and_accept_thresholds",
        }
    )

    assert rendered["response_text"] == "\n".join(
        [
            "⚠️ TAOP REVIEW REQUIRED",
            "Report: Daily Bale Summary",
            "Branch: BENA ROAD",
            "Date: 05/04/26",
            "",
            "VALIDATION",
            "✔ Report type detected: Daily Bale Summary",
            "✔ Branch resolved",
            "✔ Date resolved",
            "",
            "ISSUES",
            "1. TAOP could not identify the exact validation issue from the current parser output.",
            "2. Confidence between review and accept thresholds.",
            "",
            "ACTION",
            "Please correct the issues above and resend.",
        ]
    )


def test_review_ack_falls_back_safely_when_reason_missing() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "bena_road",
            "report_date": "2026-04-05",
            "reason": None,
        }
    )

    assert "⚠️ TAOP REVIEW REQUIRED" in rendered["response_text"]
    assert "ISSUES" in rendered["response_text"]
    assert "TAOP could not identify the exact validation issue from the current parser output." in rendered["response_text"]
    assert "Additional verification is required" not in rendered["response_text"]


def test_review_ack_does_not_infer_branch_failure_from_missing_reply_metadata() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "sales_income",
            "reason": "mixed_report_split_not_safe",
            "feedback_context": {
                "raw_text": "\n".join(
                    [
                        "DAY-END SALES REPORT",
                        "Gross Sales: 1200",
                        "Cash Sales: 600",
                    ]
                ),
            },
        }
    )

    assert "ℹ Branch resolution not surfaced in reply metadata" in rendered["response_text"]
    assert "❌ Branch not resolved" not in rendered["response_text"]
    assert "❌ Branch is missing" not in rendered["response_text"]


def test_review_ack_missing_branch_uses_deterministic_branch_guidance() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "staff_attendance",
            "report_date": "2026-04-26",
            "feedback_context": {
                "validation": {
                    "reason_codes": ["missing_branch"],
                    "rejections": [
                        {
                            "reason_code": "missing_branch",
                            "reason_detail": "Branch could not be resolved from the report.",
                        }
                    ],
                },
            },
        }
    )

    assert "❌ Branch unresolved" in rendered["response_text"]
    assert "FAILED CHECKS" in rendered["response_text"]
    assert "1. Branch could not be resolved." in rendered["response_text"]
    assert "- Expected: Branch: <branch>" in rendered["response_text"]
    assert "- Received: missing" in rendered["response_text"]
    assert "ACTION REQUIRED" in rendered["response_text"]
    assert "Branch missing or unclear. Add Branch: <branch>." in rendered["response_text"]
    assert "2. Branch could not be resolved from the report." not in rendered["response_text"]


def test_review_ack_supervisor_control_invalid_format_uses_deterministic_guidance() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "supervisor_control",
            "branch": "waigani",
            "report_date": "2026-04-26",
            "feedback_context": {
                "validation": {
                    "reason_codes": ["missing_fields"],
                    "rejections": [
                        {
                            "reason_code": "missing_fields",
                            "reason_detail": "Supervisor control fields were incomplete.",
                        }
                    ],
                },
                "warnings": [{"code": "missing_fields", "message": "Supervisor control fields were incomplete."}],
            },
        }
    )

    assert "FAILED CHECKS" in rendered["response_text"]
    assert "1. Supervisor control fields were incomplete." in rendered["response_text"]
    assert "ACTION REQUIRED" in rendered["response_text"]
    assert "Report format failed SOP validation. Recheck required fields and totals." in rendered["response_text"]


def test_review_ack_mixed_child_requires_review_surfaces_blocking_sales_totals() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "mixed",
            "reason": "mixed_child_requires_review",
            "feedback_context": {
                "mixed_children": [
                    {
                        "report_type": "sales_income",
                        "report_family": "sales_income",
                        "branch": "bena_road",
                        "report_date": "2026-04-28",
                        "status": "needs_review",
                        "blocks_transactional_processing": True,
                        "warnings": [
                            {
                                "code": "till_mismatch",
                                "severity": "warning",
                                "message": "Till total does not reconcile with cash sales.",
                            }
                        ],
                        "validation": {
                            "rejections": [
                                {
                                    "reason_code": "sales_totals_mismatch",
                                    "reason_detail": "Sales totals do not match till/payment totals.",
                                    "declared_total_cash": 2205.0,
                                    "expected_total_cash": 2640.0,
                                    "declared_total_card": 370.0,
                                    "expected_total_card": 370.0,
                                    "declared_total_sales": 2575.0,
                                    "expected_total_sales": 3010.0,
                                }
                            ]
                        },
                        "metrics": {
                            "cash_sales": 435.0,
                            "eftpos_sales": 25.0,
                            "gross_sales": 460.0,
                            "till_total": 435.0,
                            "deposit_total": 0.0,
                        },
                    },
                    {
                        "report_type": "supervisor_control",
                        "report_family": "supervisor_control",
                        "report_family_label": "intelligence",
                        "status": "accepted",
                        "blocks_transactional_processing": False,
                        "warnings": [
                            {
                                "code": "missing_fields",
                                "severity": "warning",
                                "message": "Supervisor control fields were incomplete.",
                            }
                        ],
                    },
                ]
            },
        }
    )

    assert "⚠️ TAOP REVIEW REQUIRED" in rendered["response_text"]
    assert "Report: Day-End Sales Report" in rendered["response_text"]
    assert "Branch: BENA ROAD" in rendered["response_text"]
    assert "Date: 28/04/26" in rendered["response_text"]
    assert "Sales totals do not match till/payment totals." in rendered["response_text"]
    assert "- Expected: Cash K2,640.00 | Card K370.00 | Sales K3,010.00" in rendered["response_text"]
    assert "- Received: Cash K2,205.00 | Card K370.00 | Sales K2,575.00" in rendered["response_text"]
    assert "Declared Total Sales: K460.00" not in rendered["response_text"]
    assert "Expected Total Sales: K805.00" not in rendered["response_text"]
    assert "Correct the TOTALS section and resend the Day-End Sales Report." in rendered["response_text"]
    assert "One split report still needs review" not in rendered["response_text"]
    assert "Supervisor Control Report format" not in rendered["response_text"]


def test_review_ack_mixed_child_requires_review_falls_back_when_child_detail_missing() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "mixed",
            "reason": "mixed_child_requires_review",
            "feedback_context": {
                "mixed_children": [
                    {
                        "report_type": "sales_income",
                        "report_family": "sales_income",
                        "status": "rejected",
                        "blocks_transactional_processing": True,
                    },
                    {
                        "report_type": "supervisor_control",
                        "report_family": "supervisor_control",
                        "report_family_label": "intelligence",
                        "status": "accepted",
                        "blocks_transactional_processing": False,
                    },
                ]
            },
        }
    )

    assert "Report: Day-End Sales Report" in rendered["response_text"]
    assert "ℹ Branch resolution not surfaced in reply metadata" in rendered["response_text"]
    assert "ℹ Date resolution not surfaced in reply metadata" in rendered["response_text"]
    assert "TAOP split the message into multiple reports, but one split report still needs review." in rendered["response_text"]
    assert "Please resend the report that still needs review as one report per message." in rendered["response_text"]


def test_review_ack_mixed_child_requires_review_lists_child_accountability_for_truncated_supervisor_summary() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "mixed",
            "reason": "mixed_child_requires_review",
            "feedback_context": {
                "mixed_children": [
                    {
                        "child_index": 1,
                        "report_type": "sales_income",
                        "report_family": "sales_income",
                        "branch": "waigani",
                        "report_date": "2026-04-28",
                        "response_report_type": "day_end_sales",
                        "status": "accepted",
                        "response_status": "accepted",
                        "reason": "totals_reconciled",
                        "blocks_transactional_processing": True,
                    },
                    {
                        "child_index": 2,
                        "report_type": "supervisor_control",
                        "report_family": "supervisor_control",
                        "branch": "waigani",
                        "report_date": "2026-04-28",
                        "response_report_type": "supervisor_control_summary",
                        "status": "needs_review",
                        "response_status": "review",
                        "reason": 'incomplete after "Exceptions es\u2026"',
                        "validation_error_code": "child_report_incomplete_or_truncated",
                        "validation_error_message": 'incomplete after "Exceptions es\u2026"',
                        "blocks_transactional_processing": False,
                        "header_line": "Supervisor Control Summary",
                    },
                ]
            },
        }
    )

    assert "Report: Supervisor Control Report" in rendered["response_text"]
    assert "Branch: WAIGANI" in rendered["response_text"]
    assert "Date: 28/04/26" in rendered["response_text"]
    assert '1. incomplete after "Exceptions es\u2026"' in rendered["response_text"]
    assert "- Received: child_report_incomplete_or_truncated" in rendered["response_text"]
    assert "Please resend the report that still needs review as one report per message." in rendered["response_text"]
    assert "child_1" not in rendered["response_text"]
    assert "One split report still needs review" not in rendered["response_text"]


def test_accepted_ack_mixed_partial_success_surfaces_only_reviewed_sales_child() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "accepted_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "mixed",
            "feedback_context": {
                "mixed_children": [
                    {
                        "child_index": 1,
                        "report_type": "sales_income",
                        "report_family": "sales_income",
                        "branch": "bena_road",
                        "report_date": "2026-04-28",
                        "response_report_type": "day_end_sales",
                        "status": "rejected",
                        "response_status": "review",
                        "validation_error_code": "sales_totals_mismatch",
                        "validation_error_message": "Sales totals do not match till/payment totals.",
                        "blocks_transactional_processing": True,
                    },
                    {
                        "child_index": 2,
                        "report_type": "supervisor_control",
                        "report_family": "supervisor_control",
                        "report_family_label": "intelligence",
                        "branch": "bena_road",
                        "report_date": "2026-04-28",
                        "response_report_type": "supervisor_control_summary",
                        "status": "accepted",
                        "response_status": "accepted",
                        "blocks_transactional_processing": False,
                    },
                ]
            },
        }
    )

    assert "✅ Mixed split reports received for BENA ROAD, 28/04/26." in rendered["response_text"]
    assert "Processed: Supervisor Control Summary." in rendered["response_text"]
    assert "Needs review: Day-End Sales Report." in rendered["response_text"]
    assert "Resend only the Day-End Sales Report as one complete report." in rendered["response_text"]
    assert "TAOP REVIEW REQUIRED" not in rendered["response_text"]
    assert "Please resend the report" not in rendered["response_text"]


def test_accepted_ack_mixed_partial_success_surfaces_only_reviewed_supervisor_child() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "accepted_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "mixed",
            "feedback_context": {
                "mixed_children": [
                    {
                        "child_index": 1,
                        "report_type": "sales_income",
                        "report_family": "sales_income",
                        "branch": "waigani",
                        "report_date": "2026-04-28",
                        "response_report_type": "day_end_sales",
                        "status": "accepted",
                        "response_status": "accepted",
                        "reason": "totals_reconciled",
                        "blocks_transactional_processing": True,
                    },
                    {
                        "child_index": 2,
                        "report_type": "supervisor_control",
                        "report_family": "supervisor_control",
                        "report_family_label": "intelligence",
                        "branch": "waigani",
                        "report_date": "2026-04-28",
                        "response_report_type": "supervisor_control_summary",
                        "status": "needs_review",
                        "response_status": "review",
                        "validation_error_code": "child_report_incomplete_or_truncated",
                        "validation_error_message": 'incomplete after "Exceptions es\u2026"',
                        "blocks_transactional_processing": False,
                    },
                ]
            },
        }
    )

    assert "✅ Mixed split reports received for WAIGANI, 28/04/26." in rendered["response_text"]
    assert "Processed: Day-End Sales Report." in rendered["response_text"]
    assert "Needs review: Supervisor Control Summary." in rendered["response_text"]
    assert "Resend only the Supervisor Control Summary as one complete report." in rendered["response_text"]
    assert "TAOP REVIEW REQUIRED" not in rendered["response_text"]


def test_rejected_fix_request_renders_correctly() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "rejected_fix_request",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "staff_attendance",
            "reason": "validation_failed",
        }
    )

    assert rendered["response_text"] == "\n".join(
        [
            "❌ TAOP REPORT REJECTED",
            "Report: Staff Attendance Report",
            "",
            "ISSUES",
            "1. Report format failed SOP validation. Recheck required fields and totals.",
            "2. TAOP could not approve this report with the current format.",
            "",
            "ACTION",
            "Recheck required fields and totals, then resend using the exact SOP format for Staff Attendance Report.",
        ]
    )


def test_rejected_fix_request_renders_correction_requires_full_report_message() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "rejected_fix_request",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "waigani",
            "report_date": "2026-04-24",
            "reason": "correction_request_requires_full_report",
        }
    )

    assert "Correction request detected, but full replacement report rows are required." in rendered["response_text"]
    assert "Resend the full replacement report with all bale item rows and totals." in rendered["response_text"]


def test_rejected_fix_request_surfaces_unsupported_sales_title() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "rejected_fix_request",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-24",
            "reason": "validation_failed",
            "feedback_context": {
                "validation": {
                    "reason_codes": ["unsupported_report_title"],
                    "rejections": [
                        {
                            "reason_code": "unsupported_report_title",
                            "reason_detail": "Unsupported report title.",
                            "expected_title": "DAY-END SALES REPORT",
                            "received_title": "Sales report.",
                        }
                    ],
                },
                "raw_text": "\n".join(
                    [
                        "Sales report.",
                        "Branch: Waigani",
                        "Date: 24/04/26",
                        "Gross Sales: 1200",
                    ]
                ),
            },
        }
    )

    assert rendered["response_text"].startswith(
        "❌ TAOP REPORT REJECTED\nReport: Day-End Sales Report\nBranch: WAIGANI\nDate: 24/04/26"
    )
    assert "1. Unsupported report title." in rendered["response_text"]
    assert "- Expected: DAY-END SALES REPORT" in rendered["response_text"]
    assert "- Received: Sales report." in rendered["response_text"]
    assert "ACTION REQUIRED\nUse the exact title DAY-END SALES REPORT and resend the Day-End Sales Report." in rendered[
        "response_text"
    ]


def test_rejected_fix_request_surfaces_sales_parser_field_failures() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "rejected_fix_request",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-24",
            "reason": "validation_failed",
            "feedback_context": {
                "validation": {
                    "details": {"parser_failure": True},
                },
                "raw_text": "\n".join(
                    [
                        "DAY-END SALES REPORT",
                        "Branch: Waigani",
                        "Date: 24/04/26",
                        "Till 01",
                        "Cashier: Alice",
                        "T/Cash: 500",
                        "Total Cash: 500",
                        "Total Card: 200",
                        "Total Sales: 700",
                        "Total Guest: 12",
                        "Head Count: 10",
                    ]
                ),
            },
        }
    )

    assert "❌ Specialist parser failed: sales_income_agent" in rendered["response_text"]
    assert "Unsupported till header format." in rendered["response_text"]
    assert "- Expected: Till#1 or Till#2" in rendered["response_text"]
    assert "- Received: Till 01" in rendered["response_text"]
    assert "Missing required till support operator." in rendered["response_text"]
    assert "Unsupported customer count labels." in rendered["response_text"]
    assert "ACTION REQUIRED\nCorrect only the failed fields above and resend the Day-End Sales Report." in rendered[
        "response_text"
    ]


def test_review_ack_mixed_split_failure_surfaces_detected_titles_and_resend_guidance() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "mixed",
            "reason": "mixed_report_split_not_safe",
            "feedback_context": {
                "raw_text": "\n".join(
                    [
                        "DAY-END SALES REPORT",
                        "Branch: Waigani",
                        "Date: 24/04/26",
                        "Gross Sales: 1200",
                        "SUPERVISOR CONTROL REPORT",
                        "Cashier Reconciled: Yes",
                    ]
                ),
                "mixed_detection": {
                    "boundary_hints": [
                        {
                            "line_number": 5,
                            "raw_line": "SUPERVISOR CONTROL REPORT",
                        }
                    ]
                },
            },
        }
    )

    assert "❌ Mixed content split failed: Mixed content could not be safely split for fan-out." in rendered["response_text"]
    assert "❌ Specialist parser not run: Mixed fan-out stopped before specialist parsing." in rendered["response_text"]
    assert "1. Mixed content could not be safely split." in rendered["response_text"]
    assert "Detected titles:" in rendered["response_text"]
    assert "DAY-END SALES REPORT" in rendered["response_text"]
    assert "SUPERVISOR CONTROL REPORT" in rendered["response_text"]
    assert "Unsafe boundary: line 5: SUPERVISOR CONTROL REPORT" in rendered["response_text"]
    assert "ACTION REQUIRED\nSend one report family per WhatsApp message and resend." in rendered["response_text"]


def test_rejected_fix_request_bale_parser_failure_surfaces_expected_item_format() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "rejected_fix_request",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "waigani",
            "report_date": "2026-04-24",
            "reason": "validation_failed",
            "feedback_context": {
                "metrics": {
                    "total_qty": 10,
                    "total_amount": 100.0,
                },
                "validation": {
                    "reason_codes": ["missing_items"],
                    "rejections": [
                        {
                            "reason_code": "missing_items",
                            "reason_detail": "No bale items were parsed.",
                        }
                    ],
                },
                "raw_text": "\n".join(
                    [
                        "DAILY BALE SUMMARY - RELEASED TO RAIL",
                        "Branch: Waigani",
                        "Date: 24/04/26",
                        "Total Qty: 10",
                        "Total Amount: K100",
                    ]
                ),
            },
        }
    )

    assert "❌ Specialist parser failed: pricing_stock_release_agent" in rendered["response_text"]
    assert "1. Bale item rows were not recognized." in rendered["response_text"]
    assert "- Expected: # 01. Item Name / Qty: 10 / Amt: K100" in rendered["response_text"]
    assert "- Received: Totals were present but no item rows were parsed." in rendered["response_text"]
    assert "ACTION REQUIRED\nResend the bale rows using one supported item format." in rendered["response_text"]


def test_duplicate_notice_renders_correctly() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "duplicate_notice",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
        }
    )

    assert rendered["response_text"] == (
        "ℹ️ This report was already received and processed earlier.\n"
        "No new processing was applied."
    )


def test_unknown_message_guidance_renders_supported_format_list() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "unknown_message_guidance",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
        }
    )

    assert rendered["response_text"] == "\n".join(
        [
            "❌ TAOP REPORT REJECTED",
            "Reason: Format does not match any supported report type.",
            "",
            "SUPPORTED REPORT TYPES",
            "1. DAY-END SALES REPORT",
            "2. DAILY BALE SUMMARY - RELEASED TO RAIL",
            "3. ATTENDANCE REPORT",
            "4. STAFF PERFORMANCE REPORT",
            "5. SUPERVISOR CONTROL REPORT",
            "6. STORE MONITORING REPORT",
            "",
            "ACTION",
            "If this is a question, ask it as a status request.",
            "If this is a report, resend using one exact report title above.",
        ]
    )


def test_missing_metadata_uses_safe_fallback_wording() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "accepted_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
        }
    )

    assert rendered["response_text"] == "✅ REPORT received.\nProcessed successfully."


def test_operational_query_status_renders_provided_text_and_feedback() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "operational_query_status",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "attendance_status",
            "report_date": "2026-04-25",
            "response_text": "📋 TAOP ATTENDANCE RECEIPT STATUS\nDate: 25/04/26",
            "feedback": {"query_type": "attendance_receipt_status"},
        }
    )

    assert rendered == {
        "response_type": "operational_query_status",
        "response_text": "📋 TAOP ATTENDANCE RECEIPT STATUS\nDate: 25/04/26",
        "should_send": True,
        "channel": "whatsapp",
        "is_replay": False,
        "feedback": {"query_type": "attendance_receipt_status"},
    }


def test_should_reply_false_returns_should_send_false() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": None,
            "channel": "whatsapp",
            "should_reply": False,
            "is_replay": False,
        }
    )

    assert rendered == {
        "response_type": None,
        "response_text": None,
        "should_send": False,
        "channel": "whatsapp",
        "is_replay": False,
    }


def test_replay_marked_input_preserves_is_replay_in_output() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "accepted_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": True,
            "report_type": "sales_income",
        }
    )

    assert rendered["should_send"] is True
    assert rendered["is_replay"] is True


def test_bale_review_ack_renders_specific_taop_feedback() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "lae_5th_street",
            "report_date": "2026-04-25",
            "governance_status": "needs_review",
            "reason": "confidence_between_review_and_accept_thresholds",
            "feedback_context": {
                "report_type": "bale_summary",
                "branch": "lae_5th_street",
                "report_date": "2026-04-25",
                "confidence": 0.76,
                "acceptance": {
                    "confidence": 0.76,
                    "thresholds": {
                        "auto_accept_min": 0.88,
                        "review_min": 0.58,
                        "reject_max": 0.38,
                    },
                },
                "metrics": {
                    "total_qty": 486,
                    "total_amount": 2335.0,
                },
                "items": [
                    {"qty": 74, "amount": 1508.0},
                    {"qty": 412, "amount": 827.0},
                ],
                "raw_text": "\n".join(
                    [
                        "DAILY BALE SUMMARY - RELEASED TO RAIL",
                        "Branch: TTC LAE 5TH STREET BRANCH",
                        "Day: Saturday",
                        "Date: 25/04/26",
                        "Prepared By: Joyce - Supervisor",
                        "# 01. OSH",
                        "Qty: 74 pcs",
                        "Amt: K1, 508.00",
                        "# 02. Jeans",
                        "Qty: 412",
                        "Amt: K827.00",
                        "Total Qty: 486",
                        "Total Amount: K2, 335.00",
                    ]
                ),
            },
        }
    )

    assert rendered["response_text"].startswith(
        "⚠️ TAOP REVIEW REQUIRED\nReport: Daily Bale Summary\nBranch: LAE 5TH STREET\nDate: 25/04/26"
    )
    assert "VALIDATION" in rendered["response_text"]
    assert "✔ Specialist parser passed: pricing_stock_release_agent" in rendered["response_text"]
    assert "1. Confidence is below the auto-accept threshold." in rendered["response_text"]
    assert "- Expected: >= 0.88" in rendered["response_text"]
    assert "- Received: 0.76" in rendered["response_text"]
    assert '"Day: Saturday" is not required by SOP' not in rendered["response_text"]
    assert '"K1, 508.00" should be "1508.00"' not in rendered["response_text"]
    assert 'Use numbers only, e.g. "74" not "74 pcs"' not in rendered["response_text"]
    assert (
        "ACTION REQUIRED\nCorrect only the failed fields above and resend if the report values were wrong."
        in rendered["response_text"]
    )
    assert "Score: 0.76" in rendered["response_text"]
    assert rendered["feedback"]["confidence"] == 0.76
    assert rendered["feedback"]["diagnostics"]["stage"] == "governance"


def test_bale_review_ack_uses_agent_metrics_instead_of_recalculating_raw_totals() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "lae_5th_street",
            "report_date": "2026-04-25",
            "governance_status": "needs_review",
            "reason": "confidence_between_review_and_accept_thresholds",
            "feedback_context": {
                "report_type": "bale_summary",
                "branch": "lae_5th_street",
                "report_date": "2026-04-25",
                "confidence": 0.76,
                "acceptance": {
                    "confidence": 0.76,
                    "thresholds": {
                        "auto_accept_min": 0.88,
                        "review_min": 0.58,
                        "reject_max": 0.38,
                    },
                },
                "metrics": {
                    "total_qty": 485,
                    "total_amount": 2334.0,
                },
                "items": [
                    {"qty": 74, "amount": 1508.0},
                    {"qty": 411, "amount": 826.0},
                ],
                "raw_text": "\n".join(
                    [
                        "DAILY BALE SUMMARY - RELEASED TO RAIL",
                        "Branch: TTC LAE 5TH STREET BRANCH",
                        "Date: 25/04/26",
                        "Prepared By: Joyce - Supervisor",
                        "# 01. OSH",
                        "Qty: 74",
                        "Amt: K1508.00",
                        "# 02. Jeans",
                        "Qty: 411",
                        "Amt: K826.00",
                        "Total Qty: 486",
                        "Total Amount: K2,335.00",
                    ]
                ),
            },
        }
    )

    assert "1. Confidence is below the auto-accept threshold." in rendered["response_text"]
    assert "Total Qty: 485 pcs" not in rendered["response_text"]
    assert "Total Amount: K2,334.00" not in rendered["response_text"]
    assert "❌ Total Qty mismatch" not in rendered["response_text"]
    assert "❌ Total Amount mismatch" not in rendered["response_text"]
    assert "Declared:" not in rendered["response_text"]
    assert "Calculated:" not in rendered["response_text"]
    assert "Difference:" not in rendered["response_text"]


def test_bale_accepted_ack_includes_validation_summary() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "accepted_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "lae_malaita",
            "report_date": "2026-04-25",
            "governance_status": "accepted",
            "feedback_context": {
                "report_type": "bale_summary",
                "branch": "lae_malaita",
                "report_date": "2026-04-25",
                "confidence": 0.94,
                "acceptance": {
                    "confidence": 0.94,
                    "thresholds": {
                        "auto_accept_min": 0.88,
                        "review_min": 0.58,
                        "reject_max": 0.38,
                    },
                },
                "structured_output_path": "records/structured/pricing_stock_release/lae_malaita/2026-04-25.json",
                "metrics": {
                    "total_qty": 529,
                    "total_amount": 6132.0,
                },
                "items": [
                    {"qty": 100, "amount": 1200.0},
                    {"qty": 110, "amount": 1450.0},
                    {"qty": 99, "amount": 980.0},
                    {"qty": 120, "amount": 1420.0},
                    {"qty": 100, "amount": 1082.0},
                ],
                "raw_text": "\n".join(
                    [
                        "DAILY BALE SUMMARY - RELEASED TO RAIL",
                        "Branch: Malaita Street",
                        "Date: 25/04/26",
                        "Prepared By: Maria - Supervisor",
                        "# 01. Item A",
                        "Qty: 100",
                        "Amt: 1200.00",
                        "# 02. Item B",
                        "Qty: 110",
                        "Amt: 1450.00",
                        "# 03. Item C",
                        "Qty: 99",
                        "Amt: 980.00",
                        "# 04. Item D",
                        "Qty: 120",
                        "Amt: 1420.00",
                        "# 05. Item E",
                        "Qty: 100",
                        "Amt: 1082.00",
                        "Total Qty: 529",
                        "Total Amount: 6132.00",
                    ]
                ),
            },
        }
    )

    assert rendered["response_text"].startswith("✅ TAOP BALE SUMMARY ACCEPTED – LAE MALAITA\nDate: 25/04/26")
    assert "STATUS: ✅ ACCEPTED" in rendered["response_text"]
    assert "SUMMARY" in rendered["response_text"]
    assert "Items: 5" in rendered["response_text"]
    assert "Total Qty: 529 pcs" in rendered["response_text"]
    assert "Total Amount: K6,132.00" in rendered["response_text"]
    assert "VALIDATION RESULTS" not in rendered["response_text"]
    assert "No correction required." in rendered["response_text"]


def test_bale_accepted_with_warning_ack_shows_warnings_without_resend() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "accepted_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "bale_summary",
            "branch": "lae_5th_street",
            "report_date": "2026-05-01",
            "governance_status": "accepted_with_warning",
            "feedback_context": {
                "report_type": "bale_summary",
                "branch": "lae_5th_street",
                "report_date": "2026-05-01",
                "status": "accepted_with_warning",
                "metrics": {
                    "total_qty": 492,
                    "total_amount": 3292.0,
                },
                "items": [
                    {"qty": 419, "amount": 2179.0},
                    {"qty": 73, "amount": 1113.0},
                ],
                "warnings": [
                    {
                        "code": "format_cleanup",
                        "severity": "warning",
                        "message": "One or more bale rows required safe currency or quantity format cleanup before parsing.",
                    }
                ],
                "raw_text": "\n".join(
                    [
                        "DAILY BALE SUMMARY - RELEASED TO RAIL",
                        "Branch: TTC LAE 5TH STREET BRANCH",
                        "Date: 01/05/26",
                        "Prepared By: Joyce - Supervisor",
                        "#01. OSH",
                        "Qty: 419pcs",
                        "Amt: K2, 179.00",
                        "#02. Jeans",
                        "Qty: 73pcs",
                        "Amt: K1, 113.00",
                        "Total Qty: 492pcs",
                        "Total Amount: K3, 292.00",
                    ]
                ),
            },
        }
    )

    assert rendered["response_text"].startswith(
        "✅ TAOP BALE SUMMARY ACCEPTED WITH WARNING – LAE 5TH STREET\nDate: 01/05/26"
    )
    assert "STATUS: ⚠️ ACCEPTED WITH WARNING" in rendered["response_text"]
    assert "Total Qty: 492 pcs" in rendered["response_text"]
    assert "Total Amount: K3,292.00" in rendered["response_text"]
    assert "WARNINGS" in rendered["response_text"]
    assert "Format cleanup applied" in rendered["response_text"]
    assert "ACTION REQUIRED" not in rendered["response_text"]
    assert "Please resend" not in rendered["response_text"]
    assert rendered["response_text"].endswith("No resend required.")


def test_duplicate_notice_renders_specific_report_scope() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "duplicate_notice",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "feedback_context": {
                "raw_text": "\n".join(
                    [
                        "DAILY BALE SUMMARY - RELEASED TO RAIL",
                        "Branch: Malaita Street",
                        "Date: 25/04/26",
                        "Total Qty: 529",
                        "Total Amount: 6132.00",
                    ]
                ),
            },
        }
    )

    assert rendered["response_text"] == "\n".join(
        [
            "ℹ️ TAOP DUPLICATE REPORT DETECTED",
            "Report: Daily Bale Summary",
            "Branch: LAE MALAITA",
            "Date: 25/04/26",
            "",
            "This report was already received and processed earlier.",
            "No new processing was applied.",
        ]
    )


def test_duplicate_notice_resolves_attendance_alias_to_staff_attendance_report() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "duplicate_notice",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "attendance",
            "branch": "waigani",
            "report_date": "2026-04-25",
        }
    )

    assert "Report: Staff Attendance Report" in rendered["response_text"]
    assert "Report: Report" not in rendered["response_text"]


def test_duplicate_notice_resolves_sales_alias_to_day_end_sales_report() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "duplicate_notice",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "day_end_sales",
            "branch": "waigani",
            "report_date": "2026-04-25",
        }
    )

    assert "Report: Day-End Sales Report" in rendered["response_text"]
    assert "Report: Report" not in rendered["response_text"]


def test_duplicate_notice_skips_generic_report_placeholder_when_subtype_is_known() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "duplicate_notice",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "report",
            "signal_subtype": "hr_attendance",
            "branch": "waigani",
            "report_date": "2026-04-25",
        }
    )

    assert "Report: Staff Attendance Report" in rendered["response_text"]
    assert "Report: Report" not in rendered["response_text"]


def test_review_ack_resolves_staff_attendance_label_from_placeholder_report_type() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "routing",
            "signal_subtype": "staff_attendance",
            "branch": "waigani",
            "report_date": "2026-04-26",
            "reason": "confidence_between_review_and_accept_thresholds",
            "feedback_context": {
                "raw_text": "\n".join(
                    [
                        "SUNDAY:26/04/26",
                        "STAFFS ATTENDANCE",
                        "1. Alice Koko = P",
                        "2. Grace Masson = Off",
                        "3. Fidelma Wobilo = Leave",
                    ]
                ),
            },
        }
    )

    assert "Report: Staff Attendance Report" in rendered["response_text"]
    assert "Report: Report" not in rendered["response_text"]


def test_review_ack_surfaces_human_tolerance_normalization_details() -> None:
    rendered = render_whatsapp_response(
        {
            "response_type": "review_ack",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
            "report_type": "routing",
            "signal_subtype": "staff_attendance",
            "branch": "waigani",
            "report_date": "2026-04-26",
            "reason": "confidence_between_review_and_accept_thresholds",
            "feedback_context": {
                "human_tolerance": {
                    "report_type_hint": "staff_attendance",
                    "normalized_fields": {
                        "branch": "waigani",
                        "date": "2026-04-26",
                        "report_title": "ATTENDANCE REPORT",
                    },
                    "corrections": [
                        {
                            "field": "branch",
                            "raw_value": "WAIGANI BRANCH",
                            "normalized_value": "waigani",
                        },
                        {
                            "field": "date",
                            "raw_value": "SUNDAY:26/04/26",
                            "normalized_value": "2026-04-26",
                        },
                        {
                            "field": "report_title",
                            "raw_value": "STAFFS ATTENDANCE.",
                            "normalized_value": "ATTENDANCE REPORT",
                        },
                    ],
                },
            },
        }
    )

    assert "NORMALIZED BY TAOP" in rendered["response_text"]
    assert "✔ Branch detected from header: Waigani" in rendered["response_text"]
    assert '✔ Date detected from "SUNDAY:26/04/26"' in rendered["response_text"]
    assert "✔ Report title normalized: STAFFS ATTENDANCE. -> ATTENDANCE REPORT" in rendered["response_text"]
    assert "Report: Routing" not in rendered["response_text"]
