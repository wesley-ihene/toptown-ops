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

    assert "❌ Branch is missing" in rendered["response_text"]
    assert "1. Branch is missing from the report text." in rendered["response_text"]
    assert "2. TAOP cannot route the report without a branch." in rendered["response_text"]
    assert "Add the Branch line and resend." in rendered["response_text"]


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

    assert "Supervisor Control Report format did not match the expected structure." in rendered["response_text"]
    assert "Use the exact Supervisor Control Report fields before resending." in rendered["response_text"]
    assert "Resend using the exact Supervisor Control Report format." in rendered["response_text"]


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
                                    "reason_code": "invalid_totals",
                                    "reason_detail": "Total sales does not match payment totals.",
                                }
                            ]
                        },
                        "metrics": {
                            "cash_sales": 2205.0,
                            "eftpos_sales": 805.0,
                            "gross_sales": 2575.0,
                            "till_total": 2640.0,
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
    assert "Declared Total Cash: K2,205.00" in rendered["response_text"]
    assert "Calculated Till Cash: K2,640.00" in rendered["response_text"]
    assert "Declared Total Sales: K2,575.00" in rendered["response_text"]
    assert "Expected Total Sales: K3,010.00" in rendered["response_text"]
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

    assert "TAOP split the message into multiple reports." in rendered["response_text"]
    assert "One split report still needs review before final processing." in rendered["response_text"]


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
            "1. Required report fields were missing or invalid.",
            "2. TAOP could not approve this report with the current format.",
            "",
            "ACTION",
            "Resend using the exact SOP format for Staff Attendance Report.",
        ]
    )


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

    assert rendered["response_text"].startswith("📊 TAOP BALE SUMMARY REVIEW – LAE 5TH STREET\nDate: 25/04/26")
    assert "STATUS: ⚠️ REVIEW REQUIRED" in rendered["response_text"]
    assert "VALIDATION RESULTS" in rendered["response_text"]
    assert "✔ Total Qty matches item sum: 486 pcs" in rendered["response_text"]
    assert "✔ Total Amount matches item sum: K2,335.00" in rendered["response_text"]
    assert '"Day: Saturday" is not required by SOP' in rendered["response_text"]
    assert '"K1, 508.00" should be "1508.00"' in rendered["response_text"]
    assert 'Use numbers only, e.g. "74" not "74 pcs"' in rendered["response_text"]
    assert "ACTION REQUIRED\nPlease resend using the standard bale summary format." in rendered["response_text"]
    assert "Score: 0.76" in rendered["response_text"]
    assert "Auto-accept threshold: 0.88" in rendered["response_text"]
    assert rendered["feedback"]["validation_results"][0]["label"] == "Total Qty matches item sum"


def test_bale_review_ack_shows_total_mismatches() -> None:
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

    assert "❌ Total Qty mismatch" in rendered["response_text"]
    assert "Declared: 486" in rendered["response_text"]
    assert "Calculated: 485" in rendered["response_text"]
    assert "Difference: 1" in rendered["response_text"]
    assert "❌ Total Amount mismatch" in rendered["response_text"]
    assert "Declared: K2,335.00" in rendered["response_text"]
    assert "Calculated: K2,334.00" in rendered["response_text"]
    assert "Difference: K1.00" in rendered["response_text"]


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
    assert "✔ Total Qty matches item sum: 529 pcs" in rendered["response_text"]
    assert "✔ Total Amount matches item sum: K6,132.00" in rendered["response_text"]
    assert "✔ Branch resolved" in rendered["response_text"]
    assert "✔ Report stored successfully" in rendered["response_text"]
    assert "SUMMARY" in rendered["response_text"]
    assert "Items: 5" in rendered["response_text"]
    assert "Total Qty: 529 pcs" in rendered["response_text"]
    assert "Total Amount: K6,132.00" in rendered["response_text"]
    assert rendered["response_text"].endswith("No correction required.")


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
