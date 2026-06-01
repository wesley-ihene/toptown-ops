"""Tests for deterministic conversation response routing."""

from __future__ import annotations

import apps.orchestrator_agent.worker as orchestrator_worker
from apps.conversation_router import route_conversation_response
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem


def test_accepted_report_routes_to_accepted_ack() -> None:
    result = AgentResult(
        agent_name="sales_income_agent",
        payload={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "sales_income",
            "branch": "waigani",
            "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
        },
    )

    routed = route_conversation_response(
        result,
        source_message_id="wamid.accepted-1",
        sender_phone="67570000000",
    )

    assert routed is not None
    assert routed["response_type"] == "accepted_ack"
    assert routed["channel"] == "whatsapp"
    assert routed["structured_output_path"] == "records/structured/sales_income/waigani/2026-04-07.json"
    assert routed["should_reply"] is True


def test_review_outcome_routes_to_review_ack() -> None:
    result = AgentResult(
        agent_name="sales_income_agent",
        payload={
            "status": "needs_review",
            "governance": {
                "status": "needs_review",
                "reasons": ["confidence_between_review_and_accept_thresholds"],
            },
            "signal_type": "sales_income",
            "branch": "waigani",
        },
        metadata={"review_queue_path": "records/review/2026_04_07/waigani/sales_income/review-1.json"},
    )

    routed = route_conversation_response(result)

    assert routed is not None
    assert routed["response_type"] == "review_ack"
    assert routed["reason"] == "confidence_between_review_and_accept_thresholds"
    assert routed["review_queue_path"] == "records/review/2026_04_07/waigani/sales_income/review-1.json"
    assert routed["should_reply"] is True


def test_rejected_report_routes_to_rejected_fix_request() -> None:
    result = AgentResult(
        agent_name="sales_income_agent",
        payload={
            "status": "rejected",
            "governance": {"status": "rejected", "reasons": ["validation_failed"]},
            "signal_type": "sales_income",
        },
    )

    routed = route_conversation_response(result)

    assert routed is not None
    assert routed["response_type"] == "rejected_fix_request"
    assert routed["governance_status"] == "rejected"
    assert routed["should_reply"] is True


def test_metadata_validation_reason_surfaces_for_invalid_worker_failure() -> None:
    result = AgentResult(
        agent_name="pricing_stock_release_agent",
        payload={
            "status": "invalid_input",
            "signal_type": "pricing_stock_release",
            "branch": "waigani",
            "report_date": "2026-04-07",
        },
        metadata={
            "validation": {
                "accepted": False,
                "reason_codes": ["correction_request_requires_full_report"],
                "rejections": [
                    {
                        "reason_code": "correction_request_requires_full_report",
                        "reason_detail": "Correction request detected, but full replacement report rows are required.",
                    }
                ],
            }
        },
    )

    routed = route_conversation_response(result)

    assert routed["response_type"] == "rejected_fix_request"
    assert routed["reason"] == "correction_request_requires_full_report"
    assert routed["should_reply"] is True


def test_duplicate_report_routes_to_duplicate_notice() -> None:
    result = AgentResult(
        agent_name="orchestrator_agent",
        payload={
            "status": "duplicate",
            "governance": {"status": "duplicate", "reasons": ["duplicate_message_id"]},
            "policy_guard": {"duplicate": True, "reason": "duplicate_message"},
            "signal_type": "sales_income",
        },
    )

    routed = route_conversation_response(result)

    assert routed is not None
    assert routed["response_type"] == "duplicate_notice"
    assert routed["reason"] == "duplicate_message_id"
    assert routed["should_reply"] is True


def test_duplicate_processing_status_routes_to_duplicate_notice() -> None:
    routed = route_conversation_response(
        {
            "processing_status": "duplicate",
            "classification": {"report_type": "sales"},
        },
        source_message_id="wamid.duplicate-processing-1",
        sender_phone="67570000000",
    )

    assert routed is not None
    assert routed["response_type"] == "duplicate_notice"
    assert routed["should_reply"] is True


def test_duplicate_override_uses_current_accepted_decision_without_structured_write() -> None:
    routed = route_conversation_response(
        {
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "classification": {"report_type": "sales"},
            "signal_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "duplicate_handling": {
                "duplicate": True,
                "write_suppressed": True,
                "reason": "duplicate_message",
                "previous_governance_status": "needs_review",
                "current_governance_status": "accepted",
            },
        },
        source_message_id="wamid.duplicate-override-1",
        sender_phone="67570000000",
    )

    assert routed is not None
    assert routed["response_type"] == "accepted_ack"
    assert routed["reason"] is None
    assert routed["structured_output_path"] is None
    assert routed["should_reply"] is True


def test_unknown_message_routes_to_unknown_message_guidance() -> None:
    routed = route_conversation_response(
        {
            "status": "rejected",
            "pre_ingestion_validation": {
                "status": "rejected",
                "reasons": [{"code": "unsupported_payload_kind", "message": "unsupported"}],
            },
            "classification": {"report_type": "unknown"},
        },
        source_message_id="wamid.unknown-1",
        sender_phone="67570000000",
    )

    assert routed is not None
    assert routed["response_type"] == "unknown_message_guidance"
    assert routed["should_reply"] is True


def test_ambiguous_outcome_is_handled_conservatively() -> None:
    routed = route_conversation_response(
        {
            "status": "accepted",
            "signal_type": "sales_income",
            "branch": "waigani",
        }
    )

    assert routed["response_type"] is None
    assert routed["structured_output_path"] is None
    assert routed["should_reply"] is False


def test_replay_marked_outcome_preserves_classification_and_replay_flag() -> None:
    routed = route_conversation_response(
        AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted_with_warning",
                "governance": {"status": "accepted_with_warning", "reasons": []},
                "signal_type": "sales_income",
                "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
                "replay": {"is_replay": True, "source": "raw"},
            },
        )
    )

    assert routed["response_type"] == "accepted_ack"
    assert routed["should_reply"] is True
    assert routed["is_replay"] is True


def test_missing_metadata_does_not_crash_routing() -> None:
    routed = route_conversation_response(None)

    assert routed == {
        "response_type": None,
        "channel": "whatsapp",
        "source_message_id": None,
        "sender_phone": None,
        "branch": None,
        "report_type": None,
        "report_date": None,
        "governance_status": None,
        "reason": None,
        "review_queue_path": None,
        "structured_output_path": None,
        "context_flags": {
            "previous_response_type": None,
            "correction_attempt": False,
            "repeat_failure": False,
            "correction_success": False,
        },
        "should_reply": False,
        "is_replay": False,
        "feedback_context": None,
    }


def test_hr_attendance_outcome_prefers_signal_subtype_for_report_type() -> None:
    result = AgentResult(
        agent_name="hr_agent",
        payload={
            "status": "needs_review",
            "governance": {
                "status": "needs_review",
                "reasons": ["confidence_between_review_and_accept_thresholds"],
            },
            "signal_type": "hr_staffing",
            "signal_subtype": "staff_attendance",
            "branch": "waigani",
        },
    )

    routed = route_conversation_response(result)

    assert routed["response_type"] == "review_ack"
    assert routed["report_type"] == "staff_attendance"


def test_mixed_review_routes_child_summaries_into_feedback_context() -> None:
    result = AgentResult(
        agent_name="orchestrator_agent",
        payload={
            "status": "needs_review",
            "governance": {"status": "needs_review", "reasons": ["mixed_child_requires_review"]},
            "routing": {"review_reason": "mixed_child_requires_review"},
            "classification": {"report_type": "mixed"},
            "fanout": {
                "children": [
                    {
                        "child_index": 1,
                        "report_type": "sales_income",
                        "response_report_type": "day_end_sales",
                        "status": "accepted",
                        "reason": "totals_reconciled",
                    },
                    {
                        "child_index": 2,
                        "report_type": "supervisor_control",
                        "response_report_type": "supervisor_control_summary",
                        "status": "needs_review",
                        "response_status": "review",
                        "validation_error_code": "child_report_incomplete_or_truncated",
                        "validation_error_message": 'incomplete after "Exceptions es\u2026"',
                    },
                ]
            },
        },
    )

    routed = route_conversation_response(result)

    assert routed["response_type"] == "review_ack"
    assert routed["feedback_context"]["mixed_children"][0]["response_report_type"] == "day_end_sales"
    assert routed["feedback_context"]["mixed_children"][1]["validation_error_code"] == "child_report_incomplete_or_truncated"


def test_review_feedback_uses_routed_attendance_date_from_governance_context() -> None:
    sample_text = "\n".join(
        [
            "ATTENDANCE REPORT",
            "Branch :LAE _5th Street",
            "Date: Thursday , 21/05/26.",
        ]
    )
    routed_work_item = orchestrator_worker._build_routed_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": sample_text},
                "metadata": {
                    "received_at": "2026-05-21T09:00:00Z",
                    "sender": "attendance-date-propagation",
                },
            },
        )
    )
    governance_context = orchestrator_worker._routing_governance_context(routed_work_item.payload)

    result = AgentResult(
        agent_name="hr_agent",
        payload={
            "status": "needs_review",
            "governance": {"status": "needs_review", "reasons": ["validation_failed"]},
            "signal_type": "hr_staffing",
            "signal_subtype": "staff_attendance",
            "warnings": [
                {
                    "code": "missing_items",
                    "severity": "error",
                    "message": "The payload items field must include at least one mapping item.",
                }
            ],
        },
        metadata={
            "validation": {
                "reason_codes": ["missing_items"],
                "rejections": [
                    {
                        "reason_code": "missing_items",
                        "reason_detail": "The payload items field must include at least one mapping item.",
                    }
                ],
            },
            "governance_context": {
                **governance_context,
                "raw_text": sample_text,
            },
        },
    )

    routed = route_conversation_response(result)

    assert routed["response_type"] == "review_ack"
    assert routed["report_type"] == "staff_attendance"
    assert routed["branch"] == "lae_5th_street"
    assert routed["report_date"] == "2026-05-21"
    assert routed["feedback_context"]["report_date"] == "2026-05-21"
    assert routed["feedback_context"]["normalized_report_date"] == "2026-05-21"
    assert routed["feedback_context"]["raw_report_date"] == "Thursday , 21/05/26"
    failed_rule_codes = {
        rule["code"]
        for rule in routed["feedback_context"]["diagnostics"]["failed_rules"]
        if isinstance(rule, dict) and isinstance(rule.get("code"), str)
    }
    assert "missing_report_date" not in failed_rule_codes
    assert "date_unresolved" not in failed_rule_codes


def test_mixed_partial_success_routes_to_accepted_ack_with_child_feedback() -> None:
    result = AgentResult(
        agent_name="orchestrator_agent",
        payload={
            "status": "accepted_with_warning",
            "governance": {"status": "accepted_with_warning", "reasons": []},
            "routing": {"review_reason": None},
            "classification": {"report_type": "mixed"},
            "outputs": ["records/intelligence/supervisor_control/2026-04-28/waigani.json"],
            "fanout": {
                "children": [
                    {
                        "child_index": 1,
                        "report_type": "sales_income",
                        "response_report_type": "day_end_sales",
                        "status": "accepted",
                        "reason": "totals_reconciled",
                    },
                    {
                        "child_index": 2,
                        "report_type": "supervisor_control",
                        "response_report_type": "supervisor_control_summary",
                        "status": "needs_review",
                        "response_status": "review",
                        "validation_error_code": "child_report_incomplete_or_truncated",
                        "validation_error_message": 'incomplete after "Exceptions es\u2026"',
                    },
                ]
            },
        },
    )

    routed = route_conversation_response(result)

    assert routed["response_type"] == "accepted_ack"
    assert routed["structured_output_path"] == "records/intelligence/supervisor_control/2026-04-28/waigani.json"
    assert routed["feedback_context"]["mixed_children"][1]["response_report_type"] == "supervisor_control_summary"
