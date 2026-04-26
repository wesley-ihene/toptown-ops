"""Tests for deterministic conversation response routing."""

from __future__ import annotations

from apps.conversation_router import route_conversation_response
from packages.signal_contracts.agent_result import AgentResult


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
