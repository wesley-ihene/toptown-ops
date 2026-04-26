"""Tests for conversation context persistence and flag resolution."""

from __future__ import annotations

from apps.conversation_context import load_sender_context, resolve_context_flags, store_sender_interaction


def test_context_store_persists_and_overwrites_last_interaction(tmp_path) -> None:
    first = store_sender_interaction(
        sender_phone="67570000000",
        response_context={
            "sender_phone": "67570000000",
            "source_message_id": "wamid.1",
            "response_type": "rejected_fix_request",
            "report_type": "sales",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "reason": "validation_failed",
            "should_reply": True,
            "is_replay": False,
        },
        output_root=tmp_path,
        stored_at="2026-04-23T10:00:00Z",
    )
    second = store_sender_interaction(
        sender_phone="67570000000",
        response_context={
            "sender_phone": "67570000000",
            "source_message_id": "wamid.2",
            "response_type": "accepted_ack",
            "report_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "should_reply": True,
            "is_replay": False,
        },
        output_root=tmp_path,
        stored_at="2026-04-23T10:05:00Z",
    )

    loaded = load_sender_context("67570000000", output_root=tmp_path)

    assert first is not None
    assert second is not None
    assert loaded is not None
    assert loaded["stored_at"] == "2026-04-23T10:05:00Z"
    assert loaded["last_interaction"]["source_message_id"] == "wamid.2"
    assert loaded["last_interaction"]["response_type"] == "accepted_ack"


def test_resolver_detects_correction_success_for_matching_scope() -> None:
    flags = resolve_context_flags(
        current_response_context={
            "response_type": "accepted_ack",
            "report_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "is_replay": False,
        },
        stored_context={
            "last_interaction": {
                "response_type": "rejected_fix_request",
                "report_type": "sales",
                "branch": "waigani",
                "report_date": "2026-04-07",
            }
        },
    )

    assert flags["correction_attempt"] is True
    assert flags["correction_success"] is True
    assert flags["repeat_failure"] is False


def test_resolver_avoids_false_positive_for_different_report_family() -> None:
    flags = resolve_context_flags(
        current_response_context={
            "response_type": "accepted_ack",
            "report_type": "staff_attendance",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "is_replay": False,
        },
        stored_context={
            "last_interaction": {
                "response_type": "rejected_fix_request",
                "report_type": "sales",
                "branch": "waigani",
                "report_date": "2026-04-07",
            }
        },
    )

    assert flags == {
        "previous_response_type": "rejected_fix_request",
        "correction_attempt": False,
        "repeat_failure": False,
        "correction_success": False,
    }
