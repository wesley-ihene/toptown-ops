"""Tests for deterministic WhatsApp command handling."""

from __future__ import annotations

from pathlib import Path

from apps.command_handler.worker import handle_whatsapp_command
from packages.response_store import write_response_artifacts


def test_help_command_returns_static_command_reply() -> None:
    response = handle_whatsapp_command(
        {
            "is_command": True,
            "command_name": "help",
            "should_reply": True,
            "channel": "whatsapp",
            "is_replay": False,
        }
    )

    assert response["response_type"] == "command_reply"
    assert response["response_text"].startswith("Supported commands:")


def test_format_command_returns_supported_template() -> None:
    response = handle_whatsapp_command(
        {
            "is_command": True,
            "command_name": "format",
            "report_type": "sales_income",
            "should_reply": True,
            "channel": "whatsapp",
            "is_replay": False,
        }
    )

    assert response["response_text"].startswith("DAY-END SALES REPORT")
    assert "Supervisor Confirmed: YES/NO" in response["response_text"]


def test_status_command_uses_latest_non_command_response_record(tmp_path: Path) -> None:
    _write_response(
        tmp_path,
        source_message_id="wamid.older",
        sender_phone="67570000000",
        response_type="accepted_ack",
        governance_status="accepted",
        report_type="sales_income",
        branch="waigani",
        reason=None,
        generated_at="2026-04-07T09:00:00Z",
        response_text="older",
    )
    _write_response(
        tmp_path,
        source_message_id="wamid.command",
        sender_phone="67570000000",
        response_type="command_reply",
        governance_status=None,
        report_type=None,
        branch=None,
        reason=None,
        generated_at="2026-04-07T10:00:00Z",
        response_text="help",
    )
    _write_response(
        tmp_path,
        source_message_id="wamid.newer",
        sender_phone="67570000000",
        response_type="review_ack",
        governance_status="needs_review",
        report_type="bale_summary",
        branch="bena_road",
        reason="confidence_between_review_and_accept_thresholds",
        generated_at="2026-04-07T11:00:00Z",
        response_text="newer",
    )

    response = handle_whatsapp_command(
        {
            "is_command": True,
            "command_name": "status",
            "sender_phone": "67570000000",
            "should_reply": True,
            "channel": "whatsapp",
            "is_replay": False,
        },
        output_root=tmp_path,
    )

    assert response["branch"] == "bena_road"
    assert response["report_type"] == "bale_summary"
    assert response["governance_status"] == "needs_review"
    assert response["response_text"] == "Latest status: DAILY BALE SUMMARY for Bena Road is in review."


def test_why_rejected_uses_last_rejection_reason(tmp_path: Path) -> None:
    _write_response(
        tmp_path,
        source_message_id="wamid.reject",
        sender_phone="67570000000",
        response_type="rejected_fix_request",
        governance_status="rejected",
        report_type="staff_attendance",
        branch="waigani",
        reason="validation_failed",
        generated_at="2026-04-07T12:00:00Z",
        response_text="rejected",
    )

    response = handle_whatsapp_command(
        {
            "is_command": True,
            "command_name": "why_rejected",
            "sender_phone": "67570000000",
            "should_reply": True,
            "channel": "whatsapp",
            "is_replay": False,
        },
        output_root=tmp_path,
    )

    assert response["reason"] == "validation_failed"
    assert response["response_text"] == (
        "Last rejection: ATTENDANCE REPORT for Waigani. "
        "Reason: required report fields were missing or invalid."
    )


def test_status_command_falls_back_when_no_record_exists(tmp_path: Path) -> None:
    response = handle_whatsapp_command(
        {
            "is_command": True,
            "command_name": "status",
            "sender_phone": "67570000000",
            "should_reply": True,
            "channel": "whatsapp",
            "is_replay": False,
        },
        output_root=tmp_path,
    )

    assert response["response_text"] == "No prior report status was found for this number."


def _write_response(
    root: Path,
    *,
    source_message_id: str,
    sender_phone: str,
    response_type: str,
    governance_status: str | None,
    report_type: str | None,
    branch: str | None,
    reason: str | None,
    generated_at: str,
    response_text: str,
) -> None:
    write_response_artifacts(
        {
            "source_message_id": source_message_id,
            "sender_phone": sender_phone,
            "response_type": response_type,
            "governance_status": governance_status,
            "report_type": report_type,
            "branch": branch,
            "reason": reason,
            "generated_at": generated_at,
            "response_text": response_text,
            "dispatch_status": "generated",
        },
        output_root=root,
    )
