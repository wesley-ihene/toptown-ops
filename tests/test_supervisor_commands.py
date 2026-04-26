"""Tests for supervisor command parsing and response formatting."""

from __future__ import annotations

from apps.command_router.worker import route_whatsapp_command
from apps.supervisor_commands import worker


def test_route_whatsapp_command_detects_supervisor_approve() -> None:
    routed = route_whatsapp_command("approve hash_123", sender_phone="67570000000")

    assert routed["is_command"] is True
    assert routed["command_name"] == "approve"
    assert routed["record_id"] == "hash_123"
    assert routed["requires_supervisor_auth"] is True


def test_parse_supervisor_command_requires_record_id() -> None:
    assert worker.parse_supervisor_command("approve") is None


def test_handle_supervisor_command_formats_success_reply(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "process_supervisor_action",
        lambda command, output_root=None: {
            "action": "approve",
            "record_id": "hash_123",
            "status": "completed",
            "branch": "waigani",
            "report_type": "sales",
            "report_date": "2026-04-23",
            "governance_status": "accepted",
            "reason": "approved_after_review",
            "message": "approved successfully",
        },
    )

    response = worker.handle_supervisor_command(
        {
            "is_command": True,
            "command_name": "approve",
            "record_id": "hash_123",
            "sender_phone": "67570000000",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
        }
    )

    assert response["response_type"] == "command_reply"
    assert response["response_text"] == "Review item hash_123 approved for Waigani."


def test_handle_supervisor_command_formats_unauthorized_reply(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "process_supervisor_action",
        lambda command, output_root=None: {
            "action": "reject",
            "record_id": "hash_123",
            "status": "unauthorized",
            "branch": "waigani",
            "report_type": "sales",
            "report_date": "2026-04-23",
            "governance_status": None,
            "reason": "branch_not_allowed",
            "message": "sender is not authorized for this branch",
        },
    )

    response = worker.handle_supervisor_command(
        {
            "is_command": True,
            "command_name": "reject",
            "record_id": "hash_123",
            "sender_phone": "67570000000",
            "channel": "whatsapp",
            "should_reply": True,
            "is_replay": False,
        }
    )

    assert response["response_text"] == "Supervisor action denied for review item hash_123."
