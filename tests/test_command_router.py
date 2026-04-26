"""Tests for deterministic WhatsApp command routing."""

from __future__ import annotations

from apps.command_router.worker import route_whatsapp_command


def test_help_command_is_detected() -> None:
    routed = route_whatsapp_command(" help ", source_message_id="wamid.1", sender_phone="67570000000")

    assert routed["is_command"] is True
    assert routed["command_name"] == "help"
    assert routed["should_reply"] is True


def test_format_command_maps_supported_report() -> None:
    routed = route_whatsapp_command("FORMAT staff performance", source_message_id="wamid.2")

    assert routed["is_command"] is True
    assert routed["command_name"] == "format"
    assert routed["command_argument"] == "staff performance"
    assert routed["report_type"] == "staff_performance"


def test_status_command_is_detected() -> None:
    routed = route_whatsapp_command("status")

    assert routed["is_command"] is True
    assert routed["command_name"] == "status"


def test_why_rejected_command_is_detected() -> None:
    routed = route_whatsapp_command("why rejected")

    assert routed["is_command"] is True
    assert routed["command_name"] == "why_rejected"


def test_non_command_text_is_ignored() -> None:
    routed = route_whatsapp_command("DAY-END SALES REPORT\nBranch: Waigani")

    assert routed["is_command"] is False
    assert routed["command_name"] is None
    assert routed["should_reply"] is False


def test_unknown_format_alias_is_still_routed_as_format_command() -> None:
    routed = route_whatsapp_command("format inventory")

    assert routed["is_command"] is True
    assert routed["command_name"] == "format"
    assert routed["report_type"] is None
