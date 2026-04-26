"""Deterministic command routing for WhatsApp command messages."""

from __future__ import annotations

import re
from typing import Any

from apps.ceo_router.worker import parse_ceo_query
from apps.cross_branch_router.worker import parse_cross_branch_query
from apps.supervisor_commands.worker import parse_supervisor_command

_FORMAT_PATTERN = re.compile(r"^\s*format\s+([a-z0-9 _-]+)\s*$", re.IGNORECASE)
_HELP_PATTERN = re.compile(r"^\s*help\s*$", re.IGNORECASE)
_STATUS_PATTERN = re.compile(r"^\s*status\s*$", re.IGNORECASE)
_WHY_REJECTED_PATTERN = re.compile(r"^\s*why\s+rejected\s*$", re.IGNORECASE)

_REPORT_ALIASES = {
    "attendance": "staff_attendance",
    "attendance report": "staff_attendance",
    "bale": "bale_summary",
    "bale summary": "bale_summary",
    "daily bale summary": "bale_summary",
    "day end sales": "sales_income",
    "day end sales report": "sales_income",
    "performance": "staff_performance",
    "released to rail": "bale_summary",
    "sales": "sales_income",
    "sales report": "sales_income",
    "staff attendance": "staff_attendance",
    "staff performance": "staff_performance",
    "staff performance report": "staff_performance",
    "supervisor control": "supervisor_control",
    "supervisor control report": "supervisor_control",
}


def route_whatsapp_command(
    message_text: str | None,
    *,
    source_message_id: str | None = None,
    sender_phone: str | None = None,
    channel: str = "whatsapp",
    is_replay: bool = False,
) -> dict[str, Any]:
    """Return one deterministic command classification object."""

    text = _clean_text(message_text)
    response = {
        "is_command": False,
        "command_name": None,
        "command_text": text,
        "command_argument": None,
        "report_type": None,
        "source_message_id": _clean_text(source_message_id),
        "sender_phone": _clean_text(sender_phone),
        "channel": _clean_text(channel) or "whatsapp",
        "is_replay": bool(is_replay),
        "should_reply": False,
    }
    if text is None:
        return response

    if _HELP_PATTERN.fullmatch(text):
        response.update(
            {
                "is_command": True,
                "command_name": "help",
                "should_reply": True,
            }
        )
        return response
    if _STATUS_PATTERN.fullmatch(text):
        response.update(
            {
                "is_command": True,
                "command_name": "status",
                "should_reply": True,
            }
        )
        return response
    if _WHY_REJECTED_PATTERN.fullmatch(text):
        response.update(
            {
                "is_command": True,
                "command_name": "why_rejected",
                "should_reply": True,
            }
        )
        return response

    supervisor_command = parse_supervisor_command(
        text,
        source_message_id=source_message_id,
        sender_phone=sender_phone,
        channel=channel,
        is_replay=is_replay,
    )
    if supervisor_command is not None:
        return supervisor_command

    cross_branch_query = parse_cross_branch_query(
        text,
        source_message_id=source_message_id,
        sender_phone=sender_phone,
        channel=channel,
        is_replay=is_replay,
    )
    if cross_branch_query is not None:
        return cross_branch_query

    ceo_query = parse_ceo_query(
        text,
        source_message_id=source_message_id,
        sender_phone=sender_phone,
        channel=channel,
        is_replay=is_replay,
    )
    if ceo_query is not None:
        return ceo_query

    format_match = _FORMAT_PATTERN.fullmatch(text)
    if format_match is None:
        return response

    requested_report = _normalized_report_name(format_match.group(1))
    response.update(
        {
            "is_command": True,
            "command_name": "format",
            "command_argument": requested_report,
            "report_type": _REPORT_ALIASES.get(requested_report),
            "should_reply": True,
        }
    )
    return response


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalized_report_name(value: str) -> str:
    return " ".join(value.replace("-", " ").replace("_", " ").casefold().split())
