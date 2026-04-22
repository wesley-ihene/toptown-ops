"""Deterministic WhatsApp preview formatting for manual operator actions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def format_whatsapp_action_preview(action: Mapping[str, Any]) -> str:
    """Return one concise WhatsApp-ready preview for a manual action."""

    priority = (_string_or_none(action.get("priority")) or "medium").upper()
    branch = _string_or_none(action.get("branch")) or "unknown"
    report_date = _string_or_none(action.get("report_date")) or "unknown-date"
    summary = _string_or_none(action.get("summary")) or "Review the action artifact."
    assigned_to = _string_or_none(action.get("assigned_to")) or "branch_supervisor"
    requires_ack = "Yes" if action.get("requires_ack") is True else "No"
    action_id = _string_or_none(action.get("action_id")) or "unknown"

    return "\n".join(
        [
            f"TOPTOWN ACTION {priority}",
            f"Branch: {branch} | Date: {report_date}",
            f"Owner: {assigned_to}",
            f"Action: {summary}",
            f"Ack Required: {requires_ack}",
            f"Action ID: {action_id}",
        ]
    )


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
