"""Deterministic routing and reply formatting for CEO assistant queries."""

from __future__ import annotations

from collections.abc import Mapping
import re
from pathlib import Path
from typing import Any

from apps.executive_engine.worker import execute_executive_query
from packages.common.analytics_loader import display_branch_name

_QUERY_PATTERN = re.compile(
    r"^\s*(?:ceo|executive)\s+(overview|alerts|learning|proposals)(?:\s+on\s+(\d{4}-\d{2}-\d{2}))?\s*$",
    re.IGNORECASE,
)
_QUERY_TYPES = {
    "overview": "executive_overview",
    "alerts": "executive_alerts",
    "learning": "executive_learning",
    "proposals": "executive_proposals",
}


def parse_ceo_query(
    message_text: str | None,
    *,
    source_message_id: str | None = None,
    sender_phone: str | None = None,
    channel: str = "whatsapp",
    is_replay: bool = False,
) -> dict[str, Any] | None:
    """Return one CEO assistant command payload when syntax matches exactly."""

    if is_replay:
        return None
    text = _clean_text(message_text)
    if text is None:
        return None
    match = _QUERY_PATTERN.fullmatch(text)
    if match is None:
        return None

    query_key = match.group(1).casefold()
    query_type = _QUERY_TYPES.get(query_key)
    if query_type is None:
        return None

    return {
        "is_command": True,
        "command_name": "ceo_query",
        "command_text": text,
        "command_argument": query_key,
        "query_type": query_type,
        "report_date": _clean_text(match.group(2)),
        "source_message_id": _clean_text(source_message_id),
        "sender_phone": _clean_text(sender_phone),
        "channel": _clean_text(channel) or "whatsapp",
        "is_replay": False,
        "should_reply": True,
        "requires_supervisor_auth": True,
    }


def handle_ceo_query(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Execute one CEO assistant query and return a response context."""

    result = execute_executive_query(command, output_root=output_root)
    return {
        "response_type": "command_reply",
        "channel": _clean_text(command.get("channel")) or "whatsapp",
        "source_message_id": _clean_text(command.get("source_message_id")),
        "sender_phone": _clean_text(command.get("sender_phone")),
        "branch": None,
        "report_type": None,
        "governance_status": None,
        "reason": _clean_text(result.get("reason")),
        "report_date": _clean_text(result.get("report_date")),
        "should_reply": True,
        "is_replay": False,
        "response_text": _response_text(result),
        "command_name": "ceo_query",
    }


def _response_text(result: Mapping[str, Any]) -> str:
    status = _clean_text(result.get("status")) or "failed"
    query_type = _clean_text(result.get("query_type"))
    report_date = _clean_text(result.get("report_date")) or "unknown date"
    insight = result.get("insight")
    insight_map = dict(insight) if isinstance(insight, Mapping) else {}

    if status == "unauthorized":
        return "Executive query access denied."
    if status != "completed":
        detail = _clean_text(result.get("message")) or "unable to generate executive insight safely"
        return f"Executive query failed: {detail}."

    if query_type == "executive_overview":
        top_branch = _branch_label(insight_map.get("top_branch"))
        weakest_branch = _branch_label(insight_map.get("weakest_branch"))
        return (
            f"Executive overview {report_date}. Top branch: {top_branch}. "
            f"Weakest branch: {weakest_branch}. Critical alerts: {_int_text(insight_map.get('critical_alert_count'))}. "
            f"Pending proposals: {_int_text(insight_map.get('pending_proposals_count'))}."
        )
    if query_type == "executive_alerts":
        highest_risk = _branch_label(insight_map.get("highest_risk_branch"))
        return (
            f"Executive alerts {report_date}. Critical: {_int_text(insight_map.get('critical_alert_count'))}. "
            f"Warning: {_int_text(insight_map.get('warning_alert_count'))}. Highest-risk branch: {highest_risk}. "
            f"Pending proposals: {_int_text(insight_map.get('pending_proposals_count'))}."
        )
    if query_type == "executive_learning":
        recurring = _clean_text(insight_map.get("top_recurring_review_cause")) or "none"
        recommendation = _clean_text(insight_map.get("top_threshold_recommendation")) or "none"
        weakest_branch = _branch_label(insight_map.get("weakest_branch"))
        return (
            f"Executive learning {report_date}. Recurring review cause: {recurring}. "
            f"Top recommendation: {recommendation}. Weakest branch: {weakest_branch}. "
            f"Pending proposals: {_int_text(insight_map.get('pending_proposals_count'))}."
        )
    if query_type == "executive_proposals":
        branch = _branch_label(insight_map.get("leading_pending_branch"))
        return (
            f"Executive proposals {report_date}. Pending: {_int_text(insight_map.get('pending_proposals_count'))}. "
            f"Approved not applied: {_int_text(insight_map.get('approved_not_applied_count'))}. "
            f"Leading branch: {branch}. Critical alerts: {_int_text(insight_map.get('critical_alert_count'))}."
        )
    return f"Executive insight ready for {report_date}."


def _branch_label(value: object) -> str:
    text = _clean_text(value)
    if text is None:
        return "none"
    return display_branch_name(text)


def _int_text(value: object) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return "0"


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
