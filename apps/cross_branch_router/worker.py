"""Deterministic routing and reply formatting for supervisor cross-branch queries."""

from __future__ import annotations

from collections.abc import Mapping
import re
from pathlib import Path
from typing import Any

from apps.analytics_query_engine.worker import execute_cross_branch_query
from packages.branch_registry import canonical_branch_slug_or_none
from packages.common.analytics_loader import display_branch_name

_QUERY_PATTERN = re.compile(
    r"^\s*rank\s+(sales|conversion|productivity|operations|ops)\s+for\s+([a-z0-9][a-z0-9 _-]*?)"
    r"(?:\s+on\s+(\d{4}-\d{2}-\d{2}))?\s*$",
    re.IGNORECASE,
)
_QUERY_TYPES = {
    "sales": "branch_sales_rank",
    "conversion": "branch_conversion_rank",
    "productivity": "branch_productivity_rank",
    "operations": "branch_operational_rank",
    "ops": "branch_operational_rank",
}
_QUERY_LABELS = {
    "branch_sales_rank": "sales",
    "branch_conversion_rank": "conversion",
    "branch_productivity_rank": "productivity",
    "branch_operational_rank": "operational score",
}


def parse_cross_branch_query(
    message_text: str | None,
    *,
    source_message_id: str | None = None,
    sender_phone: str | None = None,
    channel: str = "whatsapp",
    is_replay: bool = False,
) -> dict[str, Any] | None:
    """Return one cross-branch query command when syntax matches exactly."""

    if is_replay:
        return None

    text = _clean_text(message_text)
    if text is None:
        return None
    match = _QUERY_PATTERN.fullmatch(text)
    if match is None:
        return None

    metric_key = match.group(1).casefold()
    query_type = _QUERY_TYPES.get(metric_key)
    if query_type is None:
        return None

    branch = canonical_branch_slug_or_none(match.group(2))
    if branch is None:
        return None
    report_date = _clean_text(match.group(3))
    return {
        "is_command": True,
        "command_name": "cross_branch_query",
        "command_text": text,
        "command_argument": metric_key,
        "query_type": query_type,
        "branch": branch,
        "report_date": report_date,
        "source_message_id": _clean_text(source_message_id),
        "sender_phone": _clean_text(sender_phone),
        "channel": _clean_text(channel) or "whatsapp",
        "is_replay": False,
        "should_reply": True,
        "requires_supervisor_auth": True,
    }


def handle_cross_branch_query(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Execute one cross-branch query and return a response-engine context."""

    result = execute_cross_branch_query(command, output_root=output_root)
    return {
        "response_type": "command_reply",
        "channel": _clean_text(command.get("channel")) or "whatsapp",
        "source_message_id": _clean_text(command.get("source_message_id")),
        "sender_phone": _clean_text(command.get("sender_phone")),
        "branch": _clean_text(result.get("branch")),
        "report_type": None,
        "governance_status": None,
        "reason": _clean_text(result.get("reason")),
        "report_date": _clean_text(result.get("report_date")),
        "should_reply": True,
        "is_replay": False,
        "response_text": _response_text(result),
        "command_name": "cross_branch_query",
    }


def _response_text(result: Mapping[str, Any]) -> str:
    status = _clean_text(result.get("status")) or "failed"
    branch = _clean_text(result.get("branch"))
    query_type = _clean_text(result.get("query_type"))
    report_date = _clean_text(result.get("report_date"))
    branch_label = display_branch_name(branch) if branch is not None else "Requested branch"

    if status == "completed":
        metric_label = _QUERY_LABELS.get(query_type or "", "metric")
        rank = int(result.get("rank") or 0)
        total = int(result.get("total_branches") or 0)
        branch_value = _format_metric_value(query_type, result.get("branch_value"))
        top_branch = _clean_text(result.get("top_branch"))
        top_label = display_branch_name(top_branch) if top_branch is not None else "Unknown"
        top_value = _format_metric_value(query_type, result.get("top_value"))
        return (
            f"{branch_label} ranks {rank} of {total} branches by {metric_label} on {report_date}. "
            f"{branch_label}: {branch_value}. Top branch: {top_label} ({top_value})."
        )

    if status == "unauthorized":
        return "Cross-branch query denied for this branch."

    if status == "failed" and _clean_text(result.get("reason")) == "analytics_not_found":
        return "Cross-branch query unavailable: branch comparison analytics were not found."
    if status == "failed" and _clean_text(result.get("reason")) == "branch_not_present":
        return f"Cross-branch query unavailable: {branch_label} is not present in branch comparison analytics."
    detail = _clean_text(result.get("message")) or "unable to process the query safely"
    return f"Cross-branch query failed: {detail}."


def _format_metric_value(query_type: str | None, value: object) -> str:
    number = _number_or_none(value)
    if number is None:
        return "n/a"
    if query_type == "branch_sales_rank":
        return f"K{number:,.2f}"
    if query_type == "branch_conversion_rank":
        return f"{number:.2%}"
    if query_type == "branch_operational_rank":
        return str(int(round(number)))
    return f"{number:.2f}"


def _number_or_none(value: object) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
