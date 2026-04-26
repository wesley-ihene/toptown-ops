"""Supervisor-only WhatsApp command parsing and execution."""

from __future__ import annotations

from collections.abc import Mapping
import re
from pathlib import Path
from typing import Any

from apps.apply_engine.worker import process_proposal_command
from apps.approval_engine.worker import process_supervisor_action

_LIST_PROPOSALS_PATTERN = re.compile(r"^\s*list\s+proposals\s*$", re.IGNORECASE)
_SHOW_PROPOSAL_PATTERN = re.compile(r"^\s*show\s+proposal\s+([a-z0-9][a-z0-9._-]*)\s*$", re.IGNORECASE)
_APPROVE_PROPOSAL_PATTERN = re.compile(r"^\s*approve\s+proposal\s+([a-z0-9][a-z0-9._-]*)\s*$", re.IGNORECASE)
_REJECT_PROPOSAL_PATTERN = re.compile(r"^\s*reject\s+proposal\s+([a-z0-9][a-z0-9._-]*)\s*$", re.IGNORECASE)
_SIMULATE_PROPOSAL_PATTERN = re.compile(r"^\s*simulate\s+proposal\s+([a-z0-9][a-z0-9._-]*)\s*$", re.IGNORECASE)
_APPLY_PROPOSAL_PATTERN = re.compile(r"^\s*apply\s+proposal\s+([a-z0-9][a-z0-9._-]*)\s*$", re.IGNORECASE)
_SUPERVISOR_COMMAND_PATTERN = re.compile(
    r"^\s*(approve|reject|replay)\s+([a-z0-9][a-z0-9._-]*)\s*$",
    re.IGNORECASE,
)


def parse_supervisor_command(
    message_text: str | None,
    *,
    source_message_id: str | None = None,
    sender_phone: str | None = None,
    channel: str = "whatsapp",
    is_replay: bool = False,
) -> dict[str, Any] | None:
    """Return one supervisor command payload when syntax matches exactly."""

    text = _clean_text(message_text)
    if text is None:
        return None
    for pattern, command_name in (
        (_LIST_PROPOSALS_PATTERN, "list_proposals"),
        (_SHOW_PROPOSAL_PATTERN, "show_proposal"),
        (_APPROVE_PROPOSAL_PATTERN, "approve_proposal"),
        (_REJECT_PROPOSAL_PATTERN, "reject_proposal"),
        (_SIMULATE_PROPOSAL_PATTERN, "simulate_proposal"),
        (_APPLY_PROPOSAL_PATTERN, "apply_proposal"),
    ):
        match = pattern.fullmatch(text)
        if match is None:
            continue
        proposal_id = match.group(1).strip() if match.groups() else None
        return {
            "is_command": True,
            "command_name": command_name,
            "command_text": text,
            "command_argument": proposal_id,
            "proposal_id": proposal_id,
            "record_id": None,
            "report_type": None,
            "source_message_id": _clean_text(source_message_id),
            "sender_phone": _clean_text(sender_phone),
            "channel": _clean_text(channel) or "whatsapp",
            "is_replay": bool(is_replay),
            "should_reply": True,
            "requires_supervisor_auth": True,
        }
    match = _SUPERVISOR_COMMAND_PATTERN.fullmatch(text)
    if match is None:
        return None

    command_name = match.group(1).casefold()
    record_id = match.group(2).strip()
    return {
        "is_command": True,
        "command_name": command_name,
        "command_text": text,
        "command_argument": record_id,
        "record_id": record_id,
        "report_type": None,
        "source_message_id": _clean_text(source_message_id),
        "sender_phone": _clean_text(sender_phone),
        "channel": _clean_text(channel) or "whatsapp",
        "is_replay": bool(is_replay),
        "should_reply": True,
        "requires_supervisor_auth": True,
    }


def handle_supervisor_command(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Execute one supervisor command and return a reply context."""

    command_name = _clean_text(command.get("command_name"))
    if command_name in {
        "list_proposals",
        "show_proposal",
        "approve_proposal",
        "reject_proposal",
        "simulate_proposal",
        "apply_proposal",
    }:
        result = process_proposal_command(command, output_root=output_root)
    else:
        result = process_supervisor_action(command, output_root=output_root)
    return {
        "response_type": "command_reply",
        "channel": _clean_text(command.get("channel")) or "whatsapp",
        "source_message_id": _clean_text(command.get("source_message_id")),
        "sender_phone": _clean_text(command.get("sender_phone")),
        "branch": result.get("branch"),
        "report_type": result.get("report_type"),
        "governance_status": result.get("governance_status"),
        "reason": result.get("reason"),
        "report_date": result.get("report_date"),
        "should_reply": True,
        "is_replay": bool(command.get("is_replay") is True),
        "response_text": _clean_text(result.get("response_text")) or _response_text(result),
        "command_name": command_name,
    }


def _response_text(result: Mapping[str, Any]) -> str:
    action = _clean_text(result.get("action")) or "supervisor action"
    record_id = _clean_text(result.get("record_id")) or "unknown"
    proposal_id = _clean_text(result.get("proposal_id"))
    status = _clean_text(result.get("status")) or "failed"
    branch = _clean_text(result.get("branch"))
    scope = f" for {branch.replace('_', ' ').title()}" if branch is not None else ""

    if proposal_id is not None:
        if status == "completed" and action == "approve_proposal":
            return f"Proposal {proposal_id} approved{scope}."
        if status == "completed" and action == "reject_proposal":
            return f"Proposal {proposal_id} rejected{scope}."
        if status == "completed" and action == "apply_proposal":
            return f"Proposal {proposal_id} applied{scope}."
        if status == "completed" and action == "simulate_proposal":
            return f"Proposal {proposal_id} simulated{scope}."
        if status == "completed" and action == "show_proposal":
            return f"Proposal {proposal_id} shown{scope}."
        if status == "unauthorized":
            return f"Supervisor action denied for proposal {proposal_id}."
        detail = _clean_text(result.get("message")) or "unable to process the request safely"
        return f"Proposal action failed for {proposal_id}: {detail}."

    if status == "completed":
        if action == "approve":
            return f"Review item {record_id} approved{scope}."
        if action == "reject":
            return f"Review item {record_id} rejected{scope}."
        if action == "replay":
            return f"Replay started for review item {record_id}{scope}."
    if status == "unauthorized":
        return f"Supervisor action denied for review item {record_id}."
    detail = _clean_text(result.get("message")) or "unable to process the request safely"
    return f"Supervisor action failed for review item {record_id}: {detail}."


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
