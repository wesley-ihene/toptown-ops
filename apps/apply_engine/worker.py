"""Supervisor proposal review, simulation, and apply workflow."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from apps.proposals_store.store import list_proposals, load_proposal, update_proposal
from apps.supervisor_auth.worker import authorize_supervisor, lookup_supervisor
from packages.action_store import write_action_record
from packages.record_store.paths import get_action_path, get_action_preview_path
from packages.record_store.writer import write_json_file

_PROPOSAL_ACTIONS = {
    "list_proposals",
    "show_proposal",
    "approve_proposal",
    "reject_proposal",
    "simulate_proposal",
    "apply_proposal",
}


def process_proposal_command(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Execute one supervisor proposal command safely."""

    action = _required_text(command.get("command_name"), field_name="command_name")
    if action not in _PROPOSAL_ACTIONS:
        raise ValueError(f"unsupported proposal command: {action}")

    sender_phone = _text_or_none(command.get("sender_phone"))
    root = Path(output_root) if output_root is not None else Path(".")
    occurred_at = _timestamp()
    if bool(command.get("is_replay") is True):
        proposal_id = _text_or_none(command.get("proposal_id"))
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=None,
            authorized=False,
            status="failed",
            reason="replay_ignored",
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            status="failed",
            reason="replay_ignored",
            message="replay must not execute proposal actions",
            audit_path=str(audit),
        )

    if action == "list_proposals":
        supervisor = lookup_supervisor(sender_phone=sender_phone)
        if supervisor is None:
            audit = _write_audit_log(
                action=action,
                proposal_id=None,
                sender_phone=sender_phone,
                branch=None,
                authorized=False,
                status="unauthorized",
                reason="supervisor_not_found",
                output_root=root,
                occurred_at=occurred_at,
            )
            return _result(
                action=action,
                status="unauthorized",
                reason="supervisor_not_found",
                message="sender is not authorized as a supervisor",
                audit_path=str(audit),
                response_text="Proposal access denied.",
            )
        proposals = list_proposals(output_root=root, branches=supervisor["branches"])
        audit = _write_audit_log(
            action=action,
            proposal_id=None,
            sender_phone=sender_phone,
            branch=None,
            authorized=True,
            status="completed",
            reason="listed",
            output_root=root,
            occurred_at=occurred_at,
            extra={"proposal_ids": [_text_or_none(item.get("proposal_id")) for item in proposals]},
        )
        return _result(
            action=action,
            status="completed",
            reason="listed",
            message="proposal list generated",
            audit_path=str(audit),
            proposal_count=len(proposals),
            proposals=proposals,
            response_text=_list_response_text(proposals),
        )

    proposal_id = _required_text(command.get("proposal_id"), field_name="proposal_id")
    proposal = load_proposal(proposal_id, output_root=root)
    if proposal is None:
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=None,
            authorized=False,
            status="failed",
            reason="proposal_not_found",
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            status="failed",
            reason="proposal_not_found",
            message="proposal not found",
            audit_path=str(audit),
            response_text=f"Proposal {proposal_id} was not found.",
        )

    branch = _text_or_none(proposal.get("branch"))
    authorization = authorize_supervisor(sender_phone=sender_phone, branch=branch)
    if authorization["authorized"] is not True:
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=False,
            status="unauthorized",
            reason=str(authorization["reason"]),
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            branch=branch,
            report_type=_text_or_none(proposal.get("report_type")),
            report_date=_text_or_none(proposal.get("generated_date")),
            status="unauthorized",
            reason=str(authorization["reason"]),
            message="sender is not authorized for this proposal branch",
            audit_path=str(audit),
            response_text=f"Proposal access denied for {proposal_id}.",
        )

    if action == "show_proposal":
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=True,
            status="completed",
            reason="shown",
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            branch=branch,
            report_type=_text_or_none(proposal.get("report_type")),
            report_date=_text_or_none(proposal.get("generated_date")),
            status="completed",
            reason="shown",
            message="proposal shown",
            audit_path=str(audit),
            response_text=_show_response_text(proposal),
        )

    if action == "approve_proposal":
        updated = update_proposal(
            proposal_id,
            {
                "approval_status": "approved",
                "status": "approved",
                "approved_at": occurred_at,
                "approved_by": sender_phone,
            },
            output_root=root,
        )
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=True,
            status="completed",
            reason="approved",
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            branch=branch,
            report_type=_text_or_none(updated.get("report_type")),
            report_date=_text_or_none(updated.get("generated_date")),
            status="completed",
            reason="approved",
            message="proposal approved",
            audit_path=str(audit),
            response_text=f"Proposal {proposal_id} approved.",
        )

    if action == "reject_proposal":
        updated = update_proposal(
            proposal_id,
            {
                "approval_status": "rejected",
                "status": "rejected",
                "rejected_at": occurred_at,
                "rejected_by": sender_phone,
            },
            output_root=root,
        )
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=True,
            status="completed",
            reason="rejected",
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            branch=branch,
            report_type=_text_or_none(updated.get("report_type")),
            report_date=_text_or_none(updated.get("generated_date")),
            status="completed",
            reason="rejected",
            message="proposal rejected",
            audit_path=str(audit),
            response_text=f"Proposal {proposal_id} rejected.",
        )

    if action == "simulate_proposal":
        simulation = _simulation_payload(proposal, output_root=root)
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=True,
            status="completed",
            reason="simulated",
            output_root=root,
            occurred_at=occurred_at,
            extra=simulation,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            branch=branch,
            report_type=_text_or_none(proposal.get("report_type")),
            report_date=_text_or_none(proposal.get("generated_date")),
            status="completed",
            reason="simulated",
            message="proposal simulated",
            audit_path=str(audit),
            simulation=simulation,
            response_text=_simulate_response_text(proposal_id, simulation),
        )

    if _text_or_none(proposal.get("approval_status")) != "approved":
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=True,
            status="failed",
            reason="proposal_not_approved",
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            branch=branch,
            report_type=_text_or_none(proposal.get("report_type")),
            report_date=_text_or_none(proposal.get("generated_date")),
            status="failed",
            reason="proposal_not_approved",
            message="proposal must be approved before apply",
            audit_path=str(audit),
            response_text=f"Proposal {proposal_id} must be approved before apply.",
        )

    if _text_or_none(proposal.get("apply_status")) == "applied":
        audit = _write_audit_log(
            action=action,
            proposal_id=proposal_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=True,
            status="failed",
            reason="proposal_already_applied",
            output_root=root,
            occurred_at=occurred_at,
        )
        return _result(
            action=action,
            proposal_id=proposal_id,
            branch=branch,
            report_type=_text_or_none(proposal.get("report_type")),
            report_date=_text_or_none(proposal.get("generated_date")),
            status="failed",
            reason="proposal_already_applied",
            message="proposal was already applied",
            audit_path=str(audit),
            response_text=f"Proposal {proposal_id} was already applied.",
        )

    action_result = write_action_record(_proposed_action(proposal), output_root=root)
    updated = update_proposal(
        proposal_id,
        {
            "apply_status": "applied",
            "status": "applied",
            "applied_at": occurred_at,
            "applied_by": sender_phone,
            "applied_action_path": action_result["action_path"],
            "applied_preview_path": action_result["preview_path"],
        },
        output_root=root,
    )
    audit = _write_audit_log(
        action=action,
        proposal_id=proposal_id,
        sender_phone=sender_phone,
        branch=branch,
        authorized=True,
        status="completed",
        reason="applied",
        output_root=root,
        occurred_at=occurred_at,
        extra=action_result,
    )
    return _result(
        action=action,
        proposal_id=proposal_id,
        branch=branch,
        report_type=_text_or_none(updated.get("report_type")),
        report_date=_text_or_none(updated.get("generated_date")),
        status="completed",
        reason="applied",
        message="proposal applied",
        audit_path=str(audit),
        action_path=action_result["action_path"],
        preview_path=action_result["preview_path"],
        response_text=f"Proposal {proposal_id} applied.",
    )


def _simulation_payload(proposal: Mapping[str, Any], *, output_root: Path) -> dict[str, Any]:
    proposed_action = _proposed_action(proposal)
    action_path = get_action_path(
        proposed_action["report_date"],
        proposed_action["branch"],
        proposed_action["action_type"],
        proposed_action["action_id"],
        output_root=output_root,
    )
    preview_path = get_action_preview_path(
        proposed_action["report_date"],
        proposed_action["branch"],
        proposed_action["action_type"],
        proposed_action["action_id"],
        output_root=output_root,
    )
    return {
        "action_path": str(action_path),
        "preview_path": str(preview_path),
        "proposed_action": proposed_action,
    }


def _list_response_text(proposals: list[dict[str, Any]]) -> str:
    if not proposals:
        return "No visible proposals were found."
    lines = ["Visible proposals:"]
    for item in proposals[:5]:
        lines.append(
            f"{item['proposal_id']} | {item['branch']} | {item['approval_status']} | {item['apply_status']}"
        )
    return "\n".join(lines)


def _show_response_text(proposal: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"Proposal: {proposal['proposal_id']}",
            f"Branch: {proposal['branch']}",
            f"Status: {proposal['approval_status']} / {proposal['apply_status']}",
            f"Summary: {proposal['summary']}",
        ]
    )


def _simulate_response_text(proposal_id: str, simulation: Mapping[str, Any]) -> str:
    action_path = _text_or_none(simulation.get("action_path")) or "unknown"
    return f"Simulation ready for proposal {proposal_id}. Action would be written to {action_path}."


def _proposed_action(proposal: Mapping[str, Any]) -> dict[str, Any]:
    action = proposal.get("proposed_action")
    if not isinstance(action, Mapping):
        raise ValueError("proposal missing proposed_action")
    return dict(action)


def _write_audit_log(
    *,
    action: str,
    proposal_id: str | None,
    sender_phone: str | None,
    branch: str | None,
    authorized: bool,
    status: str,
    reason: str,
    output_root: Path,
    occurred_at: str,
    extra: Mapping[str, Any] | None = None,
) -> Path:
    audit_root = output_root / "records" / "audit" / "proposal_actions" / occurred_at[:10]
    proposal_segment = proposal_id or "global"
    target = audit_root / f"{occurred_at.replace(':', '').replace('-', '')}__{action}__{proposal_segment}.json"
    payload = {
        "action": action,
        "proposal_id": proposal_id,
        "sender_phone": sender_phone,
        "branch": branch,
        "authorized": authorized,
        "status": status,
        "reason": reason,
        "occurred_at": occurred_at,
    }
    if isinstance(extra, Mapping):
        payload["details"] = dict(extra)
    write_json_file(target, payload)
    return target


def _result(
    *,
    action: str,
    status: str,
    reason: str,
    message: str,
    audit_path: str,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "action": action,
        "status": status,
        "reason": reason,
        "message": message,
        "audit_path": audit_path,
    }
    payload.update(extra)
    return payload


def _timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _required_text(value: object, *, field_name: str) -> str:
    text = _text_or_none(value)
    if text is None:
        raise ValueError(f"proposal command field `{field_name}` must be a non-empty string")
    return text


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
