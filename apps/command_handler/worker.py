"""Read-only deterministic handlers for supported WhatsApp commands."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any

from apps.conversation_policy import reason_text_for_response

_HELP_TEXT = "\n".join(
    [
        "Supported commands:",
        "HELP",
        "FORMAT SALES",
        "FORMAT ATTENDANCE",
        "FORMAT STAFF PERFORMANCE",
        "FORMAT BALE SUMMARY",
        "FORMAT SUPERVISOR CONTROL",
        "STATUS",
        "WHY REJECTED",
    ]
)

_FORMAT_TEMPLATES = {
    "sales_income": "\n".join(
        [
            "DAY-END SALES REPORT",
            "Branch: <branch>",
            "Date: DD/MM/YYYY",
            "Z Reading: <number>",
            "Cash Sales: <amount>",
            "Card Sales: <amount>",
            "Total Sales: <amount>",
            "Traffic: <number>",
            "Served: <number>",
            "Staff On Duty: <number>",
            "Cash Variance: <amount>",
            "Over Short Reason: <text>",
            "Supervisor Confirmed: YES/NO",
        ]
    ),
    "staff_attendance": "\n".join(
        [
            "ATTENDANCE REPORT",
            "Branch: <branch>",
            "Date: DD/MM/YYYY",
            "John Doe - Present",
            "Mary Kila - Off",
            "Notes: <text>",
        ]
    ),
    "staff_performance": "\n".join(
        [
            "STAFF PERFORMANCE REPORT",
            "Branch: <branch>",
            "Date: DD/MM/YYYY",
            "John Doe - Score: <number>",
            "Mary Kila - Score: <number>",
            "Notes: <text>",
        ]
    ),
    "bale_summary": "\n".join(
        [
            "DAILY BALE SUMMARY - RELEASED TO RAIL",
            "Branch: <branch>",
            "Date: DD/MM/YYYY",
            "Prepared By: <name>",
            "# 01. Item Name",
            "Qty: <number>",
            "Amt: <amount>",
            "Total Qty: <number>",
            "Total Amount: <amount>",
        ]
    ),
    "supervisor_control": "\n".join(
        [
            "SUPERVISOR CONTROL REPORT",
            "Branch: <branch>",
            "Date: DD/MM/YYYY",
            "Opening Checks: OK/ISSUE",
            "Cash Control: OK/ISSUE",
            "Escalations: <text>",
            "Notes: <text>",
        ]
    ),
}

_REPORT_LABELS = {
    "attendance_status": "ATTENDANCE STATUS",
    "bale_summary_status": "DAILY BALE SUMMARY STATUS",
    "report_status": "REPORT STATUS",
    "sales_income": "DAY-END SALES REPORT",
    "staff_attendance": "ATTENDANCE REPORT",
    "staff_performance": "STAFF PERFORMANCE REPORT",
    "bale_summary": "DAILY BALE SUMMARY",
    "supervisor_control": "SUPERVISOR CONTROL REPORT",
}

_STATUS_LABELS = {
    "operational_query_status": "status provided",
    "accepted_ack": "accepted",
    "correction_accepted_ack": "accepted",
    "correction_review_ack": "in review",
    "correction_repeat_fix_request": "rejected",
    "review_ack": "in review",
    "rejected_fix_request": "rejected",
    "duplicate_ack": "duplicate",
    "duplicate_notice": "duplicate",
    "unknown_message_guidance": "unsupported",
}


def handle_whatsapp_command(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Return one normalized response context for a supported command."""

    command_name = _clean_text(command.get("command_name"))
    response = {
        "response_type": "command_reply" if command.get("is_command") is True and command_name is not None else None,
        "channel": _clean_text(command.get("channel")) or "whatsapp",
        "source_message_id": _clean_text(command.get("source_message_id")),
        "sender_phone": _clean_text(command.get("sender_phone")),
        "branch": None,
        "report_type": _clean_text(command.get("report_type")),
        "governance_status": None,
        "reason": None,
        "report_date": None,
        "should_reply": bool(command.get("should_reply") is True and command.get("is_command") is True and command_name is not None),
        "is_replay": bool(command.get("is_replay") is True),
        "response_text": None,
        "command_name": command_name,
    }
    if response["should_reply"] is not True or command_name is None:
        return response

    if command_name == "help":
        response["response_text"] = _HELP_TEXT
        return response
    if command_name == "format":
        report_type = response["report_type"]
        response["response_text"] = _format_response_text(report_type)
        return response
    if command_name == "status":
        latest = _latest_response_record(response["sender_phone"], output_root=output_root)
        response.update(_status_context(latest))
        return response
    if command_name == "why_rejected":
        latest = _latest_rejected_response_record(response["sender_phone"], output_root=output_root)
        response.update(_rejection_context(latest))
        return response

    response["should_reply"] = False
    response["response_type"] = None
    return response


def _format_response_text(report_type: str | None) -> str:
    if report_type in _FORMAT_TEMPLATES:
        return _FORMAT_TEMPLATES[report_type]
    return "\n".join(
        [
            "Supported format reports:",
            "SALES",
            "ATTENDANCE",
            "STAFF PERFORMANCE",
            "BALE SUMMARY",
            "SUPERVISOR CONTROL",
        ]
    )


def _status_context(latest: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest is None:
        return {
            "response_text": "No prior report status was found for this number.",
        }

    response_type = _clean_text(latest.get("response_type"))
    governance_status = _clean_text(latest.get("governance_status"))
    branch = _clean_text(latest.get("branch"))
    report_type = _clean_text(latest.get("report_type"))
    report_label = _REPORT_LABELS.get(report_type or "", "REPORT")
    status_label = _STATUS_LABELS.get(response_type or "", governance_status or "recorded")
    if branch is not None:
        text = f"Latest status: {report_label} for {_display_branch(branch)} is {status_label}."
    else:
        text = f"Latest status: {report_label} is {status_label}."
    return {
        "response_text": text,
        "branch": branch,
        "report_type": report_type,
        "governance_status": governance_status,
        "reason": _clean_text(latest.get("reason")),
    }


def _rejection_context(latest: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest is None:
        return {
            "response_text": "No recent rejected report was found for this number.",
        }

    branch = _clean_text(latest.get("branch"))
    report_type = _clean_text(latest.get("report_type"))
    reason = _clean_text(latest.get("reason"))
    surfaced_reason = reason_text_for_response("rejected_fix_request", reason) or "the report needs correction before it can be processed"
    report_label = _REPORT_LABELS.get(report_type or "", "REPORT")
    if branch is not None:
        text = f"Last rejection: {report_label} for {_display_branch(branch)}. Reason: {surfaced_reason}."
    else:
        text = f"Last rejection: {report_label}. Reason: {surfaced_reason}."
    return {
        "response_text": text,
        "branch": branch,
        "report_type": report_type,
        "governance_status": _clean_text(latest.get("governance_status")),
        "reason": reason,
    }


def _latest_rejected_response_record(
    sender_phone: str | None,
    *,
    output_root: str | Path | None,
) -> dict[str, Any] | None:
    records = _response_records_for_sender(sender_phone, output_root=output_root)
    rejected = [
        record
        for record in records
        if _clean_text(record.get("response_type")) in {"rejected_fix_request", "correction_repeat_fix_request"}
    ]
    if not rejected:
        return None
    return rejected[-1]


def _latest_response_record(
    sender_phone: str | None,
    *,
    output_root: str | Path | None,
) -> dict[str, Any] | None:
    records = _response_records_for_sender(sender_phone, output_root=output_root)
    if not records:
        return None
    return records[-1]


def _response_records_for_sender(
    sender_phone: str | None,
    *,
    output_root: str | Path | None,
) -> list[dict[str, Any]]:
    if sender_phone is None:
        return []

    records: list[dict[str, Any]] = []
    for json_path in sorted(_responses_dir(output_root=output_root).glob("*/*.json")):
        payload = _load_json(json_path)
        if payload is None:
            continue
        if _clean_text(payload.get("sender_phone")) != sender_phone:
            continue
        if _clean_text(payload.get("response_type")) == "command_reply":
            continue
        records.append(payload)
    records.sort(key=_record_sort_key)
    return records


def _record_sort_key(payload: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _clean_text(payload.get("generated_at")) or "",
        _clean_text(payload.get("source_message_id")) or "",
    )


def _responses_dir(*, output_root: str | Path | None) -> Path:
    root = Path(output_root) if output_root is not None else Path.cwd()
    return root / "records" / "responses" / "whatsapp"


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _display_branch(branch: str) -> str:
    return " ".join(part.capitalize() for part in branch.replace("_", " ").split())


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
