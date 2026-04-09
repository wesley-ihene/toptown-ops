"""Standalone worker for rejection feedback generation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from apps.rejection_feedback_agent.formatter import format_feedback_message
from apps.rejection_feedback_agent.record_store import write_feedback_record
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "rejection_feedback_agent"
_SUPPORTED_CHANNELS = {"whatsapp"}


@dataclass(slots=True)
class RejectionFeedbackAgentWorker:
    """Specialist worker for rejected-report feedback generation."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into one feedback artifact result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Generate feedback artifacts for one rejected report without transport side effects."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    validation_errors = _validate_input(payload)
    if validation_errors:
        return _failure_result(validation_errors)

    report_type = str(payload["report_type"]).strip()
    channel = str(payload.get("channel") or "whatsapp").strip().lower()
    dry_run = bool(payload.get("dry_run", False))
    branch = _optional_string(payload.get("branch"))
    report_date = _optional_string(payload.get("report_date"))
    source_record_path = _optional_string(payload.get("source_record_path"))
    rejections = _normalize_rejections(payload["rejections"])

    feedback_message = format_feedback_message(
        report_type=report_type,
        rejections=rejections,
        branch=branch,
        report_date=report_date,
    )

    record_payload = {
        "agent": AGENT_NAME,
        "status": "ready",
        "report_type": report_type,
        "branch": branch,
        "report_date": report_date,
        "channel": channel,
        "dry_run": dry_run,
        "source_record_path": source_record_path,
        "feedback_message": feedback_message,
        "rejections": rejections,
        "delivery": {
            "channel": channel,
            "dry_run": dry_run,
            "dispatch_status": "dry_run" if dry_run else "pending_dispatch",
            "dispatched": False,
        },
    }
    written_paths = write_feedback_record(
        report_type=report_type,
        channel=channel,
        feedback_message=feedback_message,
        payload=record_payload,
        dry_run=dry_run,
    )

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            **record_payload,
            "record": written_paths,
        },
    )


def _validate_input(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    """Validate the rejection-feedback work item contract."""

    errors: list[dict[str, str]] = []

    report_type = payload.get("report_type")
    if not isinstance(report_type, str) or not report_type.strip():
        errors.append(
            {
                "code": "missing_fields",
                "message": "The work item report_type must be a non-empty string.",
            }
        )

    channel = payload.get("channel", "whatsapp")
    if not isinstance(channel, str) or channel.strip().lower() not in _SUPPORTED_CHANNELS:
        errors.append(
            {
                "code": "missing_fields",
                "message": "The work item channel must be `whatsapp` for Phase 2 feedback generation.",
            }
        )

    dry_run = payload.get("dry_run", False)
    if not isinstance(dry_run, bool):
        errors.append(
            {
                "code": "missing_fields",
                "message": "The work item dry_run field must be a boolean when provided.",
            }
        )

    rejections = payload.get("rejections")
    if not isinstance(rejections, Sequence) or isinstance(rejections, (str, bytes)) or not rejections:
        errors.append(
            {
                "code": "missing_fields",
                "message": "The work item rejections field must be a non-empty list.",
            }
        )
        return errors

    for rejection in rejections:
        if not isinstance(rejection, Mapping):
            errors.append(
                {
                    "code": "missing_fields",
                    "message": "Each rejection entry must be a mapping with code and message fields.",
                }
            )
            continue
        code = rejection.get("code")
        message = rejection.get("message")
        if not isinstance(code, str) or not code.strip() or not isinstance(message, str) or not message.strip():
            errors.append(
                {
                    "code": "missing_fields",
                    "message": "Each rejection entry must include non-empty code and message values.",
                }
            )
            break

    return errors


def _normalize_rejections(rejections: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Return stable rejection payloads for formatting and storage."""

    normalized: list[dict[str, str]] = []
    for rejection in rejections:
        field = _optional_string(rejection.get("field"))
        entry = {
            "code": str(rejection.get("code")).strip(),
            "message": str(rejection.get("message")).strip(),
        }
        if field is not None:
            entry["field"] = field
        normalized.append(entry)
    return normalized


def _optional_string(value: Any) -> str | None:
    """Return a stripped string value or None."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _failure_result(warnings: list[dict[str, str]]) -> AgentResult:
    """Return one safe invalid-input result for feedback generation."""

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "status": "invalid_input",
            "feedback_message": None,
            "record": {
                "json_path": None,
                "whatsapp_preview_path": None,
            },
            "delivery": {
                "channel": "whatsapp",
                "dry_run": False,
                "dispatch_status": "not_generated",
                "dispatched": False,
            },
            "warnings": warnings,
        },
    )
