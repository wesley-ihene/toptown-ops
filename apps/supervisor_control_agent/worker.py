"""Minimal deterministic worker for supervisor control reports."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from apps.branch_resolver_agent.worker import resolve_branch
from apps.date_resolver_agent.worker import resolve_report_date
from apps.header_normalizer_agent.worker import normalize_headers
from apps.supervisor_control_agent.record_store import write_structured_record
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "supervisor_control_agent"
SIGNAL_TYPE = "supervisor_control"


@dataclass(slots=True)
class SupervisorControlAgentWorker:
    """Specialist worker for supervisor control reports."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured supervisor control result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Parse, summarize, and persist one supervisor control report."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_message = payload.get("raw_message")
    raw_text = _raw_text(raw_message)
    if not raw_text:
        return _failure_result("The work item raw_message.text field must be present for supervisor control parsing.")

    header_result = normalize_headers(raw_text)
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    branch_resolution = resolve_branch(
        header_result,
        metadata_branch_hint=metadata.get("branch_hint") if isinstance(metadata.get("branch_hint"), str) else None,
    )
    date_resolution = resolve_report_date(header_result)

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    body_lines = _body_lines(lines)
    key_values: dict[str, str] = {}
    checklist: list[str] = []
    notes: list[str] = []
    for line in body_lines:
        if ":" in line:
            key, value = line.split(":", 1)
            cleaned_key = key.strip()
            cleaned_value = value.strip()
            if cleaned_key and cleaned_value:
                key_values[cleaned_key] = cleaned_value
                continue
        if line.startswith(("-", "*")):
            checklist.append(line.lstrip("-* ").strip())
        else:
            notes.append(line)

    status = "accepted"
    warnings: list[dict[str, str]] = []
    if branch_resolution.branch_hint is None or date_resolution.iso_date is None:
        status = "needs_review"
        warnings.append(
            {
                "code": "missing_fields",
                "severity": "warning",
                "message": "Branch or report date could not be fully resolved from the supervisor control report.",
            }
        )
    if not body_lines:
        status = "needs_review"
        warnings.append(
            {
                "code": "missing_fields",
                "severity": "warning",
                "message": "No structured supervisor control body lines were extracted.",
            }
        )

    result = AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "branch": branch_resolution.branch_hint,
            "report_date": date_resolution.iso_date,
            "confidence": 0.9 if status == "accepted" else 0.7,
            "metrics": {
                "checklist_count": len(checklist),
                "key_value_count": len(key_values),
                "note_count": len(notes),
            },
            "items": checklist,
            "checklist": checklist,
            "key_values": key_values,
            "notes": notes,
            "provenance": {
                "raw_branch": branch_resolution.raw_branch_line,
                "raw_date": date_resolution.raw_date,
                "detected_subtype": "supervisor_control",
                "notes": list(branch_resolution.evidence) + list(date_resolution.evidence),
            },
            "warnings": warnings,
            "status": status,
        },
    )
    write_structured_record(result.payload)
    return result


def _raw_text(raw_message: object) -> str:
    """Return raw text from a work item raw-message payload."""

    if isinstance(raw_message, Mapping):
        value = raw_message.get("text")
        if isinstance(value, str):
            return value.strip()
    if isinstance(raw_message, str):
        return raw_message.strip()
    return ""


def _body_lines(lines: list[str]) -> list[str]:
    """Return body lines after the first explicit supervisor control title."""

    start_index = 0
    for index, line in enumerate(lines):
        lowered = line.casefold()
        if "supervisor control" in lowered or "supervisor report" in lowered:
            start_index = index + 1
            break
    return lines[start_index:]


def _failure_result(message: str) -> AgentResult:
    """Return one invalid-input result for supervisor control parsing."""

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "branch": None,
            "report_date": None,
            "confidence": 0.0,
            "metrics": {},
            "items": [],
            "warnings": [
                {
                    "code": "missing_fields",
                    "severity": "error",
                    "message": message,
                }
            ],
            "status": "invalid_input",
        },
    )
