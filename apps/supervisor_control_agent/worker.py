"""Worker for contract-driven supervisor control signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from apps.supervisor_control_agent.alerts import generate_alerts
from apps.supervisor_control_agent.exceptions import derive_exceptions
from apps.supervisor_control_agent.controls import derive_controls
from apps.supervisor_control_agent.escalation import derive_escalation
from apps.supervisor_control_agent.parser import ParsedSupervisorControlReport, parse_work_item
from apps.supervisor_control_agent.record_store import write_structured_record
from packages.common.paths import OUTBOX_DIR
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "supervisor_control_agent"
SIGNAL_TYPE = "supervisor_control"
SIGNAL_WEIGHT = 0.4
OUTBOX_PATH = OUTBOX_DIR / AGENT_NAME
_SUPPORTED_REPORT_TYPES = {"supervisor_control"}


@dataclass(slots=True)
class SupervisorControlAgentWorker:
    """Specialist worker for supervisor control signals."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured supervisor control result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a structured supervisor-control result without raising."""

    try:
        payload = work_item.payload if isinstance(work_item.payload, dict) else {}
        source = _source_trace(payload)
        validation_warnings = _validate_input(payload)
        if validation_warnings:
            result = _build_failure_result(work_item, warnings=validation_warnings, source=source)
            write_structured_record(result.payload)
            _write_result_to_outbox(result)
            return result

        parsed = parse_work_item(work_item)
        exceptions = derive_exceptions(parsed)
        controls = derive_controls(exceptions)
        escalation = derive_escalation(exceptions)
        warnings = dedupe_warnings(
            parsed.warnings + generate_alerts(parsed=parsed, exceptions=exceptions, controls=controls, escalation=escalation)
        )

        if any(warning.severity == "error" for warning in warnings):
            status = "invalid_input"
        else:
            status = "ready" if not warnings else "needs_review"

        result = AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "source_agent": AGENT_NAME,
                "source": source,
                "branch": parsed.branch_slug or parsed.branch,
                "report_date": parsed.report_date,
                "sop_compliance": parsed.sop_compliance,
                "signal_weight": SIGNAL_WEIGHT,
                "confidence": _compute_confidence(parsed=parsed, warnings=warnings, status=status),
                "metrics": {
                    "exception_count": exceptions.exception_count,
                    "open_exception_count": exceptions.open_exception_count,
                    "escalated_count": escalation.escalated_count,
                    "confirmed_count": controls.confirmed_count,
                    "control_gap_count": controls.control_gap_count,
                },
                "items": [item.to_payload() for item in exceptions.items],
                "provenance": {
                    "branch_text": parsed.branch,
                    "notes": parsed.notes,
                },
                "warnings": [warning.to_payload() for warning in warnings],
                "status": status,
            },
        )
        write_structured_record(result.payload)
        _write_result_to_outbox(result)
        return result
    except Exception:
        result = _build_failure_result(
            work_item,
            warnings=[
                make_warning(
                    code="missing_fields",
                    severity="error",
                    message="The work item could not be processed safely.",
                )
            ],
            source=source,
        )
        write_structured_record(result.payload)
        _write_result_to_outbox(result)
        return result


def _validate_input(payload: dict[str, Any]) -> list[WarningEntry]:
    """Validate the strict input contract for routed supervisor-control items."""

    warnings: list[WarningEntry] = []
    classification = payload.get("classification")
    raw_message = payload.get("raw_message")

    if not isinstance(classification, Mapping) or classification.get("report_type") not in _SUPPORTED_REPORT_TYPES:
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item classification must be `supervisor_control`.",
            )
        )

    if not isinstance(raw_message, Mapping):
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item raw_message must be a mapping with a `text` field.",
            )
        )
    else:
        text = raw_message.get("text")
        if not isinstance(text, str) or not text.strip():
            warnings.append(
                make_warning(
                    code="missing_fields",
                    severity="error",
                    message="The work item raw_message.text field must be a non-empty string.",
                )
            )

    return dedupe_warnings(warnings)


def _build_failure_result(
    work_item: WorkItem,
    *,
    parsed: ParsedSupervisorControlReport | None = None,
    warnings: list[WarningEntry] | None = None,
    source: str = "live",
) -> AgentResult:
    """Return a safe failure result that still matches the output contract."""

    del work_item
    warning_list = dedupe_warnings(
        warnings
        or [
            make_warning(
                code="missing_fields",
                severity="error",
                message="The supervisor control input was incomplete or invalid.",
            )
        ]
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "source": source,
            "branch": parsed.branch_slug if parsed is not None else None,
            "report_date": parsed.report_date if parsed is not None else None,
            "sop_compliance": parsed.sop_compliance if parsed is not None else "strict",
            "signal_weight": SIGNAL_WEIGHT,
            "confidence": 0.0,
            "metrics": {
                "exception_count": 0,
                "open_exception_count": 0,
                "escalated_count": 0,
                "confirmed_count": 0,
                "control_gap_count": 0,
            },
            "items": [],
            "provenance": {
                "branch_text": parsed.branch if parsed is not None else None,
                "notes": parsed.notes if parsed is not None else [],
            },
            "warnings": [warning.to_payload() for warning in warning_list],
            "status": "invalid_input",
        },
    )


def _source_trace(payload: dict[str, Any]) -> str:
    """Return whether this worker invocation came from live intake or replay."""

    replay = payload.get("replay")
    if isinstance(replay, Mapping) and replay:
        return "replay"
    return "live"


def _compute_confidence(
    *,
    parsed: ParsedSupervisorControlReport,
    warnings: list[WarningEntry],
    status: str,
) -> float:
    """Return a conservative confidence score for the structured result."""

    if status == "invalid_input":
        return 0.0

    confidence = 1.0
    if not parsed.branch_slug:
        confidence -= 0.15
    if not parsed.report_date:
        confidence -= 0.15
    if not parsed.exception_entries:
        confidence -= 0.35

    penalties = {
        "missing_fields": 0.25,
        "unknown_exception_type": 0.1,
        "missing_confirmation": 0.1,
        "escalation_required": 0.15,
        "control_gap_present": 0.1,
    }
    for warning in warnings:
        confidence -= penalties.get(warning.code, 0.0)

    return round(max(confidence, 0.0), 2)


def _write_result_to_outbox(result: AgentResult) -> Path:
    """Persist the supervisor-control payload to the agent outbox."""

    OUTBOX_PATH.mkdir(parents=True, exist_ok=True)
    output_path = OUTBOX_PATH / _build_output_filename(result.payload)
    temp_path = output_path.with_suffix(".json.tmp")
    temp_path.write_text(
        json.dumps(result.payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(output_path)
    return output_path


def _build_output_filename(payload: dict[str, Any]) -> str:
    """Return a stable outbox filename for a supervisor-control payload."""

    branch = str(payload.get("branch") or "unknown").strip() or "unknown"
    report_date = str(payload.get("report_date") or datetime.now(timezone.utc).date().isoformat()).strip()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_branch = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in branch)
    safe_date = "".join(character if character.isdigit() or character == "-" else "_" for character in report_date)
    return f"{timestamp}__{safe_branch}__{safe_date}__supervisor_control.json"
