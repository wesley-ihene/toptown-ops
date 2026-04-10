"""Worker for contract-driven HR attendance signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from apps.hr_agent.alerts import generate_alerts
from apps.hr_agent.attendance import derive_attendance
from apps.hr_agent.coverage import derive_coverage
from apps.hr_agent.parser import ParsedHrReport, parse_work_item
from apps.hr_agent.record_store import write_structured_record
from apps.hr_agent.staffing import derive_staffing
from apps.staff_performance_agent.parser import parse_work_item as parse_staff_performance_work_item
from apps.staff_performance_agent.worker import build_staff_performance_result
from packages.common.paths import OUTBOX_DIR
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "hr_agent"
SIGNAL_TYPE = "hr_staffing"
OUTBOX_PATH = OUTBOX_DIR / AGENT_NAME
_SUPPORTED_REPORT_TYPES = {"staff_attendance", "staff_performance"}


@dataclass(slots=True)
class HrAgentWorker:
    """Specialist worker for HR attendance signals."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured HR result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a structured HR attendance result without raising."""

    try:
        payload = work_item.payload if isinstance(work_item.payload, dict) else {}
        report_type = _report_type(payload)
        if report_type == "staff_performance":
            return _process_staff_performance_work_item(work_item, payload=payload)

        validation_warnings = _validate_input(payload)
        if validation_warnings:
            result = _build_failure_result(
                work_item,
                warnings=validation_warnings,
                report_type=report_type,
            )
            _write_result_to_outbox(result)
            return result

        parsed = parse_work_item(work_item)
        attendance = derive_attendance(parsed)
        staffing = derive_staffing(attendance, declared_total_staff=parsed.declared_total_staff)
        coverage = derive_coverage(staffing)
        derived_warnings = generate_alerts(
            parsed=parsed,
            attendance=attendance,
            staffing=staffing,
            coverage=coverage,
        )
        warnings = dedupe_warnings(parsed.warnings + derived_warnings)
        if any(warning.severity == "error" for warning in warnings):
            status = "invalid_input"
        else:
            status = "ready" if not warnings else "needs_review"

        result = AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "signal_subtype": "staff_attendance",
                "source_agent": AGENT_NAME,
                "branch": parsed.branch_slug,
                "report_date": parsed.report_date,
                "confidence": _compute_confidence(parsed=parsed, warnings=warnings, status=status),
                "metrics": {
                    "total_staff_listed": staffing.total_staff_listed,
                    "present_count": attendance.present_count,
                    "absent_count": attendance.absent_count,
                    "off_count": attendance.off_count,
                    "leave_count": attendance.leave_count,
                    "active_count": staffing.active_count,
                    "coverage_ratio": coverage.coverage_ratio,
                    "attendance_gap": staffing.attendance_gap,
                },
                "items": [
                    {
                        "staff_name": record.staff_name,
                        "status": record.status,
                    }
                    for record in parsed.records
                ],
                "provenance": {
                    "branch_text": parsed.raw_branch or parsed.branch,
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
            report_type="staff_attendance",
            warnings=[
                make_warning(
                    code="missing_fields",
                    severity="error",
                    message="The work item could not be processed safely.",
                )
            ],
        )
        _write_result_to_outbox(result)
        return result


def _process_staff_performance_work_item(
    work_item: WorkItem,
    *,
    payload: dict[str, Any],
) -> AgentResult:
    """Process one staff-performance report through the HR family agent."""

    validation_warnings = _validate_input(payload)
    if validation_warnings:
        result = _build_failure_result(
            work_item,
            warnings=validation_warnings,
            report_type="staff_performance",
        )
        _write_result_to_outbox(result)
        return result

    parsed = parse_staff_performance_work_item(work_item)
    result = build_staff_performance_result(parsed, source_agent=AGENT_NAME)
    write_structured_record(result.payload)
    _write_result_to_outbox(result)
    return result


def _validate_input(payload: dict[str, Any]) -> list[WarningEntry]:
    """Validate the strict input contract for routed HR attendance items."""

    warnings: list[WarningEntry] = []
    classification = payload.get("classification")
    raw_message = payload.get("raw_message")

    if not isinstance(classification, Mapping) or classification.get("report_type") not in _SUPPORTED_REPORT_TYPES:
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item classification must be `staff_attendance`.",
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
    parsed: ParsedHrReport | None = None,
    report_type: str = "staff_attendance",
    warnings: list[WarningEntry] | None = None,
) -> AgentResult:
    """Return a safe failure result that still matches the output contract."""

    del work_item
    warning_list = dedupe_warnings(
        warnings
        or [
            make_warning(
                code="missing_fields",
                severity="error",
                message="The HR attendance input was incomplete or invalid.",
            )
        ]
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
                "signal_type": "hr" if report_type == "staff_performance" else SIGNAL_TYPE,
                "signal_subtype": report_type,
                "source_agent": AGENT_NAME,
                "branch": parsed.branch_slug if parsed is not None else None,
                "report_date": parsed.report_date if parsed is not None else None,
                "confidence": 0.0,
                "metrics": {
                    "total_staff_listed": 0,
                    "present_count": 0,
                    "absent_count": 0,
                    "off_count": 0,
                    "leave_count": 0,
                    "active_count": 0,
                    "coverage_ratio": 0.0,
                    "attendance_gap": 0,
                },
                "items": [],
                "provenance": {
                    "branch_text": (parsed.raw_branch or parsed.branch) if parsed is not None else None,
                    "notes": parsed.notes if parsed is not None else [],
                },
                "warnings": [warning.to_payload() for warning in warning_list],
                "status": "invalid_input",
        },
    )


def _report_type(payload: dict[str, Any]) -> str:
    """Return the classified HR-family report type or `staff_attendance` by default."""

    classification = payload.get("classification")
    if isinstance(classification, Mapping):
        report_type = classification.get("report_type")
        if isinstance(report_type, str) and report_type in _SUPPORTED_REPORT_TYPES:
            return report_type
    return "staff_attendance"


def _compute_confidence(
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
    status: str,
) -> float:
    """Return a conservative confidence score for the structured result."""

    if status == "invalid_input":
        return 0.0

    confidence = 1.0
    if not parsed.branch_slug:
        confidence -= 0.2
    if not parsed.report_date:
        confidence -= 0.2
    if not parsed.records and not parsed.declared_status_totals:
        confidence -= 0.4
    elif not parsed.records:
        confidence -= 0.1

    penalties = {
        "missing_fields": 0.25,
        "data_mismatch": 0.15,
        "low_coverage": 0.15,
        "unknown_attendance_status": 0.1,
        "attendance_gap_present": 0.1,
    }
    for warning in warnings:
        confidence -= penalties.get(warning.code, 0.0)

    return round(max(confidence, 0.0), 2)


def _write_result_to_outbox(result: AgentResult) -> Path:
    """Persist the agent result payload to the HR outbox."""

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
    """Return a stable outbox filename for an HR payload."""

    branch = str(payload.get("branch") or "unknown").strip() or "unknown"
    report_date = str(payload.get("report_date") or datetime.now(timezone.utc).date().isoformat()).strip()
    signal_subtype = str(payload.get("signal_subtype") or "staff_attendance").strip() or "staff_attendance"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_branch = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in branch)
    safe_date = "".join(character if character.isdigit() or character == "-" else "_" for character in report_date)
    safe_subtype = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in signal_subtype
    )
    return f"{timestamp}__{safe_branch}__{safe_date}__{safe_subtype}.json"
