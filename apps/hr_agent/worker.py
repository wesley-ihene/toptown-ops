"""Worker orchestrator for HR specialist processing."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from apps.hr_agent.attendance import summarize_attendance
from apps.hr_agent.compliance import (
    evaluate_attendance_compliance,
    evaluate_performance_compliance,
)
from apps.hr_agent.confidence import compute_confidence
from apps.hr_agent.parser import (
    ParsedStaffAttendanceReport,
    ParsedStaffPerformanceReport,
    detect_report_subtype,
    parse_staff_attendance,
    parse_staff_performance,
)
from apps.hr_agent.performance import summarize_performance
from apps.hr_agent.record_store import write_structured_record
from apps.hr_agent.scoring import attendance_presence_score, compute_performance_score
from apps.hr_agent.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "hr_agent"
SIGNAL_TYPE = "hr"


@dataclass(slots=True)
class HrAgentWorker:
    """Specialist worker for WhatsApp HR reports."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured HR signal."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a structured HR result without depending on external setup."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    validation_warnings = _validate_input(payload)
    if validation_warnings:
        return _failure_result(warnings=validation_warnings)

    classification = payload.get("classification")
    subtype = detect_report_subtype(payload, classification=classification)
    if subtype == "staff_performance":
        result = _build_performance_result(parse_staff_performance(work_item))
        write_structured_record(result.payload)
        return result
    if subtype == "staff_attendance":
        result = _build_attendance_result(parse_staff_attendance(work_item))
        write_structured_record(result.payload)
        return result

    return _failure_result(
        warnings=[
            make_warning(
                code="missing_fields",
                severity="error",
                message="The HR report subtype could not be safely determined from the raw text.",
            )
        ]
    )


def _validate_input(payload: dict[str, object]) -> list[WarningEntry]:
    """Validate only the strict upstream raw-message requirement."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping) or not isinstance(raw_message.get("text"), str):
        return [
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item raw_message.text field must be present for HR parsing.",
            )
        ]
    if not raw_message["text"].strip():
        return [
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item raw_message.text field must be a non-empty string.",
            )
        ]
    return []


def _build_performance_result(parsed: ParsedStaffPerformanceReport) -> AgentResult:
    """Build the structured staff-performance signal payload."""

    figures, figure_warnings = summarize_performance(
        parsed.records,
        declared_items_moved=parsed.figures.declared_items_moved,
        declared_assisting_count=parsed.figures.declared_assisting_count,
        declared_record_count=parsed.figures.declared_record_count,
    )
    compliance_warnings = evaluate_performance_compliance(parsed.records)
    warnings = dedupe_warnings(parsed.warnings + figure_warnings + compliance_warnings)
    status = "ready" if not warnings else "needs_review"

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "signal_subtype": "staff_performance",
            "source_agent": AGENT_NAME,
            "branch": parsed.branch_slug or parsed.branch,
            "report_date": parsed.report_date,
            "confidence": compute_confidence(
                branch=parsed.branch_slug or parsed.branch,
                report_date=parsed.report_date,
                record_count=len(parsed.records),
                warnings=warnings,
            ),
            "metrics": {
                "total_staff_records": figures.parsed_record_count,
                "total_items_moved": figures.parsed_items_moved,
                "total_assisting_count": figures.parsed_assisting_count,
                "declared_total_staff_records": figures.declared_record_count,
                "declared_total_items_moved": figures.declared_items_moved,
                "declared_total_assisting_count": figures.declared_assisting_count,
            },
            "items": [
                {
                    "record_number": record.record_number,
                    "staff_name": record.staff_name,
                    "section": record.section,
                    "raw_section": record.raw_section,
                    "role": record.role,
                    "duty_status": record.duty_status,
                    "items_moved": record.items_moved,
                    "assisting_count": record.assisting_count,
                    "activity_score": compute_performance_score(
                        items_moved=record.items_moved,
                        assisting_count=record.assisting_count,
                    ),
                    "notes": record.notes,
                }
                for record in parsed.records
            ],
            "provenance": parsed.provenance.to_payload(),
            "warnings": [warning.to_payload() for warning in warnings],
            "status": status,
        },
    )


def _build_attendance_result(parsed: ParsedStaffAttendanceReport) -> AgentResult:
    """Build the structured staff-attendance signal payload."""

    figures, figure_warnings = summarize_attendance(
        parsed.records,
        declared_status_totals=parsed.figures.declared_status_totals,
    )
    compliance_warnings = evaluate_attendance_compliance(parsed.records)
    warnings = dedupe_warnings(parsed.warnings + figure_warnings + compliance_warnings)
    status = "ready" if not warnings else "needs_review"

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "signal_subtype": "staff_attendance",
            "source_agent": AGENT_NAME,
            "branch": parsed.branch_slug or parsed.branch,
            "report_date": parsed.report_date,
            "confidence": compute_confidence(
                branch=parsed.branch_slug or parsed.branch,
                report_date=parsed.report_date,
                record_count=len(parsed.records),
                warnings=warnings,
            ),
            "metrics": {
                "total_staff_records": figures.parsed_record_count,
                "present_count": figures.parsed_status_totals["present"],
                "off_count": figures.parsed_status_totals["off"],
                "annual_leave_count": figures.parsed_status_totals["annual_leave"],
                "suspended_count": figures.parsed_status_totals["suspended"],
                "absent_count": figures.parsed_status_totals["absent"],
                "sick_count": figures.parsed_status_totals["sick"],
                "declared_status_totals": figures.declared_status_totals,
            },
            "items": [
                {
                    "record_number": record.record_number,
                    "staff_name": record.staff_name,
                    "status": record.status,
                    "raw_status": record.raw_status,
                    "section": record.section,
                    "raw_section": record.raw_section,
                    "presence_score": attendance_presence_score(record.status),
                }
                for record in parsed.records
            ],
            "provenance": parsed.provenance.to_payload(),
            "warnings": [warning.to_payload() for warning in warnings],
            "status": status,
        },
    )


def _failure_result(*, warnings: list[WarningEntry]) -> AgentResult:
    """Return a safe structured failure result for HR parsing."""

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "signal_subtype": None,
            "source_agent": AGENT_NAME,
            "branch": None,
            "report_date": None,
            "confidence": 0.0,
            "metrics": {},
            "items": [],
            "provenance": {
                "raw_branch": None,
                "raw_date": None,
                "detected_subtype": None,
                "notes": [],
            },
            "warnings": [warning.to_payload() for warning in dedupe_warnings(warnings)],
            "status": "invalid_input",
        },
    )
