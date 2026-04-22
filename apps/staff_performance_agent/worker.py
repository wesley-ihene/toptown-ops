"""Dedicated worker for structured staff performance reports."""

from __future__ import annotations

from dataclasses import dataclass

from apps.hr_agent.compliance import evaluate_performance_compliance
from apps.hr_agent.performance import summarize_performance
from apps.hr_agent.record_store import write_structured_record
from apps.hr_agent.scoring import compute_performance_score
from apps.hr_agent.warnings import dedupe_warnings, make_warning
from apps.staff_performance_agent.parser import ParsedStaffPerformanceReport, parse_work_item
from packages.data_governance import build_governance_context
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.validation import ValidationMetadata, normalize_rejections

AGENT_NAME = "staff_performance_agent"
SIGNAL_TYPE = "hr"


@dataclass(slots=True)
class StaffPerformanceAgentWorker:
    """Specialist worker for staff performance reports."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into one structured performance result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Parse, summarize, and persist one staff performance report."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    candidate_only = _candidate_mode_requested(payload)
    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, dict) or not isinstance(raw_message.get("text"), str) or not raw_message["text"].strip():
        return _failure_result(
            "The work item raw_message.text field must be present for staff performance parsing.",
            work_item_payload=payload,
        )

    try:
        parsed = parse_work_item(work_item)
        result = build_staff_performance_result(parsed)
        result.metadata["governance_context"] = build_governance_context(payload)
    except Exception:
        result = _failure_result(
            "The staff performance report could not be parsed safely.",
            work_item_payload=payload,
            parser_failure=True,
        )

    if candidate_only:
        return result

    write_result = write_structured_record(result.payload, metadata=result.metadata)
    _apply_governance_result(result, write_result)
    return result


def build_staff_performance_result(
    parsed: ParsedStaffPerformanceReport,
    *,
    source_agent: str = AGENT_NAME,
) -> AgentResult:
    """Return one structured staff-performance result for the owning agent."""

    figures, figure_warnings = summarize_performance(
        parsed.records,
        declared_items_moved=parsed.figures.declared_items_moved,
        declared_assisting_count=parsed.figures.declared_assisting_count,
        declared_record_count=parsed.figures.declared_record_count,
    )
    compliance_warnings = evaluate_performance_compliance(parsed.records)
    warnings = dedupe_warnings(parsed.warnings + figure_warnings + compliance_warnings)
    confidence = _compute_confidence(parsed=parsed, warnings=warnings)
    status = _determine_status(parsed=parsed, warnings=warnings, confidence=confidence)

    return AgentResult(
        agent_name=source_agent,
        payload={
            "signal_type": SIGNAL_TYPE,
            "signal_subtype": "staff_performance",
            "source_agent": source_agent,
            "branch": parsed.branch_slug or parsed.branch,
            "report_date": parsed.report_date,
            "confidence": confidence,
            "metrics": {
                "total_staff_records": figures.parsed_record_count,
                "total_items_moved": figures.parsed_items_moved,
                "total_assisting_count": figures.parsed_assisting_count,
                "declared_total_staff_records": figures.declared_record_count,
                "declared_total_items_moved": figures.declared_items_moved,
                "declared_total_assisting_count": figures.declared_assisting_count,
                "price_room_staff_count": len(parsed.price_room_staff),
                "special_assignment_count": len(parsed.special_assignments),
                "resolved_section_count": parsed.diagnostics["section_resolution_stats"]["resolved_count"],
                "unresolved_section_count": parsed.diagnostics["section_resolution_stats"]["unresolved_count"],
            },
            "items": [
                {
                    "record_number": record.record_number,
                    "staff_name": record.staff_name,
                    "section": record.section,
                    "raw_section": record.raw_section,
                    "role": record.role,
                    "duty_status": record.duty_status,
                    "arrangement_grade": record.arrangement_grade,
                    "display_grade": record.display_grade,
                    "performance_grade": record.performance_grade,
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
            "price_room_staff": [
                {
                    "name": entry.name,
                    "role": entry.role,
                    "notes": entry.notes,
                }
                for entry in parsed.price_room_staff
            ],
            "special_assignments": [
                {
                    "record_number": assignment.record_number,
                    "staff_name": assignment.staff_name,
                    "role": assignment.role,
                    "assignment_type": assignment.assignment_type,
                    "pricing_by": assignment.pricing_by,
                    "items_sold": assignment.items_sold,
                    "notes": assignment.notes,
                }
                for assignment in parsed.special_assignments
            ],
            "provenance": parsed.provenance.to_payload(),
            "diagnostics": parsed.diagnostics,
            "review_policy": _review_policy_summary(status=status, warnings=warnings),
            "warnings": [warning.to_payload() for warning in warnings],
            "status": status,
        },
        metadata=_validation_metadata(
            stage=source_agent,
            status=status,
            warnings=warnings,
        ),
    )


def _failure_result(
    message: str,
    *,
    work_item_payload: dict[str, object],
    parser_failure: bool = False,
) -> AgentResult:
    """Return one invalid-input result for staff performance parsing."""

    warning = make_warning(
        code="parser_failure" if parser_failure else "missing_fields",
        severity="error",
        message=message,
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "signal_subtype": "staff_performance",
            "source_agent": AGENT_NAME,
            "branch": None,
            "report_date": None,
            "confidence": 0.0,
            "metrics": {},
            "items": [],
            "provenance": {
                "raw_branch": None,
                "raw_date": None,
                "detected_subtype": "staff_performance",
                "notes": [],
            },
            "diagnostics": {"unmatched_lines": []},
            "warnings": [warning.to_payload()],
            "status": "invalid_input",
        },
        metadata=_validation_metadata(
            stage=AGENT_NAME,
            status="invalid_input",
            warnings=[warning],
            work_item_payload=work_item_payload,
            parser_failure=parser_failure,
        ),
    )


def _compute_confidence(
    *,
    parsed: ParsedStaffPerformanceReport,
    warnings,
) -> float:
    """Return a deterministic confidence score for staff performance parsing."""

    if not parsed.branch_slug or not parsed.report_date or not parsed.records:
        return 0.0

    confidence = 0.9
    if len(parsed.records) >= 10:
        confidence += 0.05
    if parsed.price_room_staff:
        confidence += 0.02
    if parsed.special_assignments:
        confidence += 0.01

    unresolved = parsed.diagnostics["section_resolution_stats"]["unresolved_count"]
    resolved = parsed.diagnostics["section_resolution_stats"]["resolved_count"]
    if unresolved and resolved == 0:
        confidence -= 0.08
    elif unresolved:
        confidence -= min(0.05, unresolved * 0.005)

    severe_warnings = sum(1 for warning in warnings if warning.severity == "error")
    if severe_warnings:
        confidence -= 0.2

    return round(max(0.0, min(confidence, 0.99)), 2)


def _determine_status(
    *,
    parsed: ParsedStaffPerformanceReport,
    warnings,
    confidence: float,
) -> str:
    """Return the final staff performance status under the Phase 1.5 review policy."""

    if not parsed.branch_slug or not parsed.report_date or not parsed.records:
        return "invalid_input"

    critical_warning_codes = {"missing_fields", "data_mismatch", "compliance_issue"}
    if any(warning.code in critical_warning_codes and warning.severity in {"warning", "error"} for warning in warnings):
        return "needs_review"
    if confidence < 0.75:
        return "needs_review"

    noncritical_signals = bool(parsed.diagnostics["unmatched_lines"]) or bool(warnings)
    if noncritical_signals:
        return "accepted_with_warning"
    return "accepted"


def _validation_metadata(
    *,
    stage: str,
    status: str,
    warnings,
    work_item_payload: dict[str, object] | None = None,
    parser_failure: bool = False,
) -> dict[str, object]:
    """Return sidecar validation metadata for staff performance records."""

    return {
        "validation": ValidationMetadata(
            stage=stage,
            status="passed" if status != "invalid_input" else "rejected",
            accepted=status != "invalid_input",
            rejections=normalize_rejections([warning.to_payload() for warning in warnings if warning.severity == "error"]),
            details={
                "final_status": status,
                "parser_failure": parser_failure,
            },
        ).to_payload(),
        "governance_context": build_governance_context(work_item_payload or {}),
    }


def _apply_governance_result(result: AgentResult, write_result: object) -> None:
    """Project the persisted governance result back onto the live agent payload."""

    governance = getattr(write_result, "governance", None)
    if governance is None:
        return
    result.payload["status"] = governance.status
    result.payload["export_allowed"] = governance.export_allowed
    result.payload["governance"] = governance.to_payload()


def _review_policy_summary(*, status: str, warnings) -> dict[str, object]:
    """Return a small explicit summary of the review policy outcome."""

    return {
        "policy_version": "phase_1_5",
        "critical_warning_codes": ["missing_fields", "data_mismatch", "compliance_issue"],
        "warning_count": len(warnings),
        "final_status": status,
    }


def _candidate_mode_requested(payload: dict[str, object]) -> bool:
    """Return whether this worker should stop at candidate generation."""

    return payload.get("governance_mode") == "candidate"
