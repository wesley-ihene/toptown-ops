"""Worker for contract-driven HR attendance signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from apps.adaptive_sop_engine.worker import apply_adaptive_sop, sync_validation_metadata
from apps.hr_agent.alerts import generate_alerts
from apps.hr_agent.attendance import derive_attendance
from apps.hr_agent.coverage import derive_coverage
from apps.hr_agent.parser import ParsedHrReport, parse_work_item
from apps.hr_agent.record_store import write_structured_record
from apps.hr_agent.scoring import (
    attendance_validation_issue_is_blocking,
    build_attendance_accountability,
    calculate_attendance_totals,
    compute_attendance_confidence,
    resolve_attendance_validation_issue,
)
from apps.hr_agent.staffing import derive_staffing
from apps.staff_performance_agent.parser import parse_work_item as parse_staff_performance_work_item
from apps.staff_performance_agent.worker import build_staff_performance_result
from packages.common.paths import OUTBOX_DIR
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.data_governance import build_governance_context
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.validation import ValidationMetadata, normalize_rejections

AGENT_NAME = "hr_agent"
SIGNAL_TYPE = "hr_staffing"
OUTBOX_PATH = OUTBOX_DIR / AGENT_NAME
_SUPPORTED_REPORT_TYPES = {"staff_attendance", "staff_performance"}
RUNTIME_STATUS = "LIVE_RUNTIME"
RUNTIME_OWNER = "hr_agent"
RUNTIME_NOTE = (
    "Live runtime owner for both staff_attendance and routed "
    "staff_performance processing."
)


@dataclass(slots=True)
class HrAgentWorker:
    """Specialist worker for HR attendance signals."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured HR result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a structured HR attendance result without raising."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    candidate_only = _candidate_mode_requested(payload)
    try:
        report_type = _report_type(payload)
        if report_type == "staff_performance":
            return _process_staff_performance_work_item(
                work_item,
                payload=payload,
                candidate_only=candidate_only,
            )

        validation_warnings = _validate_input(payload)
        if validation_warnings:
            result = _build_failure_result(
                work_item,
                warnings=validation_warnings,
                report_type=report_type,
                work_item_payload=payload,
            )
            result.payload = apply_adaptive_sop(
                report_type="hr_attendance",
                structured_payload=result.payload,
                work_item_payload=payload,
                enabled=False,
            ).payload
            result.metadata = sync_validation_metadata(result.metadata, result.payload)
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
        warnings, insights = _partition_alerts(parsed.warnings + derived_warnings)
        warnings = _normalize_attendance_warnings(parsed=parsed, warnings=warnings)
        warning_status = _warning_status(warnings)
        confidence = _compute_confidence(parsed=parsed, warnings=warnings, status=warning_status)
        detailed_totals = calculate_attendance_totals(parsed)
        validation_issue = resolve_attendance_validation_issue(
            parsed=parsed,
            warnings=warnings,
            status=warning_status,
        )
        status = _final_status(warnings=warnings, validation_issue=validation_issue)
        accountability = _build_attendance_accountability_payload(
            parsed=parsed,
            warnings=warnings,
            status=status,
            confidence=confidence,
            report_type=report_type,
        )

        result = AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "signal_subtype": "staff_attendance",
                "source_agent": AGENT_NAME,
                "branch": parsed.branch_slug,
                "report_date": parsed.report_date,
                "confidence": confidence,
                "metrics": {
                    "total_staff_listed": staffing.total_staff_listed,
                    "present_count": attendance.present_count,
                    "present_half_count": attendance.present_half_count,
                    "absent_count": attendance.absent_count,
                    "off_count": attendance.off_count,
                    "leave_count": attendance.leave_count,
                    "present": detailed_totals["present"],
                    "present_full": detailed_totals["present_full"],
                    "present_half": detailed_totals["present_half"],
                    "effective_present": detailed_totals["effective_present"],
                    "day_off": detailed_totals["off"],
                    "leave": detailed_totals["leave"],
                    "suspend": detailed_totals["suspend"],
                    "late": detailed_totals["late"],
                    "awn": detailed_totals["awn"],
                    "awon": detailed_totals["awon"],
                    "sick": detailed_totals["sick"],
                    "lay_off": detailed_totals["lay_off"],
                    "non_active": detailed_totals["non_active"],
                    "absent": detailed_totals["absent"],
                    "transfer": detailed_totals["transfer"],
                    "total_staff": detailed_totals["total_staff"],
                    "active_count": staffing.active_count,
                    "effective_active_count": attendance.effective_present,
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
                **({"insights": [insight.to_payload() for insight in insights]} if insights else {}),
                **_validation_error_fields(validation_issue or accountability),
                **({"accountability": accountability} if accountability is not None else {}),
                "status": status,
            },
            metadata=_validation_metadata(
                status=status,
                warnings=warnings,
                report_type=report_type,
                work_item_payload=payload,
                accountability=accountability,
                validation_issue=validation_issue,
            ),
        )
        result.payload = apply_adaptive_sop(
            report_type="hr_attendance",
            structured_payload=result.payload,
            work_item_payload=payload,
            enabled=not candidate_only,
        ).payload
        result.metadata = sync_validation_metadata(result.metadata, result.payload)
        if not candidate_only:
            write_result = write_structured_record(result.payload, metadata=result.metadata)
            _apply_governance_result(result, write_result)
        _write_result_to_outbox(result)
        return result
    except Exception:
        result = _build_failure_result(
            work_item,
            report_type="staff_attendance",
            warnings=[
                make_warning(
                    code="parser_failure",
                    severity="error",
                    message="The HR report could not be processed safely.",
                )
            ],
            work_item_payload=payload,
            parser_failure=True,
        )
        result.payload = apply_adaptive_sop(
            report_type="hr_attendance",
            structured_payload=result.payload,
            work_item_payload=payload,
            enabled=not candidate_only,
        ).payload
        result.metadata = sync_validation_metadata(result.metadata, result.payload)
        _write_result_to_outbox(result)
        return result


def _process_staff_performance_work_item(
    work_item: WorkItem,
    *,
    payload: dict[str, Any],
    candidate_only: bool,
) -> AgentResult:
    """Process one staff-performance report through the HR family agent."""

    validation_warnings = _validate_input(payload)
    if validation_warnings:
        result = _build_failure_result(
            work_item,
            warnings=validation_warnings,
            report_type="staff_performance",
            work_item_payload=payload,
        )
        result.payload = apply_adaptive_sop(
            report_type="hr_attendance",
            structured_payload=result.payload,
            work_item_payload=payload,
            enabled=False,
        ).payload
        result.metadata = sync_validation_metadata(result.metadata, result.payload)
        _write_result_to_outbox(result)
        return result

    parsed = parse_staff_performance_work_item(work_item)
    result = build_staff_performance_result(parsed, source_agent=AGENT_NAME)
    result.metadata["governance_context"] = build_governance_context(payload)
    result.payload = apply_adaptive_sop(
        report_type="hr_attendance",
        structured_payload=result.payload,
        work_item_payload=payload,
        enabled=False,
    ).payload
    result.metadata = sync_validation_metadata(result.metadata, result.payload)
    if not candidate_only:
        write_result = write_structured_record(result.payload, metadata=result.metadata)
        _apply_governance_result(result, write_result)
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
    work_item_payload: Mapping[str, Any] | None = None,
    parser_failure: bool = False,
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
    accountability = _build_failure_accountability(
        parsed=parsed,
        warnings=warning_list,
        report_type=report_type,
        parser_failure=parser_failure,
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
                    "present_half_count": 0,
                    "absent_count": 0,
                    "off_count": 0,
                    "leave_count": 0,
                    "present_full": 0,
                    "present_half": 0,
                    "effective_present": 0.0,
                    "active_count": 0,
                    "effective_active_count": 0.0,
                    "coverage_ratio": 0.0,
                    "attendance_gap": 0,
                },
                "items": [],
                "provenance": {
                    "branch_text": (parsed.raw_branch or parsed.branch) if parsed is not None else None,
                    "notes": parsed.notes if parsed is not None else [],
                },
                "warnings": [warning.to_payload() for warning in warning_list],
                **_validation_error_fields(accountability),
                **({"accountability": accountability} if accountability is not None else {}),
                "status": "invalid_input",
        },
        metadata=_validation_metadata(
            status="invalid_input",
            warnings=warning_list,
            report_type=report_type,
            work_item_payload=work_item_payload or {},
            parser_failure=parser_failure,
            accountability=accountability,
            validation_issue=accountability,
        ),
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

    return compute_attendance_confidence(parsed=parsed, warnings=warnings, status=status)


def _partition_alerts(entries: list[WarningEntry]) -> tuple[list[WarningEntry], list[WarningEntry]]:
    """Split blocking warnings from informational HR insights."""

    warnings = dedupe_warnings([entry for entry in entries if entry.severity != "info"])
    insights = dedupe_warnings([entry for entry in entries if entry.severity == "info"])
    return warnings, insights


def _normalize_attendance_warnings(
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
) -> list[WarningEntry]:
    """Replace coarse grouped warnings with one exact attendance validation issue."""

    explicit_missing_codes = {warning.code for warning in warnings} & {
        "missing_branch",
        "missing_report_date",
        "no_attendance_rows",
    }
    filtered = [
        warning
        for warning in warnings
        if warning.code not in {"data_mismatch", "attendance_totals_mismatch"}
        and not (warning.code == "missing_fields" and explicit_missing_codes)
    ]
    validation_issue = resolve_attendance_validation_issue(
        parsed=parsed,
        warnings=filtered,
        status="accepted",
    )
    if validation_issue is None:
        return dedupe_warnings(filtered)
    return dedupe_warnings(
        _upsert_warning(
            filtered,
            code=validation_issue["code"],
            severity=_validation_issue_severity(validation_issue["code"]),
            message=validation_issue["message"],
        )
    )


def _upsert_warning(
    warnings: list[WarningEntry],
    *,
    code: str,
    severity: str,
    message: str,
) -> list[WarningEntry]:
    """Return warnings with one code updated or appended."""

    updated: list[WarningEntry] = []
    replaced = False
    for warning in warnings:
        if warning.code == code:
            updated.append(make_warning(code=code, severity=severity, message=message))
            replaced = True
            continue
        updated.append(warning)
    if not replaced:
        updated.append(make_warning(code=code, severity=severity, message=message))
    return updated


def _validation_issue_severity(code: str) -> str:
    """Return the warning severity for one attendance validation issue."""

    if code in {"missing_branch", "missing_report_date", "no_attendance_rows", "parser_failure", "missing_fields"}:
        return "error"
    return "warning"


def _warning_status(warnings: list[WarningEntry]) -> str:
    """Return the provisional status implied by warning severities alone."""

    if any(warning.severity == "error" for warning in warnings):
        return "invalid_input"
    if any(warning.severity == "warning" for warning in warnings):
        return "needs_review"
    return "accepted"


def _final_status(
    *,
    warnings: list[WarningEntry],
    validation_issue: Mapping[str, Any] | None,
) -> str:
    """Return the final HR result status using explicit blocking validation rules."""

    if any(warning.severity == "error" for warning in warnings):
        return "invalid_input"
    if attendance_validation_issue_is_blocking(validation_issue):
        return "needs_review"
    return "accepted"


def _validation_metadata(
    *,
    status: str,
    warnings: list[WarningEntry],
    report_type: str,
    work_item_payload: Mapping[str, Any],
    parser_failure: bool = False,
    accountability: Mapping[str, Any] | None = None,
    validation_issue: Mapping[str, Any] | None = None,
) -> dict[str, object]:
    """Return sidecar validation metadata for HR-family records."""

    details: dict[str, Any] = {
        "final_status": status,
        "report_type": report_type,
        "parser_failure": parser_failure,
    }
    if accountability is not None:
        details["accountability"] = dict(accountability)
    validation_error = validation_issue or accountability
    if isinstance(validation_error, Mapping):
        validation_error_code = validation_error.get("validation_error_code") or validation_error.get("code")
        validation_error_message = validation_error.get("validation_error_message") or validation_error.get("message")
        if isinstance(validation_error_code, str) and validation_error_code.strip():
            details["validation_error_code"] = validation_error_code.strip()
        if isinstance(validation_error_message, str) and validation_error_message.strip():
            details["validation_error_message"] = validation_error_message.strip()
    return {
        "validation": ValidationMetadata(
            stage=AGENT_NAME,
            status="passed" if status != "invalid_input" else "rejected",
            accepted=status != "invalid_input",
            rejections=normalize_rejections([warning.to_payload() for warning in warnings if warning.severity == "error"]),
            details=details,
        ).to_payload(),
        "governance_context": build_governance_context(work_item_payload),
    }


def _candidate_mode_requested(payload: Mapping[str, Any]) -> bool:
    """Return whether this worker should stop at candidate generation."""

    return payload.get("governance_mode") == "candidate"


def _apply_governance_result(result: AgentResult, write_result: object) -> None:
    """Project the persisted governance result back onto the live agent payload."""

    governance = getattr(write_result, "governance", None)
    if governance is None:
        return
    result.payload["status"] = governance.status
    result.payload["export_allowed"] = governance.export_allowed
    result.payload["governance"] = governance.to_payload()


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


def _build_attendance_accountability_payload(
    *,
    parsed: ParsedHrReport,
    warnings: list[WarningEntry],
    status: str,
    confidence: float,
    report_type: str,
) -> dict[str, Any] | None:
    """Return attendance accountability only for review or rejection outputs."""

    if report_type != "staff_attendance" or status not in {"needs_review", "invalid_input"}:
        return None
    return build_attendance_accountability(
        parsed=parsed,
        warnings=warnings,
        status=status,
        confidence=confidence,
    )


def _build_failure_accountability(
    *,
    parsed: ParsedHrReport | None,
    warnings: list[WarningEntry],
    report_type: str,
    parser_failure: bool,
) -> dict[str, Any] | None:
    """Return an accountability payload for failure results when possible."""

    if report_type != "staff_attendance":
        return None
    if parsed is not None:
        return build_attendance_accountability(
            parsed=parsed,
            warnings=warnings,
            status="invalid_input",
            confidence=0.0,
        )
    failing_rule = "parser_failure" if parser_failure else "missing_input_contract_fields"
    recommended_correction = (
        "Resend the attendance report in plain text with clear branch, date, and staff lines."
        if parser_failure
        else "Provide a non-empty raw attendance message with classification, branch, date, and staff lines."
    )
    return {
        "failing_layer": "hr_parser" if parser_failure else "hr_input_contract",
        "failing_rule": failing_rule,
        "validation_error_code": failing_rule,
        "validation_error_message": "The attendance parser failed safely." if parser_failure else "The attendance work item was incomplete.",
        "normalized_values_attempted": [],
        "calculated_totals": {},
        "declared_totals": {},
        "final_confidence_score": 0.0,
        "reason_detail": "The attendance work item was incomplete." if not parser_failure else "The attendance parser failed safely.",
        "recommended_correction": recommended_correction,
    }


def _validation_error_fields(source: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return exact validation error fields for review/rejection payloads."""

    if not isinstance(source, Mapping):
        return {}

    code = source.get("validation_error_code") or source.get("failing_rule") or source.get("code")
    message = source.get("validation_error_message") or source.get("reason_detail") or source.get("message")
    payload: dict[str, Any] = {}
    if isinstance(code, str) and code.strip():
        payload["validation_error_code"] = code.strip()
    if isinstance(message, str) and message.strip():
        payload["validation_error_message"] = message.strip()
    return payload
