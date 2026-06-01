"""Worker for contract-driven supervisor control signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any

from apps.adaptive_sop_engine.worker import apply_adaptive_sop, sync_validation_metadata
from apps.supervisor_control_agent.alerts import generate_alerts
from apps.supervisor_control_agent.exceptions import derive_exceptions
from apps.supervisor_control_agent.controls import derive_controls
from apps.supervisor_control_agent.escalation import derive_escalation
from apps.supervisor_control_agent.parser import ParsedSupervisorControlReport, parse_work_item
from apps.supervisor_control_agent.record_store import write_structured_record
from packages.common.paths import OUTBOX_DIR
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.data_governance import build_governance_context
from packages.record_store.intelligence_store import build_supervisor_control_intelligence_record
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.validation import ValidationMetadata, normalize_rejections

AGENT_NAME = "supervisor_control_agent"
SIGNAL_TYPE = "supervisor_control"
SIGNAL_WEIGHT = 0.4
OUTBOX_PATH = OUTBOX_DIR / AGENT_NAME
_SUPPORTED_REPORT_TYPES = {"supervisor_control"}
LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class SupervisorControlAgentWorker:
    """Specialist worker for supervisor control signals."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured supervisor control result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a structured supervisor-control result without raising."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    candidate_only = _candidate_mode_requested(payload)
    source = _source_trace(payload)
    governance_context = build_governance_context(payload)
    try:
        validation_warnings = _validate_input(payload)
        if validation_warnings:
            result = _build_failure_result(
                work_item,
                warnings=validation_warnings,
                source=source,
                work_item_payload=payload,
                governance_context=governance_context,
            )
            result.payload = apply_adaptive_sop(
                report_type=SIGNAL_TYPE,
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

        parsed = _apply_routing_fallbacks(parse_work_item(work_item), payload)
        exceptions = derive_exceptions(parsed)
        controls = derive_controls(exceptions)
        escalation = derive_escalation(exceptions)
        warnings = dedupe_warnings(
            parsed.warnings
            + _routing_context_warnings(parsed=parsed, payload=payload)
            + generate_alerts(parsed=parsed, exceptions=exceptions, controls=controls, escalation=escalation)
        )

        if any(warning.severity == "error" for warning in warnings):
            status = "invalid_input"
        else:
            status = "accepted" if not warnings else "needs_review"

        confidence = _compute_confidence(parsed=parsed, warnings=warnings, status=status)
        result = AgentResult(
            agent_name=AGENT_NAME,
            payload=build_supervisor_control_intelligence_record(
                branch=parsed.branch_slug or parsed.branch,
                report_date=parsed.report_date,
                supervisor=parsed.supervisor,
                supervisor_confirmation=parsed.supervisor_confirmation,
                raw_text=_raw_text_from_payload(payload),
                confidence=confidence,
                source_message_id=_string_or_none(governance_context.get("message_id")),
                sender_phone=_string_or_none(governance_context.get("sender_phone")),
                created_at=_record_created_at(payload),
                source_agent=AGENT_NAME,
                source=source,
                signal_weight=SIGNAL_WEIGHT,
                sop_compliance=parsed.sop_compliance,
                status=status,
                metrics={
                    "exception_count": exceptions.exception_count,
                    "open_exception_count": exceptions.open_exception_count,
                    "escalated_count": escalation.escalated_count,
                    "confirmed_count": controls.confirmed_count,
                    "control_gap_count": controls.control_gap_count,
                },
                items=[item.to_payload() for item in exceptions.items],
                notes=parsed.notes,
                warnings=[warning.to_payload() for warning in warnings],
                branch_text=parsed.branch,
            ),
            metadata=_validation_metadata(
                status=status,
                warnings=warnings,
                source=source,
                governance_context=governance_context,
            ),
        )
        _emit_intelligence_extracted_log(result)
        result.payload = apply_adaptive_sop(
            report_type=SIGNAL_TYPE,
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
            warnings=[
                make_warning(
                    code="parser_failure",
                    severity="error",
                    message="The supervisor control report could not be parsed safely.",
                )
            ],
            source=source,
            work_item_payload=payload,
            governance_context=governance_context,
            parser_failure=True,
        )
        result.payload = apply_adaptive_sop(
            report_type=SIGNAL_TYPE,
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
    work_item_payload: Mapping[str, Any] | None = None,
    governance_context: Mapping[str, Any] | None = None,
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
                message="The supervisor control input was incomplete or invalid.",
            )
        ]
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload=build_supervisor_control_intelligence_record(
            branch=parsed.branch_slug if parsed is not None else None,
            report_date=parsed.report_date if parsed is not None else None,
            supervisor=parsed.supervisor if parsed is not None else None,
            supervisor_confirmation=parsed.supervisor_confirmation if parsed is not None else None,
            raw_text=_raw_text_from_payload(work_item_payload or {}),
            confidence=0.0,
            source_message_id=_string_or_none((governance_context or {}).get("message_id")),
            sender_phone=_string_or_none((governance_context or {}).get("sender_phone")),
            created_at=_record_created_at(work_item_payload or {}),
            source_agent=AGENT_NAME,
            source=source,
            signal_weight=SIGNAL_WEIGHT,
            sop_compliance=parsed.sop_compliance if parsed is not None else "strict",
            status="invalid_input",
            metrics={
                "exception_count": 0,
                "open_exception_count": 0,
                "escalated_count": 0,
                "confirmed_count": 0,
                "control_gap_count": 0,
            },
            items=[],
            notes=parsed.notes if parsed is not None else [],
            warnings=[warning.to_payload() for warning in warning_list],
            branch_text=parsed.branch if parsed is not None else None,
        ),
        metadata=_validation_metadata(
            status="invalid_input",
            warnings=warning_list,
            source=source,
            governance_context=governance_context or {},
            parser_failure=parser_failure,
        ),
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


def _apply_routing_fallbacks(
    parsed: ParsedSupervisorControlReport,
    payload: Mapping[str, Any],
) -> ParsedSupervisorControlReport:
    """Fill branch/date from routed context when the raw intelligence text omits them."""

    routing = payload.get("routing")
    metadata = payload.get("metadata")

    if parsed.branch_slug is None:
        branch_hint = None
        if isinstance(routing, Mapping):
            branch_hint = _string_or_none(routing.get("branch_hint"))
        if branch_hint is None and isinstance(metadata, Mapping):
            branch_hint = _string_or_none(metadata.get("branch_hint"))
        if branch_hint is not None:
            parsed.branch = parsed.branch or branch_hint
            parsed.branch_slug = branch_hint

    if parsed.report_date is None and isinstance(routing, Mapping):
        for field_name in ("report_date", "normalized_report_date", "raw_report_date"):
            candidate = _string_or_none(routing.get(field_name))
            if candidate is not None:
                parsed.report_date = candidate
                break

    return parsed


def _routing_context_warnings(
    *,
    parsed: ParsedSupervisorControlReport,
    payload: Mapping[str, Any],
) -> list[WarningEntry]:
    """Return non-blocking intelligence warnings derived from routed context."""

    routing = payload.get("routing")
    if not isinstance(routing, Mapping):
        return []

    routed_report_date = None
    for field_name in ("report_date", "normalized_report_date"):
        candidate = _string_or_none(routing.get(field_name))
        if candidate is not None:
            routed_report_date = candidate
            break

    if parsed.report_date is None or routed_report_date is None or parsed.report_date == routed_report_date:
        return []

    return [
        make_warning(
            code="supervisor_control_date_mismatch",
            severity="warning",
            message=(
                "Supervisor control date "
                f"{parsed.report_date} differs from the routed message date {routed_report_date}; "
                "stored as intelligence without blocking transactional processing."
            ),
        )
    ]


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


def _validation_metadata(
    *,
    status: str,
    warnings: list[WarningEntry],
    source: str,
    governance_context: Mapping[str, Any],
    parser_failure: bool = False,
) -> dict[str, object]:
    """Return sidecar validation metadata for supervisor-control records."""

    return {
        "validation": ValidationMetadata(
            stage=AGENT_NAME,
            status="passed" if status != "invalid_input" else "rejected",
            accepted=status != "invalid_input",
            rejections=normalize_rejections([warning.to_payload() for warning in warnings if warning.severity == "error"]),
            details={
                "final_status": status,
                "source": source,
                "parser_failure": parser_failure,
            },
        ).to_payload(),
        "governance_context": dict(governance_context),
    }


def _raw_text_from_payload(payload: Mapping[str, Any]) -> str | None:
    """Return raw supervisor-control text when available."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return None
    return _string_or_none(raw_message.get("text"))


def _record_created_at(payload: Mapping[str, Any]) -> str:
    """Return the persisted created-at timestamp for one intelligence record."""

    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping):
        candidate = _string_or_none(metadata.get("received_at"))
        if candidate is not None:
            return candidate

    ingress = payload.get("ingress_envelope")
    if isinstance(ingress, Mapping):
        ingress_payload = ingress.get("payload")
        if isinstance(ingress_payload, Mapping):
            for field_name in ("received_at", "timestamp"):
                candidate = _string_or_none(ingress_payload.get(field_name))
                if candidate is not None:
                    return candidate

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _emit_intelligence_extracted_log(result: AgentResult) -> None:
    """Emit one log event when supervisor intelligence signals are extracted."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    _log_intelligence_event(
        "intelligence_signal_extracted",
        branch=payload.get("branch"),
        report_date=payload.get("report_date"),
        item_count=len(payload.get("items", [])) if isinstance(payload.get("items"), list) else 0,
        warning_codes=_warning_codes(payload),
    )


def _warning_codes(payload: Mapping[str, Any]) -> list[str]:
    """Return warning codes from one supervisor-control payload."""

    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        return []
    codes: list[str] = []
    for warning in warnings:
        if isinstance(warning, Mapping):
            code = warning.get("code")
            if isinstance(code, str) and code.strip():
                codes.append(code.strip())
    return codes


def _string_or_none(value: Any) -> str | None:
    """Return one stripped string or ``None``."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _log_intelligence_event(event: str, **fields: Any) -> None:
    """Emit one compact intelligence parsing log event."""

    LOGGER.info(json.dumps({"event": event, **fields}, sort_keys=True, ensure_ascii=True))
