"""Thin intake-and-routing worker for upstream operational messages.

Live ingestion writes raw first, may update raw metadata after routing, keeps
rejected as a quarantine copy rather than the only copy, and treats structured
records as the usable source of truth. Replay is read-only at the raw archive
layer and suppresses raw writes when `payload["replay"]["is_replay"]` is true.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Final, Literal

from apps.branch_resolver_agent.worker import resolve_branch
from apps.date_resolver_agent.worker import resolve_report_date
from apps.header_normalizer_agent.worker import normalize_headers
from apps.hr_agent.worker import process_work_item as process_hr_work_item
from apps.mixed_content_detector_agent.worker import detect_mixed_content
from apps.pricing_stock_release_agent.worker import (
    process_work_item as process_pricing_stock_release_work_item,
)
import apps.sales_income_agent.record_store as sales_record_store
from apps.report_splitter_agent.worker import split_report
from apps.report_family_classifier_agent.worker import classify_report_family
from apps.routing_decision_agent.worker import build_routing_decision
from apps.sales_income_agent.worker import process_work_item as process_sales_income_work_item
from apps.staff_performance_agent.worker import (
    process_work_item as process_staff_performance_work_item,
)
from apps.supervisor_control_agent.worker import (
    process_work_item as process_supervisor_control_work_item,
)
from packages.record_store.naming import build_rejected_filename, safe_segment
from packages.record_store.paths import get_raw_path, get_rejected_path, get_structured_path
from packages.record_store.writer import write_json_file, write_text_file
from packages.report_registry import route_for_family
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME: Final[str] = "orchestrator_agent"
RAW_MESSAGE_KIND: Final[str] = "raw_message"
SIGNAL_TYPE: Final[str] = "routing"
UNKNOWN_STORAGE_BUCKET: Final[str] = "unknown"

ClassificationLabel = str

RouteStatus = Literal["routed", "needs_review", "invalid_input"]
RejectionReason = Literal[
    "unknown_report_type",
    "missing_raw_text",
    "invalid_input",
    "classifier_failure",
    "routing_failure",
    "parser_failure",
    "subtype_undetermined",
]

TargetAgent = str


@dataclass(slots=True)
class RawAuditRecord:
    """Raw-record audit file locations and normalized metadata."""

    raw_sha256: str
    raw_text: str
    source: str | None
    received_at: str
    sender: str | None
    branch_hint: str | None
    filename: str
    text_path: Path
    meta_path: Path
    is_replay: bool = False
    replay_source: str | None = None
    replay_original_path: str | None = None
    raw_written_by_ingress: bool = False
    existing_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrchestratorAgentWorker:
    """Validate raw intake, classify conservatively, and route one work item."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one raw work item into exactly one downstream agent result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a routed downstream result or a safe structured failure."""

    raw_audit = _prepare_raw_audit_record(work_item)
    if not raw_audit.is_replay and not raw_audit.raw_written_by_ingress:
        _persist_raw_record(raw_audit)
    validation_errors = _validate_raw_work_item(work_item)
    if validation_errors:
        _update_raw_metadata(
            raw_audit,
            detected_report_type="unknown",
            routing_target=None,
            processing_status="invalid_input",
            branch_hint=raw_audit.branch_hint,
            routing_metadata=None,
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason=_rejection_reason_from_validation(validation_errors),
            attempted_report_type="unknown",
            attempted_agent=None,
            attempted_branch_hint=raw_audit.branch_hint,
            exception_message=None,
        )
        return _failure_result(
            work_item,
            classification="unknown",
            status="invalid_input",
            route_reason="invalid_raw_message",
            warnings=validation_errors,
        )

    mixed_detection = detect_mixed_content(raw_audit.raw_text)
    if mixed_detection.is_mixed:
        split_result = split_report(raw_audit.raw_text, mixed_detection)
        if len(split_result.segments) >= 2:
            return _process_mixed_work_item(
                work_item,
                raw_audit=raw_audit,
                mixed_detection=mixed_detection,
                split_result=split_result,
            )

    try:
        routed_work_item = _build_routed_work_item(work_item)
    except Exception as exc:
        _update_raw_metadata(
            raw_audit,
            detected_report_type="unknown",
            routing_target=None,
            processing_status="invalid_input",
            branch_hint=raw_audit.branch_hint,
            routing_metadata=None,
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason="classifier_failure",
            attempted_report_type="unknown",
            attempted_agent=None,
            attempted_branch_hint=raw_audit.branch_hint,
            exception_message=str(exc) or None,
        )
        return _failure_result(
            work_item,
            classification="unknown",
            status="invalid_input",
            route_reason="classifier_failure",
            warnings=[
                _make_warning(
                    code="missing_fields",
                    severity="error",
                    message="The raw message could not be classified safely.",
                )
            ],
        )

    routing_payload = routed_work_item.payload["routing"]
    classification = routing_payload["classification"]
    target_agent = routing_payload["target_agent"]
    route_status = routing_payload["route_status"]
    resolved_branch_hint = routing_payload.get("branch_hint")

    if classification == "unknown" or target_agent is None or route_status != "routed":
        _update_raw_metadata(
            raw_audit,
            detected_report_type=classification,
            routing_target=None,
            processing_status="needs_review",
            branch_hint=resolved_branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason="unknown_report_type",
            attempted_report_type=classification,
            attempted_agent=None,
            attempted_branch_hint=resolved_branch_hint,
            exception_message=None,
        )
        return _failure_result(
            routed_work_item,
            classification=classification,
            status="needs_review",
            route_reason=str(routing_payload.get("review_reason") or "unknown_route"),
            warnings=[
                _make_warning(
                    code="missing_fields",
                    severity="warning",
                    message="The raw message could not be routed to a supported specialist agent.",
                )
            ],
        )

    try:
        result = _dispatch_to_specialist(routed_work_item, target_agent=target_agent)
    except Exception as exc:
        _update_raw_metadata(
            raw_audit,
            detected_report_type=classification,
            routing_target=target_agent,
            processing_status="invalid_input",
            branch_hint=resolved_branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason="routing_failure",
            attempted_report_type=classification,
            attempted_agent=target_agent,
            attempted_branch_hint=resolved_branch_hint,
            exception_message=str(exc) or None,
        )
        return _failure_result(
            routed_work_item,
            classification=classification,
            status="invalid_input",
            route_reason="routing_failure",
            warnings=[
                _make_warning(
                    code="missing_fields",
                    severity="error",
                    message="The routed work item could not be processed safely.",
                )
            ],
        )

    specialist_status = _result_status(result)
    _update_raw_metadata(
        raw_audit,
        detected_report_type=classification,
        routing_target=target_agent,
        processing_status=specialist_status,
        branch_hint=resolved_branch_hint,
        routing_metadata=_routing_metadata_from_payload(routing_payload),
    )
    if specialist_status == "invalid_input":
        _write_rejected_record(
            raw_audit,
            rejection_reason=_rejection_reason_from_result(result),
            attempted_report_type=classification,
            attempted_agent=target_agent,
            attempted_branch_hint=resolved_branch_hint,
            exception_message=None,
        )
    return result


def _process_mixed_work_item(
    work_item: WorkItem,
    *,
    raw_audit: RawAuditRecord,
    mixed_detection,
    split_result,
) -> AgentResult:
    """Process one explicitly mixed raw message through deterministic fan-out."""

    child_results: list[AgentResult] = []
    child_summaries: list[dict[str, Any]] = []
    output_paths: list[str] = []
    warnings: list[dict[str, str]] = []

    for segment in split_result.segments:
        route = route_for_family(segment.detected_report_family)
        if route.target_agent is None or route.specialist_type is None:
            warnings.append(
                _make_warning(
                    code="unsupported_segment",
                    severity="warning",
                    message=f"Mixed child family `{segment.detected_report_family}` has no configured specialist route.",
                )
            )
            child_summaries.append(
                {
                    "agent_name": None,
                    "report_family": segment.detected_report_family,
                    "status": "needs_review",
                    "output_paths": [],
                    "lineage": _build_mixed_child_lineage(
                        raw_audit=raw_audit,
                        segment_id=segment.segment_id,
                        segment_index=segment.segment_index,
                        child_count=len(split_result.segments),
                    ),
                    "segment_id": segment.segment_id,
                    "segment_range": {"start_line": segment.start_line, "end_line": segment.end_line},
                }
            )
            continue

        child_work_item = _build_mixed_child_work_item(
            parent_work_item=work_item,
            raw_audit=raw_audit,
            segment=segment,
            target_agent=route.target_agent,
            specialist_report_type=route.specialist_type,
            child_count=len(split_result.segments),
        )
        try:
            child_result = _dispatch_to_specialist(child_work_item, target_agent=route.target_agent)
        except Exception as exc:
            warnings.append(
                _make_warning(
                    code="routing_failure",
                    severity="error",
                    message=f"Mixed child routing failed for {segment.detected_report_family}: {exc}",
                )
            )
            child_summaries.append(
                {
                    "agent_name": None,
                    "report_family": segment.detected_report_family,
                    "status": "invalid_input",
                    "output_paths": [],
                    "lineage": dict(child_work_item.payload.get("lineage", {})),
                    "segment_id": segment.segment_id,
                    "segment_range": {"start_line": segment.start_line, "end_line": segment.end_line},
                    "error": str(exc),
                }
            )
            continue

        child_results.append(child_result)
        child_output_paths = _structured_output_paths_from_result(child_result)
        output_paths.extend(child_output_paths)
        child_summaries.append(
            {
                "agent_name": child_result.agent_name,
                "report_family": segment.detected_report_family,
                "branch": _result_branch(child_result),
                "report_date": _result_report_date(child_result),
                "status": _result_status(child_result),
                "output_paths": child_output_paths,
                "lineage": dict(child_work_item.payload.get("lineage", {})),
                "segment_id": segment.segment_id,
                "segment_range": {"start_line": segment.start_line, "end_line": segment.end_line},
                "split_confidence": segment.split_confidence,
                "payload": dict(child_result.payload) if isinstance(child_result.payload, dict) else {},
            }
        )

    parent_status = _mixed_parent_status(child_results=child_results, child_summaries=child_summaries)
    branch_hint = _mixed_branch_hint(child_results)
    routing_payload = _build_routing_payload(
        classification="mixed",
        target_agent="fan_out",
        status="routed",
        route_reason="mixed_report_fan_out",
        branch_hint=branch_hint,
        report_date=None,
        raw_report_date=None,
        confidence=_mixed_confidence(child_results),
        evidence=list(mixed_detection.evidence),
        normalized_header_candidates=[],
        review_reason=None if parent_status != "needs_review" else "mixed_child_requires_review",
        specialist_report_type=None,
        split_strategy="explicit_report_headers",
        child_report_types=[child["report_family"] for child in child_summaries],
        child_count=len(child_summaries),
    )

    _update_raw_metadata(
        raw_audit,
        detected_report_type="mixed",
        routing_target="fan_out",
        processing_status=parent_status,
        branch_hint=branch_hint,
        routing_metadata=_routing_metadata_from_payload(routing_payload),
    )

    parent_payload = {
        "signal_type": SIGNAL_TYPE,
        "source_agent": AGENT_NAME,
        "source": work_item.payload.get("source") if isinstance(work_item.payload, dict) else None,
        "output_path": output_paths[0] if output_paths else None,
        "output_paths": output_paths,
        "derived_output_paths": output_paths,
        "segment_count": len(child_summaries),
        "written_count": len(output_paths),
        "classification": {
            "report_type": "mixed",
            "child_report_types": [child["report_family"] for child in child_summaries],
            "mixed_detection": mixed_detection.classification,
        },
        "routing": routing_payload,
        "mixed_detection": {
            "classification": mixed_detection.classification,
            "is_mixed": mixed_detection.is_mixed,
            "detected_families": list(mixed_detection.detected_families),
            "confidence": mixed_detection.confidence,
            "evidence": list(mixed_detection.evidence),
            "boundary_hints": [
                {
                    "report_family": hint.report_family,
                    "line_number": hint.line_number,
                    "raw_line": hint.raw_line,
                }
                for hint in mixed_detection.boundary_hints
            ],
        },
        "fanout": {
            "was_split": True,
            "split_strategy": "explicit_report_headers",
            "common_prefix_lines": list(split_result.common_prefix_lines),
            "child_count": len(child_summaries),
            "children": child_summaries,
        },
        "outputs": output_paths,
        "warnings": warnings,
        "status": parent_status,
        "confidence": _mixed_confidence(child_results),
        "metrics": {
            "child_count": len(child_summaries),
            "output_count": len(output_paths),
        },
        "items": [],
        "lineage": {
            "message_role": "split_parent",
            "split_strategy": "explicit_report_headers",
            "child_count": len(child_summaries),
            "parent_raw_txt_path": str(raw_audit.text_path),
            "parent_raw_sha256": raw_audit.raw_sha256,
            "derived_from_mixed_report": False,
        },
    }
    return AgentResult(agent_name=AGENT_NAME, payload=parent_payload)


def classify_raw_message(text: str) -> ClassificationLabel:
    """Return the conservative single-route classification for raw message text."""

    header_result = normalize_headers(text)
    return classify_report_family(text, header_result).report_family


def main() -> int:
    """Return a success code for basic module smoke execution."""

    return 0


def _validate_raw_work_item(work_item: WorkItem) -> list[dict[str, str]]:
    """Validate only the strict raw input contract required for routing."""

    warnings: list[dict[str, str]] = []
    if work_item.kind != RAW_MESSAGE_KIND:
        warnings.append(
            _make_warning(
                code="missing_fields",
                severity="error",
                message="The work item kind must be `raw_message`.",
            )
        )

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        warnings.append(
            _make_warning(
                code="missing_fields",
                severity="error",
                message="The work item payload must include a `raw_message` object.",
            )
        )
        return warnings

    text = raw_message.get("text")
    if not isinstance(text, str) or not text.strip():
        warnings.append(
            _make_warning(
                code="missing_fields",
                severity="error",
                message="The work item raw_message.text field must be a non-empty string.",
            )
        )

    return warnings


def _prepare_raw_audit_record(work_item: WorkItem) -> RawAuditRecord:
    """Build the raw audit record used for live intake or replay routing."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    metadata = _sanitize_metadata(payload.get("metadata"))
    replay = _sanitize_replay(payload.get("replay"))
    raw_record = _sanitize_raw_record(payload.get("raw_record"))
    raw_text = _extract_raw_text(payload)
    received_at = metadata.get("received_at") or _utc_timestamp()
    branch_hint = metadata.get("branch_hint")
    raw_sha256 = raw_record.get("raw_sha256")
    if not isinstance(raw_sha256, str) or not raw_sha256.strip():
        raw_sha256 = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    filename = _build_raw_filename(received_at=received_at, branch_hint=branch_hint, raw_sha256=raw_sha256)
    text_path = _raw_path_from_record(raw_record, fallback_filename=filename)
    meta_path = _raw_meta_path_from_record(raw_record, fallback_text_path=text_path)
    audit = RawAuditRecord(
        raw_sha256=raw_sha256,
        raw_text=raw_text,
        source=_sanitize_optional_text(payload.get("source")),
        received_at=received_at,
        sender=metadata.get("sender"),
        branch_hint=branch_hint,
        filename=filename,
        text_path=text_path,
        meta_path=meta_path,
        is_replay=replay.get("is_replay") is True,
        replay_source=replay.get("source"),
        replay_original_path=replay.get("original_path"),
        raw_written_by_ingress=raw_record.get("raw_written") is True,
        existing_metadata=_load_existing_metadata(meta_path),
    )
    return audit


def _persist_raw_record(audit: RawAuditRecord) -> RawAuditRecord:
    """Persist the raw archive and its initial metadata for live ingestion."""

    write_text_file(audit.text_path, audit.raw_text)
    write_json_file(
        audit.meta_path,
        _raw_metadata_payload(
            audit,
            detected_report_type="unknown",
            routing_target=None,
            processing_status="received",
            branch_hint=audit.branch_hint,
            routing_metadata=None,
        ),
    )
    return audit


def _update_raw_metadata(
    audit: RawAuditRecord,
    *,
    detected_report_type: ClassificationLabel,
    routing_target: str | None,
    processing_status: str,
    branch_hint: str | None,
    routing_metadata: dict[str, Any] | None,
) -> None:
    """Update audit metadata without relocating the original raw archive."""

    if audit.is_replay:
        return

    write_json_file(
        audit.meta_path,
        _raw_metadata_payload(
            audit,
            detected_report_type=detected_report_type,
            routing_target=routing_target,
            processing_status=processing_status,
            branch_hint=branch_hint,
            routing_metadata=routing_metadata,
        ),
    )


def _write_rejected_record(
    audit: RawAuditRecord,
    *,
    rejection_reason: RejectionReason,
    attempted_report_type: ClassificationLabel,
    attempted_agent: str | None,
    attempted_branch_hint: str | None,
    exception_message: str | None,
) -> Path:
    """Write the rejected quarantine copy and its metadata."""

    rejected_bucket = _storage_bucket_for_classification(attempted_report_type)
    filename = build_rejected_filename(attempted_report_type, rejection_reason)
    text_path = get_rejected_path(rejected_bucket) / filename
    meta_path = text_path.with_suffix(".meta.json")
    write_text_file(text_path, audit.raw_text)
    write_json_file(
        meta_path,
        {
            "rejection_reason": rejection_reason,
            "source": audit.source,
            "received_at": audit.received_at,
            "sender": audit.sender,
            "branch_hint": attempted_branch_hint,
            "attempted_report_type": attempted_report_type,
            "attempted_agent": attempted_agent,
            "raw_sha256": audit.raw_sha256,
            "exception_message": exception_message,
            "replay": audit.is_replay,
            "replay_source": audit.replay_source,
            "replay_original_path": audit.replay_original_path,
        },
    )
    return text_path


def _build_routed_work_item(work_item: WorkItem) -> WorkItem:
    """Create the minimal safe routed work item for exactly one specialist agent."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_message = payload.get("raw_message")
    text = raw_message.get("text", "") if isinstance(raw_message, Mapping) else ""
    metadata = _sanitize_metadata(payload.get("metadata"))
    header_result = normalize_headers(text)
    branch_resolution = resolve_branch(header_result, metadata_branch_hint=metadata.get("branch_hint"))
    date_resolution = resolve_report_date(header_result)
    family_classification = classify_report_family(text, header_result)
    routing_decision = build_routing_decision(
        header_result=header_result,
        branch_resolution=branch_resolution,
        date_resolution=date_resolution,
        family_classification=family_classification,
    )

    routed_payload: dict[str, Any] = {
        "raw_message": {"text": text},
        "classification": {
            "report_family": routing_decision.detected_report_type,
            "report_type": routing_decision.specialist_report_type,
            "confidence": family_classification.confidence,
            "evidence": family_classification.evidence,
        },
        "routing": _build_routing_payload(
            classification=routing_decision.detected_report_type,
            target_agent=routing_decision.routing_target,
            status=routing_decision.processing_status if routing_decision.processing_status in {"routed", "needs_review"} else "needs_review",
            route_reason="classified_for_specialist" if routing_decision.routing_target is not None else "unknown_route",
            branch_hint=routing_decision.branch_hint,
            report_date=routing_decision.report_date,
            raw_report_date=routing_decision.raw_report_date,
            confidence=routing_decision.confidence,
            evidence=routing_decision.evidence,
            normalized_header_candidates=routing_decision.normalized_header_candidates,
            review_reason=routing_decision.review_reason,
            specialist_report_type=routing_decision.specialist_report_type,
        ),
    }

    source = payload.get("source")
    if isinstance(source, str) and source.strip():
        routed_payload["source"] = source

    if metadata:
        if routing_decision.branch_hint is not None:
            metadata["branch_hint"] = routing_decision.branch_hint
        routed_payload["metadata"] = metadata

    replay = _sanitize_replay(payload.get("replay"))
    if replay:
        routed_payload["replay"] = replay

    return WorkItem(kind=RAW_MESSAGE_KIND, payload=routed_payload)


def _raw_metadata_payload(
    audit: RawAuditRecord,
    *,
    detected_report_type: ClassificationLabel,
    routing_target: str | None,
    processing_status: str,
    branch_hint: str | None,
    routing_metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the raw metadata JSON payload."""

    payload = {
        **audit.existing_metadata,
        "source": audit.source,
        "received_at": audit.received_at,
        "sender": audit.sender,
        "branch_hint": branch_hint,
        "detected_report_type": detected_report_type,
        "routing_target": routing_target,
        "raw_sha256": audit.raw_sha256,
        "processing_status": processing_status,
    }
    if routing_metadata:
        payload.update(routing_metadata)
    return payload


def _sanitize_metadata(metadata: object) -> dict[str, str]:
    """Keep only the known optional routing metadata fields when present."""

    if not isinstance(metadata, Mapping):
        return {}

    safe_metadata: dict[str, str] = {}
    for field_name in ("sender", "branch_hint", "received_at"):
        value = metadata.get(field_name)
        if isinstance(value, str) and value.strip():
            safe_metadata[field_name] = value
    return safe_metadata


def _sanitize_optional_text(value: object) -> str | None:
    """Return one stripped optional string when present."""

    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _sanitize_replay(replay: object) -> dict[str, Any]:
    """Keep only explicit replay markers used to suppress raw archive writes."""

    if not isinstance(replay, Mapping):
        return {}

    if replay.get("is_replay") is not True:
        return {}

    safe_replay: dict[str, Any] = {"is_replay": True}
    for field_name in ("source", "original_path", "replayed_at"):
        value = replay.get(field_name)
        if isinstance(value, str) and value.strip():
            safe_replay[field_name] = value.strip()
    return safe_replay


def _sanitize_raw_record(raw_record: object) -> dict[str, Any]:
    """Keep only explicit raw-record fields provided by the ingress bridge."""

    if not isinstance(raw_record, Mapping):
        return {}

    safe_record: dict[str, Any] = {}
    for field_name in ("raw_txt_path", "raw_meta_path", "raw_sha256"):
        value = raw_record.get(field_name)
        if isinstance(value, str) and value.strip():
            safe_record[field_name] = value.strip()
    if raw_record.get("raw_written") is True:
        safe_record["raw_written"] = True
    return safe_record


def _raw_path_from_record(raw_record: dict[str, Any], *, fallback_filename: str) -> Path:
    """Return the bridge-provided raw text path when explicitly available."""

    raw_txt_path = raw_record.get("raw_txt_path")
    if isinstance(raw_txt_path, str) and raw_txt_path.strip():
        return Path(raw_txt_path)
    return get_raw_path(UNKNOWN_STORAGE_BUCKET) / fallback_filename


def _raw_meta_path_from_record(raw_record: dict[str, Any], *, fallback_text_path: Path) -> Path:
    """Return the bridge-provided raw metadata path when explicitly available."""

    raw_meta_path = raw_record.get("raw_meta_path")
    if isinstance(raw_meta_path, str) and raw_meta_path.strip():
        return Path(raw_meta_path)
    return fallback_text_path.with_suffix(".meta.json")


def _load_existing_metadata(path: Path) -> dict[str, Any]:
    """Return existing raw metadata when the ingress already wrote it."""

    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_raw_text(payload: dict[str, object]) -> str:
    """Return the inbound raw text or an empty string for invalid input."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""

    text = raw_message.get("text")
    if not isinstance(text, str):
        return ""
    return text


def _dispatch_to_specialist(
    work_item: WorkItem,
    *,
    target_agent: TargetAgent,
) -> AgentResult:
    """Route the prepared work item to exactly one specialist agent."""

    if target_agent == "sales_income_agent":
        return process_sales_income_work_item(work_item)
    if target_agent == "pricing_stock_release_agent":
        return process_pricing_stock_release_work_item(work_item)
    if target_agent == "staff_performance_agent":
        return process_staff_performance_work_item(work_item)
    if target_agent == "supervisor_control_agent":
        return process_supervisor_control_work_item(work_item)
    return process_hr_work_item(work_item)


def _result_status(result: AgentResult) -> str:
    """Return the downstream processing status safely."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    status = payload.get("status")
    if isinstance(status, str) and status.strip():
        return status
    return "needs_review"


def _failure_result(
    work_item: WorkItem,
    *,
    classification: ClassificationLabel,
    status: RouteStatus,
    route_reason: str,
    warnings: list[dict[str, str]],
) -> AgentResult:
    """Return a safe structured routing failure without specialist processing."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_message = payload.get("raw_message")
    metadata = _sanitize_metadata(payload.get("metadata"))
    routing = payload.get("routing") if isinstance(payload.get("routing"), Mapping) else {}
    source = payload.get("source") if isinstance(payload.get("source"), str) else None

    safe_raw_message: dict[str, Any] = {}
    if isinstance(raw_message, Mapping) and isinstance(raw_message.get("text"), str):
        safe_raw_message["text"] = raw_message["text"]

    target_agent = routing.get("target_agent")
    if not isinstance(target_agent, str):
        target_agent = route_for_family(classification).target_agent

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "source": source,
            "classification": {"report_type": classification},
            "routing": _build_routing_payload(
                classification=classification,
                target_agent=target_agent,
                status=status,
                route_reason=route_reason,
            ),
            "raw_message": safe_raw_message,
            "metadata": metadata,
            "confidence": 0.0,
            "metrics": {},
            "items": [],
            "warnings": warnings,
            "status": status,
        },
    )


def _rejection_reason_from_validation(warnings: list[dict[str, str]]) -> RejectionReason:
    """Map validation warnings into stable rejection reason codes."""

    for warning in warnings:
        if warning.get("message") == "The work item raw_message.text field must be a non-empty string.":
            return "missing_raw_text"
    return "invalid_input"


def _rejection_reason_from_result(result: AgentResult) -> RejectionReason:
    """Map specialist invalid-input results into stable rejection reason codes."""

    if result.agent_name == "hr_agent":
        payload = result.payload if isinstance(result.payload, dict) else {}
        warnings = payload.get("warnings")
        if isinstance(warnings, list):
            for warning in warnings:
                if isinstance(warning, Mapping):
                    message = warning.get("message")
                    if isinstance(message, str) and "subtype could not be safely determined" in message:
                        return "subtype_undetermined"
    return "parser_failure"


def _build_routing_payload(
    *,
    classification: ClassificationLabel,
    target_agent: str | None,
    status: RouteStatus,
    route_reason: str,
    branch_hint: str | None = None,
    report_date: str | None = None,
    raw_report_date: str | None = None,
    confidence: float | None = None,
    evidence: list[str] | None = None,
    normalized_header_candidates: list[str] | None = None,
    review_reason: str | None = None,
    specialist_report_type: str | None = None,
    split_strategy: str | None = None,
    child_report_types: list[str] | None = None,
    child_count: int | None = None,
) -> dict[str, Any]:
    """Return the minimal explicit routing metadata block."""

    return {
        "classification": classification,
        "target_agent": target_agent,
        "route_status": status,
        "route_reason": route_reason,
        "branch_hint": branch_hint,
        "report_date": report_date,
        "raw_report_date": raw_report_date,
        "confidence": confidence,
        "evidence": evidence or [],
        "normalized_header_candidates": normalized_header_candidates or [],
        "review_reason": review_reason,
        "specialist_report_type": specialist_report_type,
        "split_strategy": split_strategy,
        "child_report_types": child_report_types or [],
        "child_count": child_count,
    }


def _storage_bucket_for_classification(classification: ClassificationLabel) -> str:
    """Return the canonical raw/rejected storage bucket for one classification."""

    return route_for_family(classification).storage_bucket


def _build_raw_filename(
    *,
    received_at: str,
    branch_hint: str | None,
    raw_sha256: str,
) -> str:
    """Return a stable raw filename for one inbound message."""

    date_segment = _date_segment(received_at)
    branch_segment = safe_segment(branch_hint or UNKNOWN_STORAGE_BUCKET)
    return f"{date_segment}__{branch_segment}__{raw_sha256[:12]}.txt"


def _date_segment(received_at: str) -> str:
    """Return the date segment used in audit filenames."""

    if len(received_at) >= 10:
        candidate = received_at[:10]
        if (
            candidate[4] == "-"
            and candidate[7] == "-"
            and candidate[:4].isdigit()
            and candidate[5:7].isdigit()
            and candidate[8:10].isdigit()
        ):
            return candidate
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _utc_timestamp() -> str:
    """Return a stable UTC timestamp for audit metadata."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _make_warning(*, code: str, severity: str, message: str) -> dict[str, str]:
    """Build a minimal JSON-safe warning entry."""

    return {
        "code": code,
        "severity": severity,
        "message": message,
    }


def _routing_metadata_from_payload(routing_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Extract explicit routing debug metadata for raw audit updates."""

    metadata: dict[str, Any] = {
        "route_status": routing_payload.get("route_status"),
        "route_reason": routing_payload.get("route_reason"),
        "resolved_report_date": routing_payload.get("report_date"),
        "raw_report_date": routing_payload.get("raw_report_date"),
        "routing_confidence": routing_payload.get("confidence"),
        "routing_evidence": routing_payload.get("evidence"),
        "normalized_header_candidates": routing_payload.get("normalized_header_candidates"),
        "routing_review_reason": routing_payload.get("review_reason"),
        "specialist_report_type": routing_payload.get("specialist_report_type"),
        "split_strategy": routing_payload.get("split_strategy"),
        "split_child_count": routing_payload.get("child_count"),
        "split_child_report_types": routing_payload.get("child_report_types"),
    }
    return metadata


def _structured_output_paths_from_result(result: AgentResult) -> list[str]:
    """Return canonical structured output paths for one child result."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    if payload.get("status") == "invalid_input":
        return []

    branch = payload.get("branch")
    report_date = payload.get("report_date")
    if not isinstance(branch, str) or not isinstance(report_date, str):
        return []

    if result.agent_name == "sales_income_agent":
        canonical_branch = sales_record_store._canonical_branch_or_none(branch)
        normalized_date = sales_record_store._iso_date_or_none(report_date)
        if canonical_branch is None or normalized_date is None:
            return []
        return [_display_structured_path(get_structured_path("sales_income", canonical_branch, normalized_date))]
    if result.agent_name == "pricing_stock_release_agent":
        return [_display_structured_path(get_structured_path("pricing_stock_release", branch, report_date))]
    if result.agent_name == "hr_agent":
        subtype = payload.get("signal_subtype")
        if subtype == "staff_attendance":
            return [_display_structured_path(get_structured_path("hr_attendance", branch, report_date))]
        if subtype == "staff_performance":
            return [_display_structured_path(get_structured_path("hr_performance", branch, report_date))]
        return []
    if result.agent_name == "staff_performance_agent":
        return [_display_structured_path(get_structured_path("hr_performance", branch, report_date))]
    if result.agent_name == "supervisor_control_agent":
        return [_display_structured_path(get_structured_path("supervisor_control", branch, report_date))]
    return []


def _result_branch(result: AgentResult) -> str | None:
    """Return one result branch when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    branch = payload.get("branch")
    if isinstance(branch, str) and branch.strip():
        return branch.strip()
    return None


def _result_report_date(result: AgentResult) -> str | None:
    """Return one result report date when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    report_date = payload.get("report_date")
    if isinstance(report_date, str) and report_date.strip():
        return report_date.strip()
    return None


def _display_structured_path(path: Path) -> str:
    """Return a stable repo-style structured path for result reporting."""

    parts = path.parts
    if "records" in parts:
        return str(Path(*parts[parts.index("records") :]))
    return str(path)


def _mixed_parent_status(
    *,
    child_results: list[AgentResult],
    child_summaries: list[dict[str, Any]],
) -> str:
    """Return aggregate status for a mixed parent result."""

    if not child_summaries:
        return "invalid_input"
    child_statuses = [summary.get("status") for summary in child_summaries]
    if any(status == "invalid_input" for status in child_statuses):
        return "needs_review"
    if any(status in {"needs_review", "accepted_with_warning"} for status in child_statuses):
        return "accepted_with_warning"
    if child_results:
        return "accepted_split"
    return "invalid_input"


def _mixed_confidence(child_results: list[AgentResult]) -> float:
    """Return average child confidence for a mixed parent result."""

    confidences: list[float] = []
    for result in child_results:
        payload = result.payload if isinstance(result.payload, dict) else {}
        confidence = payload.get("confidence")
        if isinstance(confidence, (int, float)):
            confidences.append(float(confidence))
    if not confidences:
        return 0.0
    return round(sum(confidences) / len(confidences), 2)


def _mixed_branch_hint(child_results: list[AgentResult]) -> str | None:
    """Return one stable branch hint when mixed children agree."""

    branches = []
    for result in child_results:
        payload = result.payload if isinstance(result.payload, dict) else {}
        branch = payload.get("branch")
        if isinstance(branch, str) and branch.strip():
            branches.append(branch.strip())
    unique = sorted(set(branches))
    if len(unique) == 1:
        return unique[0]
    return None


def _build_mixed_child_work_item(
    *,
    parent_work_item: WorkItem,
    raw_audit: RawAuditRecord,
    segment,
    target_agent: str,
    specialist_report_type: str,
    child_count: int,
) -> WorkItem:
    """Return one child work item for a split mixed segment."""

    payload = dict(parent_work_item.payload if isinstance(parent_work_item.payload, dict) else {})
    payload["raw_message"] = {"text": segment.raw_text}
    payload["classification"] = {
        "report_family": segment.detected_report_family,
        "report_type": specialist_report_type,
        "confidence": segment.split_confidence,
        "evidence": list(segment.evidence),
    }
    lineage = _build_mixed_child_lineage(
        raw_audit=raw_audit,
        segment_id=segment.segment_id,
        segment_index=segment.segment_index,
        child_count=child_count,
    )
    payload["routing"] = {
        "classification": segment.detected_report_family,
        "target_agent": target_agent,
        "route_status": "routed",
        "route_reason": "fanout_split_child",
        "branch_hint": _sanitize_metadata(payload.get("metadata")).get("branch_hint"),
        "report_date": None,
        "raw_report_date": None,
        "confidence": segment.split_confidence,
        "evidence": list(segment.evidence),
        "normalized_header_candidates": [],
        "review_reason": None,
        "specialist_report_type": specialist_report_type,
        "lineage": lineage,
    }
    payload["lineage"] = lineage
    return WorkItem(kind=parent_work_item.kind, payload=payload)


def _build_mixed_child_lineage(
    *,
    raw_audit: RawAuditRecord,
    segment_id: str,
    segment_index: int,
    child_count: int,
) -> dict[str, Any]:
    """Return explicit lineage metadata for one derived split segment."""

    return {
        "message_role": "split_child",
        "segment_id": segment_id,
        "segment_index": segment_index,
        "child_count": child_count,
        "parent_raw_txt_path": str(raw_audit.text_path),
        "parent_raw_sha256": raw_audit.raw_sha256,
        "split_source_agent": AGENT_NAME,
        "derived_from_mixed_report": True,
    }
