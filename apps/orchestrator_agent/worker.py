"""Thin intake-and-routing worker for upstream operational messages.

Live ingestion writes raw first, may update raw metadata after routing, keeps
rejected as a quarantine copy rather than the only copy, and treats structured
records as the usable source of truth. Replay is read-only at the raw archive
layer and suppresses raw writes when `payload["replay"]["is_replay"]` is true.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Final, Literal

from apps.branch_resolver_agent.worker import resolve_branch
from apps.date_resolver_agent.worker import resolve_report_date
from apps.fallback_extraction_agent.worker import (
    process_work_item as process_fallback_extraction_work_item,
)
from apps.header_normalizer_agent.worker import normalize_headers
import apps.hr_agent.record_store as hr_record_store
from apps.hr_agent.worker import process_work_item as process_hr_work_item
from apps.mixed_content_detector_agent.worker import detect_mixed_content
from apps.orchestrator_agent.policy_guard import (
    PolicyDecision,
    evaluate_mixed_report_policy,
    evaluate_pre_specialist_policy,
    reject_decision,
)
import apps.pricing_stock_release_agent.record_store as pricing_record_store
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
import apps.supervisor_control_agent.record_store as supervisor_record_store
from packages.record_store.naming import build_rejected_filename, safe_segment
from packages.record_store.paths import get_raw_path, get_rejected_path, get_structured_path
from packages.record_store.writer import write_json_file, write_text_file
from packages.normalization.dates import normalize_report_date
from packages.normalization.engine import normalize_report
from packages.report_acceptance import decide_acceptance
from packages.report_policy import get_report_policy
from packages.provenance_store import write_provenance_record
from packages.report_registry import route_for_family
from packages.review_queue import write_review_item
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.sop_validation.router import validate_report
from packages.sop_validation.common import is_iso_date

AGENT_NAME: Final[str] = "orchestrator_agent"
RAW_MESSAGE_KIND: Final[str] = "raw_message"
SIGNAL_TYPE: Final[str] = "routing"
UNKNOWN_STORAGE_BUCKET: Final[str] = "unknown"
CLASSIFICATION_CONFIDENCE_MIN_FOR_FALLBACK: Final[float] = 0.45
MIXED_SPLIT_CONFIDENCE_MIN: Final[float] = 0.85
RAW_SHA256_DEDUP_WINDOW: Final[timedelta] = timedelta(hours=24)

ClassificationLabel = str

RouteStatus = Literal["routed", "ready", "needs_review", "invalid_input", "accepted", "accepted_with_warning", "rejected", "duplicate", "conflict_blocked"]
RawProcessingStatus = Literal["received", "processed", "rejected", "duplicate"]
RejectionReason = Literal[
    "unknown_report_type",
    "invalid_pricing_card_format",
    "missing_raw_text",
    "invalid_input",
    "duplicate_message",
    "duplicate_message_id",
    "duplicate_raw_sha256",
    "duplicate_semantic",
    "conflicting_record_same_scope",
    "mixed_report",
    "classifier_failure",
    "routing_failure",
    "parser_failure",
    "fallback_validation_failed",
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

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_audit = _prepare_raw_audit_record(work_item)
    processing_text = _extract_processing_text(payload)
    if not raw_audit.is_replay and not raw_audit.raw_written_by_ingress:
        _persist_raw_record(raw_audit)
    validation_errors = _validate_raw_work_item(work_item)
    if validation_errors:
        policy_decision = reject_decision(
            reason="invalid_raw_message",
            report_family="unknown",
            report_type=None,
            target_agent=None,
        )
        _update_raw_metadata(
            raw_audit,
            detected_report_type="unknown",
            routing_target=None,
            processing_status="rejected",
            branch_hint=raw_audit.branch_hint,
            routing_metadata=None,
            policy_decision=policy_decision,
            governance_outcome=_governance_outcome_payload(status="rejected", reasons=["insufficient_structure"]),
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason=_rejection_reason_from_validation(validation_errors),
            attempted_report_type="unknown",
            attempted_agent=None,
            attempted_branch_hint=raw_audit.branch_hint,
            exception_message=None,
            policy_decision=policy_decision,
        )
        return _failure_result(
            work_item,
            classification="unknown",
            status="invalid_input",
            route_reason="invalid_raw_message",
            warnings=validation_errors,
            policy_decision=policy_decision,
        )

    mixed_detection = detect_mixed_content(processing_text)
    if mixed_detection.is_mixed:
        mixed_policy = evaluate_mixed_report_policy(
            reject_mixed_reports=_reject_mixed_reports(payload)
        )
        if mixed_policy.action == "reject":
            routing_payload = _build_routing_payload(
                classification="mixed",
                target_agent=None,
                status="needs_review",
                route_reason="mixed_report_rejected",
                branch_hint=raw_audit.branch_hint,
                confidence=mixed_detection.confidence,
                evidence=list(mixed_detection.evidence),
                review_reason="mixed_reports_rejected_upstream",
            )
            _update_raw_metadata(
                raw_audit,
                detected_report_type="mixed",
                routing_target=None,
                processing_status="rejected",
                branch_hint=raw_audit.branch_hint,
                routing_metadata=_routing_metadata_from_payload(routing_payload),
                policy_decision=mixed_policy,
                governance_outcome=_governance_outcome_payload(status="rejected", reasons=["insufficient_structure"]),
            )
            _write_rejected_record(
                raw_audit,
                rejection_reason="mixed_report",
                attempted_report_type="mixed",
                attempted_agent=None,
                attempted_branch_hint=raw_audit.branch_hint,
                exception_message=None,
                policy_decision=mixed_policy,
            )
            rejected_payload = dict(payload)
            rejected_payload["routing"] = routing_payload
            return _failure_result(
                WorkItem(kind=work_item.kind, payload=rejected_payload),
                classification="mixed",
                status="needs_review",
                route_reason="mixed_report_rejected",
                warnings=[
                    _make_warning(
                        code="mixed_report",
                        severity="warning",
                        message="Mixed WhatsApp reports are rejected upstream and must be resubmitted as one report per message.",
                    )
                ],
                policy_decision=mixed_policy,
            )
        split_result = split_report(processing_text, mixed_detection)
        if _can_safely_split_mixed_report(mixed_detection=mixed_detection, split_result=split_result):
            return _process_mixed_work_item(
                work_item,
                raw_audit=raw_audit,
                mixed_detection=mixed_detection,
                split_result=split_result,
                policy_decision=mixed_policy,
            )
        routing_payload = _build_routing_payload(
            classification="mixed",
            target_agent=None,
            status="needs_review",
            route_reason="mixed_report_requires_review",
            branch_hint=raw_audit.branch_hint,
            confidence=split_result.split_confidence,
            evidence=list(mixed_detection.evidence),
            review_reason="mixed_report_split_not_safe",
            child_report_types=list(mixed_detection.detected_families),
            child_count=len(split_result.segments),
        )
        _update_raw_metadata(
            raw_audit,
            detected_report_type="mixed",
            routing_target=None,
            processing_status="rejected",
            branch_hint=raw_audit.branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
            policy_decision=mixed_policy,
            governance_outcome=_governance_outcome_payload(status="needs_review", reasons=["insufficient_structure"]),
        )
        review_payload = dict(payload)
        review_payload["routing"] = routing_payload
        return _failure_result(
            WorkItem(kind=work_item.kind, payload=review_payload),
            classification="mixed",
            status="needs_review",
            route_reason="mixed_report_requires_review",
            warnings=[
                _make_warning(
                    code="mixed_split_review",
                    severity="warning",
                    message="Mixed WhatsApp content was detected but could not be safely split for specialist fan-out.",
                )
            ],
            policy_decision=mixed_policy,
        )

    try:
        routed_work_item = _build_routed_work_item(work_item)
    except Exception as exc:
        policy_decision = reject_decision(
            reason="classifier_failure",
            report_family="unknown",
            report_type=None,
            target_agent=None,
        )
        _update_raw_metadata(
            raw_audit,
            detected_report_type="unknown",
            routing_target=None,
            processing_status="rejected",
            branch_hint=raw_audit.branch_hint,
            routing_metadata=None,
            policy_decision=policy_decision,
            governance_outcome=_governance_outcome_payload(status="rejected", reasons=["unknown_report_type"]),
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason="classifier_failure",
            attempted_report_type="unknown",
            attempted_agent=None,
            attempted_branch_hint=raw_audit.branch_hint,
            exception_message=str(exc) or None,
            policy_decision=policy_decision,
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
            policy_decision=policy_decision,
        )

    routing_payload = routed_work_item.payload["routing"]
    classification = routing_payload["classification"]
    target_agent = routing_payload["target_agent"]
    route_status = routing_payload["route_status"]
    resolved_branch_hint = routing_payload.get("branch_hint")
    specialist_report_type = routing_payload.get("specialist_report_type")

    policy_decision = evaluate_pre_specialist_policy(
        existing_metadata=raw_audit.existing_metadata,
        report_family=classification,
        report_type=specialist_report_type if isinstance(specialist_report_type, str) else None,
        target_agent=target_agent if isinstance(target_agent, str) else None,
        route_status=route_status if isinstance(route_status, str) else None,
    )

    if policy_decision.action == "reject":
        if policy_decision.reason == "duplicate_message":
            rejection_reason: RejectionReason = "duplicate_message"
            route_reason = "duplicate_message_rejected"
            warning_code = "duplicate_message"
            warning_message = "This raw message was already processed and was blocked by the idempotency policy."
        elif classification == "invalid_pricing_card_format":
            rejection_reason = "invalid_pricing_card_format"
            route_reason = "invalid_pricing_card_format"
            warning_code = "invalid_pricing_card_format"
            warning_message = (
                "Bale pricing card messages without release indicators are rejected upstream and must not be routed as bale releases."
            )
        else:
            rejection_reason = "unknown_report_type"
            route_reason = str(routing_payload.get("review_reason") or "unknown_route")
            warning_code = "missing_fields"
            warning_message = "The raw message could not be routed to a supported specialist agent."
        _update_raw_metadata(
            raw_audit,
            detected_report_type=classification,
            routing_target=None,
            processing_status="duplicate" if policy_decision.reason == "duplicate_message" else "rejected",
            branch_hint=resolved_branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
            policy_decision=policy_decision,
            governance_outcome=_governance_outcome_payload(
                status="duplicate" if policy_decision.reason == "duplicate_message" else "rejected",
                reasons=[_policy_reason_to_governance_reason(policy_decision.reason)],
            ),
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason=rejection_reason,
            attempted_report_type=classification,
            attempted_agent=None,
            attempted_branch_hint=resolved_branch_hint,
            exception_message=None,
            policy_decision=policy_decision,
        )
        return _failure_result(
            routed_work_item,
            classification=classification,
            status="duplicate" if policy_decision.reason == "duplicate_message" else "rejected",
            route_reason=route_reason,
            warnings=[
                _make_warning(
                    code=warning_code,
                    severity="warning",
                    message=warning_message,
                )
            ],
            policy_decision=policy_decision,
        )

    strict_work_item = _with_candidate_mode(routed_work_item)

    try:
        result = _dispatch_to_specialist(strict_work_item, target_agent=target_agent)
    except Exception as exc:
        routing_failure_policy = reject_decision(
            reason="routing_failure",
            report_family=classification,
            report_type=specialist_report_type if isinstance(specialist_report_type, str) else None,
            target_agent=target_agent,
        )
        _update_raw_metadata(
            raw_audit,
            detected_report_type=classification,
            routing_target=target_agent,
            processing_status="rejected",
            branch_hint=resolved_branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
            policy_decision=routing_failure_policy,
            governance_outcome=_governance_outcome_payload(status="rejected", reasons=["parser_failure"]),
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason="routing_failure",
            attempted_report_type=classification,
            attempted_agent=target_agent,
            attempted_branch_hint=resolved_branch_hint,
            exception_message=str(exc) or None,
            policy_decision=routing_failure_policy,
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
            policy_decision=routing_failure_policy,
        )

    specialist_status = _result_status(result)
    if _should_attempt_specialist_fallback(
        result=result,
        routed_work_item=routed_work_item,
        specialist_status=specialist_status,
        specialist_report_type=specialist_report_type if isinstance(specialist_report_type, str) else None,
        policy_decision=policy_decision,
    ):
        fallback_result = _process_specialist_fallback(
            routed_work_item=routed_work_item,
            raw_audit=raw_audit,
            classification=classification,
            target_agent=target_agent,
            specialist_report_type=specialist_report_type if isinstance(specialist_report_type, str) else None,
            resolved_branch_hint=resolved_branch_hint,
            routing_payload=routing_payload,
            policy_decision=policy_decision,
            specialist_status=specialist_status,
        )
        if fallback_result is not None:
            return fallback_result

    if isinstance(specialist_report_type, str):
        result = _finalize_strict_candidate(
            routed_work_item=routed_work_item,
            candidate_result=result,
            specialist_report_type=specialist_report_type,
            raw_audit=raw_audit,
        )

    governed_status = _result_status(result)
    governance_outcome = _result_governance(result)
    _update_raw_metadata(
        raw_audit,
        detected_report_type=classification,
        routing_target=target_agent,
        processing_status=_raw_processing_status_for_result(governed_status),
        branch_hint=resolved_branch_hint,
        routing_metadata=_routing_metadata_from_payload(routing_payload),
        policy_decision=policy_decision,
        governance_outcome=governance_outcome,
        extra_metadata=_result_metadata_extension(result),
    )
    _write_outcome_provenance(
        outcome=governed_status,
        audit=raw_audit,
        report_type=specialist_report_type if isinstance(specialist_report_type, str) else classification,
        branch=_result_branch(result) or resolved_branch_hint or raw_audit.branch_hint or "unknown",
        report_date=_result_report_date(result) or _date_segment(raw_audit.received_at),
        parser_used=result.agent_name,
        parse_mode="strict",
        confidence=_result_confidence(result),
        warnings=_result_warnings(result),
        validation_outcome=_result_validation_outcome(result),
        acceptance_outcome=_result_acceptance_outcome(result),
        downstream_references={"structured_records": _structured_output_paths_from_result(result)},
    )
    if governed_status in {"rejected", "duplicate", "conflict_blocked", "invalid_input"}:
        _write_rejected_record(
            raw_audit,
            rejection_reason=_rejection_reason_from_result(result),
            attempted_report_type=classification,
            attempted_agent=target_agent,
            attempted_branch_hint=resolved_branch_hint,
            exception_message=None,
            policy_decision=policy_decision,
            extra_metadata=_result_metadata_extension(result),
        )
    return result


def _process_specialist_fallback(
    *,
    routed_work_item: WorkItem,
    raw_audit: RawAuditRecord,
    classification: ClassificationLabel,
    target_agent: str,
    specialist_report_type: str | None,
    resolved_branch_hint: str | None,
    routing_payload: Mapping[str, Any],
    policy_decision: PolicyDecision,
    specialist_status: str,
) -> AgentResult | None:
    """Attempt schema-bound fallback extraction after strict parsing fails."""

    fallback_result = process_fallback_extraction_work_item(routed_work_item)
    fallback_payload = fallback_result.payload if isinstance(fallback_result.payload, dict) else {}
    normalized_report = fallback_payload.get("normalized_report")
    if not isinstance(normalized_report, Mapping):
        normalized_report = {}
    validation_payload = _fallback_validation_payload(
        normalized_report=normalized_report,
        routing_payload=routed_work_item.payload.get("routing") if isinstance(routed_work_item.payload, dict) else {},
    )

    validation_result = validate_report(specialist_report_type, validation_payload)
    _downgrade_resolved_fallback_date_rejection(
        report_type=specialist_report_type,
        validation_result=validation_result,
        fallback_payload=fallback_payload,
    )
    acceptance_result = decide_acceptance(
        specialist_report_type,
        validation_result=validation_result,
        work_item_payload={
            **(routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}),
            "normalized_report": dict(validation_result.normalized_payload),
            "confidence": fallback_payload.get("confidence"),
            "status": fallback_payload.get("status"),
            "parse_mode": "fallback",
        },
    )

    if acceptance_result.decision == "reject":
        if specialist_status != "invalid_input":
            return None
        fallback_policy = reject_decision(
            reason="fallback_validation_failed",
            report_family=classification,
            report_type=specialist_report_type,
            target_agent=target_agent,
        )
        fallback_metadata = _fallback_metadata(
            fallback_payload=fallback_payload,
            validation_result=validation_result,
            acceptance_payload=acceptance_result.to_payload(),
            review_queue_path=None,
        )
        _update_raw_metadata(
            raw_audit,
            detected_report_type=classification,
            routing_target=target_agent,
            processing_status="rejected",
            branch_hint=resolved_branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
            policy_decision=fallback_policy,
            governance_outcome=_governance_outcome_payload(status="rejected", reasons=["insufficient_structure"]),
            extra_metadata=fallback_metadata,
        )
        _write_rejected_record(
            raw_audit,
            rejection_reason="fallback_validation_failed",
            attempted_report_type=classification,
            attempted_agent=target_agent,
            attempted_branch_hint=resolved_branch_hint,
            exception_message=None,
            policy_decision=fallback_policy,
            extra_metadata=fallback_metadata,
        )
        return _build_fallback_result(
            routed_work_item=routed_work_item,
            classification=classification,
            route_reason="fallback_validation_rejected",
            fallback_payload=fallback_payload,
            validation_result=validation_result,
            acceptance_payload=acceptance_result.to_payload(),
            warnings=_fallback_warnings(fallback_payload=fallback_payload, validation_result=validation_result),
            policy_decision=fallback_policy,
            status="invalid_input",
            review_queue_path=None,
        )

    review_queue_path: str | None = None
    if acceptance_result.decision == "review":
        review_queue_path = write_review_item(
            routed_work_item,
            report_type=specialist_report_type,
            branch=_string_or_default(validation_result.normalized_payload.get("branch"), default="unknown"),
            report_date=_string_or_default(validation_result.normalized_payload.get("report_date"), default="unknown"),
            confidence=acceptance_result.confidence,
            warnings=_fallback_warnings(fallback_payload=fallback_payload, validation_result=validation_result),
            reason=acceptance_result.reason,
            validation_outcome=validation_result.to_payload(),
            acceptance_outcome=acceptance_result.to_payload(),
            governance_outcome={
                "status": "needs_review",
                "export_allowed": False,
                "reasons": [acceptance_result.reason],
            },
            parser_used="fallback_extraction_agent",
            parse_mode="fallback",
        )

    fallback_metadata = _fallback_metadata(
        fallback_payload=fallback_payload,
        validation_result=validation_result,
        acceptance_payload=acceptance_result.to_payload(),
        review_queue_path=review_queue_path,
    )
    fallback_status = acceptance_result.governed_status(
        warning_codes=[warning.get("code", "") for warning in _fallback_warnings(fallback_payload=fallback_payload, validation_result=validation_result)]
    )
    _update_raw_metadata(
        raw_audit,
        detected_report_type=classification,
        routing_target=target_agent,
        processing_status=_raw_processing_status_for_result(fallback_status),
        branch_hint=resolved_branch_hint,
        routing_metadata=_routing_metadata_from_payload(routing_payload),
        policy_decision=policy_decision,
        governance_outcome=_governance_outcome_payload(
            status=fallback_status,
            reasons=[acceptance_result.reason] if acceptance_result.decision == "review" else [],
            export_allowed=False,
        ),
        extra_metadata=fallback_metadata,
    )
    return _build_fallback_result(
        routed_work_item=routed_work_item,
        classification=classification,
        route_reason="fallback_review_required" if acceptance_result.decision == "review" else "fallback_accepted",
        fallback_payload=fallback_payload,
        validation_result=validation_result,
        acceptance_payload=acceptance_result.to_payload(),
        warnings=_fallback_warnings(fallback_payload=fallback_payload, validation_result=validation_result),
        policy_decision=policy_decision,
        status=fallback_status,
        review_queue_path=review_queue_path,
    )


def _should_attempt_specialist_fallback(
    *,
    result: AgentResult,
    routed_work_item: WorkItem,
    specialist_status: str,
    specialist_report_type: str | None,
    policy_decision: PolicyDecision,
) -> bool:
    """Return whether the orchestrator should try fallback before finalizing."""

    if specialist_status not in {"needs_review", "invalid_input"}:
        return False
    if specialist_report_type is None or not policy_decision.fallback_eligible:
        return False
    classification_confidence = _classification_confidence(routed_work_item)
    if classification_confidence is None:
        return False
    return classification_confidence >= CLASSIFICATION_CONFIDENCE_MIN_FOR_FALLBACK


def _finalize_strict_candidate(
    *,
    routed_work_item: WorkItem,
    candidate_result: AgentResult,
    specialist_report_type: str,
    raw_audit: RawAuditRecord,
) -> AgentResult:
    """Apply shared validation, acceptance, governance, and final action centrally."""

    candidate_payload = candidate_result.payload if isinstance(candidate_result.payload, dict) else {}
    validation_result = validate_report(specialist_report_type, candidate_payload)
    acceptance_result = decide_acceptance(
        specialist_report_type,
        validation_result=validation_result,
        work_item_payload={
            **(routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}),
            "normalized_report": dict(validation_result.normalized_payload),
            "confidence": candidate_payload.get("confidence"),
            "status": candidate_payload.get("status"),
            "parse_mode": "strict",
        },
    )

    finalized_payload = dict(candidate_payload)
    if isinstance(validation_result.normalized_payload, Mapping):
        for field_name in ("branch", "report_date"):
            value = validation_result.normalized_payload.get(field_name)
            if isinstance(value, str) and value.strip():
                finalized_payload[field_name] = value.strip()

    finalized_result = AgentResult(
        agent_name=candidate_result.agent_name,
        payload=finalized_payload,
        metadata=_strict_write_metadata(
            candidate_result=candidate_result,
            routed_work_item=routed_work_item,
            validation_result=validation_result,
            acceptance_result=acceptance_result,
        ),
    )

    write_result = _write_final_structured_result(finalized_result)
    if write_result is not None:
        _apply_final_governance(finalized_result, write_result)
        review_queue_path = _maybe_write_strict_review_item(
            routed_work_item=routed_work_item,
            result=finalized_result,
            validation_result=validation_result,
            acceptance_result=acceptance_result,
            raw_audit=raw_audit,
            candidate_payload=finalized_payload,
        )
        if review_queue_path is not None:
            finalized_result.metadata["review_queue_path"] = review_queue_path
        return finalized_result

    governance_status = acceptance_result.governed_status(
        warning_codes=[warning.get("code", "") for warning in _result_warnings(finalized_result)]
    )
    governance_reasons = _governance_reasons_without_write(finalized_result)
    finalized_result.payload["status"] = governance_status
    finalized_result.payload["export_allowed"] = False
    finalized_result.payload["governance"] = _governance_outcome_payload(
        status=governance_status,
        reasons=governance_reasons,
        export_allowed=False,
    )
    review_queue_path = _maybe_write_strict_review_item(
        routed_work_item=routed_work_item,
        result=finalized_result,
        validation_result=validation_result,
        acceptance_result=acceptance_result,
        raw_audit=raw_audit,
        candidate_payload=finalized_payload,
    )
    if review_queue_path is not None:
        finalized_result.metadata["review_queue_path"] = review_queue_path
    return finalized_result


def _strict_write_metadata(
    *,
    candidate_result: AgentResult,
    routed_work_item: WorkItem,
    validation_result,
    acceptance_result,
) -> dict[str, Any]:
    """Return the central sidecar metadata for one strict candidate result."""

    metadata = dict(candidate_result.metadata) if isinstance(candidate_result.metadata, dict) else {}
    existing_validation = metadata.get("validation")
    if isinstance(existing_validation, Mapping):
        metadata["candidate_validation"] = dict(existing_validation)

    routed_payload = routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}
    validation_payload = validation_result.to_payload()
    if isinstance(existing_validation, Mapping):
        candidate_details = existing_validation.get("details")
        if isinstance(candidate_details, Mapping) and candidate_details.get("parser_failure") is True:
            validation_payload["details"] = {
                "parser_failure": True,
                "candidate_final_status": candidate_details.get("final_status"),
            }
    metadata["validation"] = validation_payload
    metadata["acceptance"] = acceptance_result.to_payload()
    metadata["governance_context"] = {
        **_mapping(metadata.get("governance_context")),
        **_routing_governance_context(routed_payload),
    }
    return metadata


def _routing_governance_context(routed_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return governance context from the routed work item without losing raw links."""

    governance_context = {
        "classified_report_type": _string_or_default(
            _mapping(routed_payload.get("classification")).get("report_type"),
            default="unknown",
        )
    }
    raw_record = routed_payload.get("raw_record")
    if isinstance(raw_record, Mapping):
        for field_name in ("raw_meta_path", "raw_sha256"):
            value = raw_record.get(field_name)
            if isinstance(value, str) and value.strip():
                governance_context[field_name] = value.strip()
    ingress_envelope = routed_payload.get("ingress_envelope")
    if isinstance(ingress_envelope, Mapping):
        payload = ingress_envelope.get("payload")
        if isinstance(payload, Mapping):
            message_id = payload.get("message_id")
            if isinstance(message_id, str) and message_id.strip():
                governance_context["message_id"] = message_id.strip()
    return governance_context


def _write_final_structured_result(result: AgentResult):
    """Persist one finalized strict result through the existing governed writers."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    if payload.get("status") == "invalid_input":
        return None

    if result.agent_name == "sales_income_agent":
        return sales_record_store.write_structured_record(payload, metadata=result.metadata)
    if result.agent_name in {"hr_agent", "staff_performance_agent"}:
        return hr_record_store.write_structured_record(payload, metadata=result.metadata)
    if result.agent_name == "pricing_stock_release_agent":
        return pricing_record_store.write_structured_record(payload, metadata=result.metadata)
    if result.agent_name == "supervisor_control_agent":
        return supervisor_record_store.write_structured_record(payload, metadata=result.metadata)
    return None


def _apply_final_governance(result: AgentResult, write_result: object) -> None:
    """Project the final governed write result back onto the live payload."""

    governance = getattr(write_result, "governance", None)
    if governance is None:
        return
    result.payload["status"] = governance.status
    result.payload["export_allowed"] = governance.export_allowed
    result.payload["governance"] = governance.to_payload()


def _maybe_write_strict_review_item(
    *,
    routed_work_item: WorkItem,
    result: AgentResult,
    validation_result,
    acceptance_result,
    raw_audit: RawAuditRecord,
    candidate_payload: Mapping[str, Any],
) -> str | None:
    """Write one strict-path review item when governance or acceptance requires it."""

    governance = _result_governance(result)
    governance_status = _string_or_none(governance.get("status"))
    if governance_status not in {"needs_review", "conflict_blocked"}:
        return None

    payload = result.payload if isinstance(result.payload, dict) else {}
    review_reason = _strict_review_reason(
        governance=governance,
        acceptance_result=acceptance_result,
        candidate_payload=candidate_payload,
    )
    return write_review_item(
        routed_work_item,
        report_type=specialist_report_type_from_payload(payload),
        branch=_string_or_default(payload.get("branch"), default="unknown"),
        report_date=_string_or_default(payload.get("report_date"), default="unknown"),
        confidence=_result_confidence(result),
        warnings=_result_warnings(result),
        reason=review_reason,
        validation_outcome=validation_result.to_payload(),
        acceptance_outcome=acceptance_result.to_payload(),
        governance_outcome=governance,
        candidate_payload=dict(candidate_payload),
        raw_paths={
            "raw_text_path": str(raw_audit.text_path),
            "raw_meta_path": str(raw_audit.meta_path),
        },
        parser_used=result.agent_name,
        parse_mode="strict",
    )


def specialist_report_type_from_payload(payload: Mapping[str, Any]) -> str:
    """Return the specialist report type implied by the finalized payload."""

    signal_type = _string_or_none(payload.get("signal_type"))
    signal_subtype = _string_or_none(payload.get("signal_subtype"))
    if signal_subtype == "staff_attendance":
        return "staff_attendance"
    if signal_subtype == "staff_performance":
        return "staff_performance"
    if signal_type == "sales_income":
        return "sales"
    if signal_type == "pricing_stock_release":
        return "bale_summary"
    if signal_type == "supervisor_control":
        return "supervisor_control"
    return "unknown"


def _strict_review_reason(
    *,
    governance: Mapping[str, Any],
    acceptance_result,
    candidate_payload: Mapping[str, Any],
) -> str:
    """Return the stable review reason used for strict-path review queue records."""

    reasons = governance.get("reasons")
    if isinstance(reasons, list):
        for reason in reasons:
            if isinstance(reason, str) and reason.strip():
                return reason.strip()
    if acceptance_result.decision == "review":
        return acceptance_result.reason
    review_policy = candidate_payload.get("review_policy")
    if isinstance(review_policy, Mapping):
        final_status = review_policy.get("final_status")
        if isinstance(final_status, str) and final_status.strip():
            return final_status.strip()
    return "needs_review"


def _governance_reasons_without_write(result: AgentResult) -> list[str]:
    """Return fallback governance reasons when no governed structured write occurs."""

    metadata = result.metadata if isinstance(result.metadata, dict) else {}
    validation = _mapping(metadata.get("validation"))
    details = _mapping(validation.get("details"))
    reasons: list[str] = []
    if details.get("parser_failure") is True:
        reasons.append("parser_failure")
    if validation.get("accepted") is False:
        reasons.append("insufficient_structure")
    if not reasons:
        reasons.append("insufficient_structure")
    return reasons


def _fallback_validation_payload(
    *,
    normalized_report: Mapping[str, Any],
    routing_payload: object,
) -> dict[str, Any]:
    """Return the payload sent to SOP validation for fallback extraction."""

    payload = dict(normalized_report)
    report_date = payload.get("report_date")
    if not isinstance(report_date, str) or not report_date.strip():
        if isinstance(routing_payload, Mapping):
            for field_name in ("normalized_report_date", "report_date", "raw_report_date"):
                candidate = routing_payload.get(field_name)
                if isinstance(candidate, str) and candidate.strip():
                    report_date = candidate.strip()
                    break
    if isinstance(report_date, str) and report_date.strip():
        date_result = normalize_report_date(report_date)
        if date_result.normalized_value is not None:
            payload["report_date"] = date_result.normalized_value
    return payload


def _downgrade_resolved_fallback_date_rejection(
    *,
    report_type: str,
    validation_result,
    fallback_payload: Mapping[str, Any],
) -> None:
    """Convert resolvable fallback date-format failures into warnings."""

    rejections = getattr(validation_result, "rejections", None)
    if not isinstance(rejections, list) or not rejections:
        return

    invalid_date_rejections = [rejection for rejection in rejections if getattr(rejection, "code", None) == "invalid_report_date"]
    if not invalid_date_rejections or len(invalid_date_rejections) != len(rejections):
        return

    normalized_payload = getattr(validation_result, "normalized_payload", None)
    if not isinstance(normalized_payload, Mapping):
        return
    normalized_report_date = normalized_payload.get("report_date")
    if not isinstance(normalized_report_date, str) or not is_iso_date(normalized_report_date):
        return

    normalization = getattr(validation_result, "normalization", None)
    normalization_report_date = normalization.get("report_date") if isinstance(normalization, Mapping) else None
    raw_report_date = normalization_report_date.get("raw") if isinstance(normalization_report_date, Mapping) else None
    if not isinstance(raw_report_date, str) or not raw_report_date.strip() or is_iso_date(raw_report_date):
        return

    confidence = fallback_payload.get("confidence")
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool):
        return
    if float(confidence) <= get_report_policy(report_type).confidence_thresholds.reject_max:
        return

    warnings = fallback_payload.get("warnings")
    if not isinstance(warnings, list):
        return
    if any(isinstance(warning, Mapping) and warning.get("code") == "ambiguous_report_date" for warning in warnings):
        return

    validation_result.rejections = []
    validation_result.accepted = True
    warnings.append(
        _make_warning(
            code="invalid_report_date",
            severity="warning",
            message="Fallback validation accepted the normalized report_date after resolving a non-ISO raw date.",
        )
    )


def _process_mixed_work_item(
    work_item: WorkItem,
    *,
    raw_audit: RawAuditRecord,
    mixed_detection,
    split_result,
    policy_decision: PolicyDecision,
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
        strict_child_work_item = _with_candidate_mode(child_work_item)
        try:
            child_result = _dispatch_to_specialist(strict_child_work_item, target_agent=route.target_agent)
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

        child_result = _finalize_strict_candidate(
            routed_work_item=child_work_item,
            candidate_result=child_result,
            specialist_report_type=route.specialist_type,
            raw_audit=raw_audit,
        )

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
        processing_status="processed" if parent_status in {"accepted_split", "accepted_with_warning"} else "rejected",
        branch_hint=branch_hint,
        routing_metadata=_routing_metadata_from_payload(routing_payload),
        policy_decision=policy_decision,
        governance_outcome=_governance_outcome_payload(
            status="accepted_with_warning" if parent_status in {"accepted_split", "accepted_with_warning"} else "needs_review",
            reasons=[],
            export_allowed=False,
        ),
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

    processing_text = _extract_processing_text(payload)
    if not processing_text.strip():
        raw_text = raw_message.get("text")
        message = "The work item must provide a non-empty routing text value."
        if not isinstance(payload.get("cleaned_text"), str) and (not isinstance(raw_text, str) or not raw_text.strip()):
            message = "The work item raw_message.text field must be a non-empty string."
        warnings.append(
            _make_warning(
                code="missing_fields",
                severity="error",
                message=message,
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
        existing_metadata=_load_existing_metadata_for_dedup(
            meta_path=meta_path,
            raw_sha256=raw_sha256,
            received_at=received_at,
            replay=replay.get("is_replay") is True,
        ),
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
            policy_decision=None,
        ),
    )
    return audit


def _update_raw_metadata(
    audit: RawAuditRecord,
    *,
    detected_report_type: ClassificationLabel,
    routing_target: str | None,
    processing_status: RawProcessingStatus,
    branch_hint: str | None,
    routing_metadata: dict[str, Any] | None,
    policy_decision: PolicyDecision | None,
    governance_outcome: Mapping[str, Any] | None = None,
    extra_metadata: dict[str, Any] | None = None,
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
            policy_decision=policy_decision,
            governance_outcome=governance_outcome,
            extra_metadata=extra_metadata,
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
    policy_decision: PolicyDecision | None,
    extra_metadata: dict[str, Any] | None = None,
) -> Path:
    """Write the rejected quarantine copy and its metadata."""

    rejected_bucket = _storage_bucket_for_classification(attempted_report_type)
    filename = build_rejected_filename(attempted_report_type, rejection_reason)
    text_path = get_rejected_path(rejected_bucket) / filename
    meta_path = text_path.with_suffix(".meta.json")
    write_text_file(text_path, audit.raw_text)
    payload = {
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
        "policy_guard": policy_decision.to_metadata() if policy_decision is not None else None,
    }
    if extra_metadata:
        payload.update(extra_metadata)
    write_json_file(meta_path, payload)
    _write_outcome_provenance(
        outcome="rejected",
        audit=audit,
        report_type=attempted_report_type,
        branch=attempted_branch_hint or "unknown",
        report_date=_provenance_report_date(extra_metadata, audit),
        parser_used=attempted_agent or AGENT_NAME,
        parse_mode=_provenance_parse_mode(extra_metadata),
        confidence=_provenance_confidence(extra_metadata),
        warnings=_provenance_warnings(extra_metadata),
        validation_outcome=_provenance_validation(extra_metadata),
        acceptance_outcome=_provenance_acceptance(extra_metadata),
        downstream_references={
            "rejected_text_path": str(text_path),
            "rejected_meta_path": str(meta_path),
        },
    )
    return text_path


def _build_routed_work_item(work_item: WorkItem) -> WorkItem:
    """Create the minimal safe routed work item for exactly one specialist agent."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_message = payload.get("raw_message")
    text = _extract_processing_text(payload)
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
    normalization = normalize_report(
        text,
        report_family=routing_decision.detected_report_type,
        routing_context={
            "branch_hint": routing_decision.branch_hint,
            "report_date": routing_decision.report_date,
            "raw_report_date": routing_decision.raw_report_date,
            "report_type": routing_decision.specialist_report_type,
        },
    )
    normalized_branch_hint = (
        routing_decision.branch_hint
        or normalization.normalized_fields.get("branch")
    )
    normalized_report_date = (
        routing_decision.report_date
        or normalization.normalized_fields.get("report_date")
    )
    normalized_raw_report_date = (
        routing_decision.raw_report_date
        or (
            normalization.report_date.raw_value
            if normalization.report_date is not None and normalization.report_date.normalized_value is not None
            else None
        )
    )
    review_reason = routing_decision.review_reason
    processing_status = routing_decision.processing_status
    if (
        routing_decision.routing_target is not None
        and normalized_branch_hint is not None
        and normalized_report_date is not None
    ):
        processing_status = "routed"
        review_reason = None
    normalization_evidence = [
        f"normalization:{rule.name}:{rule.normalized_value}"
        for rule in normalization.provenance
        if rule.normalized_value is not None
    ]

    routed_payload: dict[str, Any] = {
        "raw_message": {
            "text": text,
            "normalized_text": normalization.normalized_text or text,
        },
        "classification": {
            "report_family": routing_decision.detected_report_type,
            "report_type": routing_decision.specialist_report_type,
            "confidence": family_classification.confidence,
            "evidence": family_classification.evidence,
        },
        "normalization": normalization.to_payload(),
        "routing": _build_routing_payload(
            classification=routing_decision.detected_report_type,
            target_agent=routing_decision.routing_target,
            status=processing_status if processing_status in {"routed", "needs_review"} else "needs_review",
            route_reason="classified_for_specialist" if routing_decision.routing_target is not None else "unknown_route",
            branch_hint=normalized_branch_hint,
            report_date=normalized_report_date,
            raw_report_date=normalized_raw_report_date,
            confidence=routing_decision.confidence,
            evidence=routing_decision.evidence + normalization_evidence,
            normalized_header_candidates=routing_decision.normalized_header_candidates,
            review_reason=review_reason,
            specialist_report_type=routing_decision.specialist_report_type,
        ),
    }

    source = payload.get("source")
    if isinstance(source, str) and source.strip():
        routed_payload["source"] = source

    if metadata:
        if normalized_branch_hint is not None:
            metadata["branch_hint"] = normalized_branch_hint
        routed_payload["metadata"] = metadata

    replay = _sanitize_replay(payload.get("replay"))
    if replay:
        routed_payload["replay"] = replay

    raw_record = _sanitize_raw_record(payload.get("raw_record"))
    if raw_record:
        routed_payload["raw_record"] = raw_record

    cleaned_text = payload.get("cleaned_text")
    if isinstance(cleaned_text, str):
        routed_payload["cleaned_text"] = cleaned_text

    pre_ingestion_validation = _sanitize_pre_ingestion_validation(payload.get("pre_ingestion_validation"))
    if pre_ingestion_validation:
        routed_payload["pre_ingestion_validation"] = pre_ingestion_validation

    ingress_envelope = payload.get("ingress_envelope")
    if isinstance(ingress_envelope, Mapping):
        routed_payload["ingress_envelope"] = dict(ingress_envelope)

    return WorkItem(kind=RAW_MESSAGE_KIND, payload=routed_payload)


def _raw_metadata_payload(
    audit: RawAuditRecord,
    *,
    detected_report_type: ClassificationLabel,
    routing_target: str | None,
    processing_status: str,
    branch_hint: str | None,
    routing_metadata: dict[str, Any] | None,
    policy_decision: PolicyDecision | None,
    governance_outcome: Mapping[str, Any] | None = None,
    extra_metadata: dict[str, Any] | None = None,
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
        "policy_guard": policy_decision.to_metadata() if policy_decision is not None else None,
        "governance_status": None,
        "governance_reasons": [],
        "export_allowed": False,
    }
    if routing_metadata:
        payload.update(routing_metadata)
    if isinstance(governance_outcome, Mapping):
        payload["governance_status"] = governance_outcome.get("status")
        reasons = governance_outcome.get("reasons")
        payload["governance_reasons"] = list(reasons) if isinstance(reasons, list) else []
        payload["export_allowed"] = governance_outcome.get("export_allowed") is True
    if extra_metadata:
        payload.update(extra_metadata)
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


def _string_or_none(value: object) -> str | None:
    """Return one stripped string or `None`."""

    return _sanitize_optional_text(value)


def _mapping(value: object) -> Mapping[str, Any]:
    """Return one mapping-like object or an empty mapping."""

    return value if isinstance(value, Mapping) else {}


def _with_candidate_mode(work_item: WorkItem) -> WorkItem:
    """Return a routed work item that asks specialists for candidates only."""

    payload = dict(work_item.payload) if isinstance(work_item.payload, dict) else {}
    payload["governance_mode"] = "candidate"
    return WorkItem(kind=work_item.kind, payload=payload)


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


def _sanitize_pre_ingestion_validation(validation: object) -> dict[str, Any]:
    """Keep one validator payload only when it follows the expected shape."""

    if not isinstance(validation, Mapping):
        return {}
    return dict(validation)


def _reject_mixed_reports(payload: dict[str, Any]) -> bool:
    """Return whether ingress policy requires mixed reports to be rejected."""

    ingress_policy = payload.get("ingress_policy")
    return isinstance(ingress_policy, Mapping) and ingress_policy.get("reject_mixed_reports") is True


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


def _load_existing_metadata_for_dedup(
    *,
    meta_path: Path,
    raw_sha256: str,
    received_at: str,
    replay: bool,
) -> dict[str, Any]:
    """Return the most relevant existing raw metadata for duplicate detection."""

    exact_metadata = _load_existing_metadata(meta_path)
    if replay:
        return exact_metadata

    current_received_at = _parse_iso8601_timestamp(received_at)
    if current_received_at is None:
        return exact_metadata

    best_match = (
        _candidate_duplicate_metadata(
            payload=exact_metadata,
            raw_sha256=raw_sha256,
            current_received_at=current_received_at,
        )
        if _is_reusable_exact_metadata(exact_metadata)
        else None
    )

    for candidate_path in get_raw_path(UNKNOWN_STORAGE_BUCKET).glob("*.meta.json"):
        if candidate_path == meta_path:
            continue
        candidate_payload = _load_existing_metadata(candidate_path)
        candidate_match = _candidate_duplicate_metadata(
            payload=candidate_payload,
            raw_sha256=raw_sha256,
            current_received_at=current_received_at,
        )
        if candidate_match is None:
            continue
        if best_match is None or candidate_match[0] > best_match[0]:
            best_match = candidate_match

    return best_match[1] if best_match is not None else exact_metadata


def _candidate_duplicate_metadata(
    *,
    payload: dict[str, Any],
    raw_sha256: str,
    current_received_at: datetime,
) -> tuple[datetime, dict[str, Any]] | None:
    """Return duplicate-candidate metadata when the hash matches inside the dedup window."""

    if _sanitize_optional_text(payload.get("raw_sha256")) != raw_sha256:
        return None

    existing_received_at = _parse_iso8601_timestamp(payload.get("received_at"))
    if existing_received_at is None:
        return None

    age = current_received_at - existing_received_at
    if age < timedelta(0) or age > RAW_SHA256_DEDUP_WINDOW:
        return None
    return existing_received_at, payload


def _is_reusable_exact_metadata(payload: dict[str, Any]) -> bool:
    """Return whether exact-path metadata predates the current ingest enough to seed dedup."""

    if not payload:
        return False
    processing_status = _sanitize_optional_text(payload.get("processing_status"))
    if processing_status and processing_status != "received":
        return True
    return isinstance(payload.get("policy_guard"), Mapping)


def _parse_iso8601_timestamp(value: object) -> datetime | None:
    """Return one timezone-aware timestamp when the input is parseable."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_raw_text(payload: dict[str, object]) -> str:
    """Return the inbound raw text or an empty string for invalid input."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""

    text = raw_message.get("text")
    if not isinstance(text, str):
        return ""
    return text


def _extract_processing_text(payload: dict[str, object]) -> str:
    """Return cleaned text when explicitly provided, otherwise the raw text."""

    cleaned_text = payload.get("cleaned_text")
    if isinstance(cleaned_text, str):
        return cleaned_text
    return _extract_raw_text(payload)


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


def _raw_processing_status_for_result(status: str) -> RawProcessingStatus:
    """Return the raw-audit processing status for one governed result."""

    if status == "duplicate":
        return "duplicate"
    if status in {"rejected", "conflict_blocked", "invalid_input"}:
        return "rejected"
    return "processed"


def _failure_result(
    work_item: WorkItem,
    *,
    classification: ClassificationLabel,
    status: RouteStatus,
    route_reason: str,
    warnings: list[dict[str, str]],
    policy_decision: PolicyDecision | None = None,
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
            "export_allowed": False,
            "governance": {
                "status": "duplicate" if status == "duplicate" else "rejected" if status in {"rejected", "invalid_input"} else status,
                "export_allowed": False,
                "reasons": [route_reason],
            },
            "policy_guard": policy_decision.to_metadata() if policy_decision is not None else None,
        },
    )


def _build_fallback_result(
    *,
    routed_work_item: WorkItem,
    classification: ClassificationLabel,
    route_reason: str,
    fallback_payload: dict[str, Any],
    validation_result,
    acceptance_payload: dict[str, Any],
    warnings: list[dict[str, str]],
    policy_decision: PolicyDecision,
    status: RouteStatus,
    review_queue_path: str | None,
) -> AgentResult:
    """Return an orchestrator result for fallback extraction outcomes."""

    result = _failure_result(
        routed_work_item,
        classification=classification,
        status=status,
        route_reason=route_reason,
        warnings=warnings,
        policy_decision=policy_decision,
    )
    result.payload["fallback"] = {
        "parse_mode": fallback_payload.get("parse_mode"),
        "confidence": fallback_payload.get("confidence"),
        "warnings": list(fallback_payload.get("warnings", [])) if isinstance(fallback_payload.get("warnings"), list) else [],
        "provenance": dict(fallback_payload.get("provenance", {})) if isinstance(fallback_payload.get("provenance"), Mapping) else {},
        "normalized_report": dict(validation_result.normalized_payload),
        "validation": validation_result.to_payload(),
        "acceptance": acceptance_payload,
        "review_queue_path": review_queue_path,
    }
    result.payload["confidence"] = fallback_payload.get("confidence", 0.0)
    return result


def _write_outcome_provenance(
    *,
    outcome: str,
    audit: RawAuditRecord,
    report_type: str,
    branch: str,
    report_date: str,
    parser_used: str,
    parse_mode: str,
    confidence: float | None,
    warnings: list[dict[str, Any]],
    validation_outcome: dict[str, Any],
    acceptance_outcome: dict[str, Any],
    downstream_references: dict[str, Any],
) -> str:
    """Persist one dedicated provenance record."""

    return write_provenance_record(
        outcome=outcome,
        report_type=report_type,
        branch=branch,
        report_date=report_date,
        raw_message_hash=audit.raw_sha256,
        parser_used=parser_used,
        parse_mode=parse_mode,
        confidence=confidence,
        warnings=warnings,
        validation_outcome=validation_outcome,
        acceptance_outcome=acceptance_outcome,
        downstream_references=downstream_references,
        extra={
            "source": audit.source,
            "received_at": audit.received_at,
            "raw_text_path": str(audit.text_path),
            "raw_meta_path": str(audit.meta_path),
        },
    )


def _fallback_warnings(*, fallback_payload: dict[str, Any], validation_result) -> list[dict[str, str]]:
    """Return combined fallback and validation warnings."""

    warnings: list[dict[str, str]] = []
    fallback_entries = fallback_payload.get("warnings")
    if isinstance(fallback_entries, list):
        warnings.extend(entry for entry in fallback_entries if isinstance(entry, dict))
    warnings.extend(_rejection_to_warning(rejection.to_payload()) for rejection in validation_result.rejections)
    return warnings


def _fallback_metadata(
    *,
    fallback_payload: dict[str, Any],
    validation_result,
    acceptance_payload: dict[str, Any],
    review_queue_path: str | None,
) -> dict[str, Any]:
    """Return fallback metadata for raw and rejected audit records."""

    return {
        "fallback_parse_mode": fallback_payload.get("parse_mode"),
        "fallback_confidence": fallback_payload.get("confidence"),
        "fallback_status": fallback_payload.get("status"),
        "fallback_validation": validation_result.to_payload(),
        "fallback_acceptance": acceptance_payload,
        "fallback_review_queue_path": review_queue_path,
        "fallback_provenance": dict(fallback_payload.get("provenance", {})) if isinstance(fallback_payload.get("provenance"), Mapping) else {},
    }


def _rejection_to_warning(rejection_payload: dict[str, Any]) -> dict[str, str]:
    """Translate one validation rejection into a warning-like payload."""

    return _make_warning(
        code=str(rejection_payload.get("code") or "validation_rejection"),
        severity="error",
        message=str(rejection_payload.get("message") or "Validation rejected the fallback extraction."),
    )


def _string_or_default(value: object, *, default: str) -> str:
    """Return a stripped string value or a default."""

    if isinstance(value, str) and value.strip():
        return value.strip()
    return default


def _result_confidence(result: AgentResult) -> float | None:
    """Return one result confidence when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    confidence = payload.get("confidence")
    if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
        return float(confidence)
    return None


def _result_governance(result: AgentResult) -> dict[str, Any]:
    """Return the final governance payload for one result when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    governance = payload.get("governance")
    if isinstance(governance, Mapping):
        return dict(governance)
    return _governance_outcome_payload(status=_result_status(result), reasons=[])


def _result_validation_outcome(result: AgentResult) -> dict[str, Any]:
    """Return the central validation outcome for provenance."""

    metadata = result.metadata if isinstance(result.metadata, dict) else {}
    validation = metadata.get("validation")
    if isinstance(validation, Mapping):
        return dict(validation)
    return {"status": "not_run"}


def _result_acceptance_outcome(result: AgentResult) -> dict[str, Any]:
    """Return the central acceptance outcome for provenance."""

    metadata = result.metadata if isinstance(result.metadata, dict) else {}
    acceptance = metadata.get("acceptance")
    if isinstance(acceptance, Mapping):
        return dict(acceptance)
    return {"status": _result_status(result)}


def _result_metadata_extension(result: AgentResult) -> dict[str, Any] | None:
    """Return extra raw-metadata fields derived from the finalized result."""

    metadata = result.metadata if isinstance(result.metadata, dict) else {}
    payload = result.payload if isinstance(result.payload, dict) else {}
    extra: dict[str, Any] = {
        "validation": _result_validation_outcome(result),
        "acceptance": _result_acceptance_outcome(result),
        "candidate_payload": dict(payload),
    }
    review_queue_path = metadata.get("review_queue_path")
    if isinstance(review_queue_path, str) and review_queue_path.strip():
        extra["review_queue_path"] = review_queue_path.strip()
    return extra


def _governance_outcome_payload(
    *,
    status: str,
    reasons: list[str],
    export_allowed: bool = False,
) -> dict[str, Any]:
    """Return the raw-metadata and review-queue governance payload."""

    return {
        "status": status,
        "export_allowed": export_allowed,
        "reasons": list(reasons),
    }


def _classification_confidence(work_item: WorkItem) -> float | None:
    """Return the specialist-family classification confidence when present."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    classification = payload.get("classification")
    if not isinstance(classification, Mapping):
        return None
    confidence = classification.get("confidence")
    if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
        return float(confidence)
    return None


def _can_safely_split_mixed_report(*, mixed_detection, split_result) -> bool:
    """Return whether mixed content can be safely fanned out into children."""

    detected_family_count = len(set(mixed_detection.detected_families))
    if detected_family_count < 2:
        return False
    if split_result.split_confidence < MIXED_SPLIT_CONFIDENCE_MIN:
        return False
    if len(split_result.segments) < detected_family_count:
        return False
    return all(
        bool(segment.raw_text.strip()) and segment.split_confidence >= MIXED_SPLIT_CONFIDENCE_MIN
        for segment in split_result.segments
    )


def _result_warnings(result: AgentResult) -> list[dict[str, Any]]:
    """Return JSON-safe warnings from a result payload."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    warnings = payload.get("warnings")
    if isinstance(warnings, list):
        return [warning for warning in warnings if isinstance(warning, dict)]
    return []


def _provenance_report_date(extra_metadata: dict[str, Any] | None, audit: RawAuditRecord) -> str:
    """Return the best provenance date for rejected records."""

    if isinstance(extra_metadata, dict):
        for field_name in ("resolved_report_date", "fallback_validation", "fallback_provenance"):
            value = extra_metadata.get(field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if field_name == "fallback_validation" and isinstance(value, Mapping):
                report_type = value.get("report_type")
                if isinstance(report_type, str):
                    break
    return _date_segment(audit.received_at)


def _provenance_parse_mode(extra_metadata: dict[str, Any] | None) -> str:
    """Return rejected-record parse mode."""

    if isinstance(extra_metadata, dict):
        value = extra_metadata.get("fallback_parse_mode")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "strict"


def _provenance_confidence(extra_metadata: dict[str, Any] | None) -> float | None:
    """Return rejected-record confidence when available."""

    if isinstance(extra_metadata, dict):
        value = extra_metadata.get("fallback_confidence")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None


def _provenance_warnings(extra_metadata: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return rejected-record warnings when available."""

    warnings: list[dict[str, Any]] = []
    if not isinstance(extra_metadata, dict):
        return warnings
    provenance = extra_metadata.get("fallback_provenance")
    if isinstance(provenance, Mapping):
        pass
    validation = extra_metadata.get("fallback_validation")
    if isinstance(validation, Mapping):
        rejections = validation.get("rejections")
        if isinstance(rejections, list):
            for rejection in rejections:
                if isinstance(rejection, Mapping):
                    warnings.append(
                        _make_warning(
                            code=str(rejection.get("code") or "validation_rejection"),
                            severity="error",
                            message=str(rejection.get("message") or "Validation rejected the record."),
                        )
                    )
    return warnings


def _provenance_validation(extra_metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Return validation outcome for provenance."""

    if isinstance(extra_metadata, dict):
        value = extra_metadata.get("fallback_validation")
        if isinstance(value, Mapping):
            return dict(value)
    return {"status": "not_run"}


def _provenance_acceptance(extra_metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Return acceptance outcome for provenance."""

    if isinstance(extra_metadata, dict):
        value = extra_metadata.get("fallback_acceptance")
        if isinstance(value, Mapping):
            return dict(value)
    return {"status": "rejected"}


def _rejection_reason_from_validation(warnings: list[dict[str, str]]) -> RejectionReason:
    """Map validation warnings into stable rejection reason codes."""

    for warning in warnings:
        if warning.get("message") == "The work item raw_message.text field must be a non-empty string.":
            return "missing_raw_text"
    return "invalid_input"


def _policy_reason_to_governance_reason(reason: str) -> str:
    """Map one policy-guard reason into the governance taxonomy."""

    mapping = {
        "duplicate_message": "duplicate_message_id",
        "unknown_report_type": "unknown_report_type",
        "invalid_pricing_card_format": "invalid_pricing_card_format",
        "route_requires_review": "insufficient_structure",
    }
    return mapping.get(reason, "insufficient_structure")


def _rejection_reason_from_result(result: AgentResult) -> RejectionReason:
    """Map specialist invalid-input results into stable rejection reason codes."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    governance = payload.get("governance")
    if isinstance(governance, Mapping):
        reasons = governance.get("reasons")
        if isinstance(reasons, list):
            for reason in reasons:
                if reason in {
                    "duplicate_message_id",
                    "duplicate_raw_sha256",
                    "duplicate_semantic",
                    "conflicting_record_same_scope",
                }:
                    return reason
    if result.agent_name == "hr_agent":
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
    normalized_report_date: str | None = None,
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
        "normalized_report_date": normalized_report_date or report_date,
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
        "normalized_report_date": routing_payload.get("normalized_report_date"),
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
    if payload.get("status") in {"invalid_input", "rejected", "duplicate", "conflict_blocked"}:
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

    child_statuses = [
        summary.get("status")
        for summary in child_summaries
        if isinstance(summary.get("status"), str)
    ]
    if not child_statuses or len(child_statuses) != len(child_summaries):
        return "needs_review"

    success_statuses = {"accepted", "accepted_with_warning"}
    warning_statuses = {"accepted_with_warning"}
    failed_statuses = {
        "invalid_input",
        "needs_review",
        "rejected",
        "duplicate",
        "conflict_blocked",
    }

    child_status_set = set(child_statuses)

    if child_status_set & failed_statuses:
        return "needs_review"
    if not child_status_set <= success_statuses:
        return "needs_review"
    if child_status_set & warning_statuses:
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
    metadata_branch_hint = _sanitize_metadata(payload.get("metadata")).get("branch_hint")
    child_headers = normalize_headers(segment.raw_text)
    child_branch_resolution = resolve_branch(child_headers, metadata_branch_hint=metadata_branch_hint)
    child_date_resolution = resolve_report_date(child_headers)
    child_normalization = normalize_report(
        segment.raw_text,
        report_family=segment.detected_report_family,
        routing_context={
            "branch_hint": child_branch_resolution.branch_hint,
            "report_date": child_date_resolution.iso_date,
            "raw_report_date": child_date_resolution.raw_date,
            "report_type": specialist_report_type,
        },
    )
    normalized_branch_hint = (
        child_branch_resolution.branch_hint
        or child_normalization.normalized_fields.get("branch")
        or metadata_branch_hint
    )
    normalized_report_date = (
        child_date_resolution.iso_date
        or child_normalization.normalized_fields.get("report_date")
    )
    raw_report_date = (
        child_date_resolution.raw_date
        or (
            child_normalization.report_date.raw_value
            if child_normalization.report_date is not None and child_normalization.report_date.raw_value
            else None
        )
    )
    payload["raw_message"] = {
        "text": segment.raw_text,
        "normalized_text": child_normalization.normalized_text or segment.raw_text,
    }
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
        "branch_hint": normalized_branch_hint,
        "report_date": normalized_report_date,
        "normalized_report_date": normalized_report_date,
        "raw_report_date": raw_report_date,
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
