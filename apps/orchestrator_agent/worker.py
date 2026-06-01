"""Thin intake-and-routing worker for upstream operational messages.

Live ingestion writes raw first, may update raw metadata after routing, keeps
rejected as a quarantine copy rather than the only copy, and treats structured
records as the usable source of truth. Replay is read-only at the raw archive
layer and suppresses raw writes when `payload["replay"]["is_replay"]` is true.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Final, Literal

from apps.branch_resolver_agent.worker import resolve_branch
from apps.date_resolver_agent.worker import resolve_report_date
from apps.fallback_extraction_agent.worker import (
    process_work_item as process_fallback_extraction_work_item,
)
from apps.header_normalizer_agent.worker import normalize_headers
import apps.hr_agent.record_store as hr_record_store
from apps.hr_agent.warnings import is_blocking_accountability_rule
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
from packages.record_store.duplicate_archive import archive_duplicate_record
from packages.record_store.paths import get_raw_path, get_rejected_path, get_structured_path
from packages.record_store.writer import write_json_file, write_text_file
from packages.human_tolerance import analyze_human_whatsapp_text
from packages.normalization.dates import normalize_report_date
from packages.normalization.engine import normalize_report
from packages.report_acceptance import decide_acceptance
from packages.report_policy import get_report_policy
from packages.provenance_store import write_provenance_record
from packages.report_registry import APPROVED_MIXED_SPLIT_TITLES, route_for_family
from packages.review_queue import write_review_item
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.sop_validation.router import validate_report
from packages.sop_validation.common import is_iso_date

AGENT_NAME: Final[str] = "orchestrator_agent"
RUNTIME_STATUS = "LIVE_RUNTIME"
RUNTIME_OWNER = "orchestrator_agent"
RUNTIME_NOTE = (
    "Live runtime orchestrator. Legacy apps.orchestra compatibility code is "
    "not the active orchestration owner."
)
RAW_MESSAGE_KIND: Final[str] = "raw_message"
SIGNAL_TYPE: Final[str] = "routing"
UNKNOWN_STORAGE_BUCKET: Final[str] = "unknown"
CLASSIFICATION_CONFIDENCE_MIN_FOR_FALLBACK: Final[float] = 0.45
MIXED_SPLIT_CONFIDENCE_MIN: Final[float] = 0.85
RAW_SHA256_DEDUP_WINDOW: Final[timedelta] = timedelta(hours=24)
INTELLIGENCE_REPORT_FAMILY: Final[str] = "intelligence"
INTELLIGENCE_SPECIALIST_REPORT_TYPES: Final[frozenset[str]] = frozenset({"supervisor_control"})
MIXED_CHILD_REVIEW_WARNING_CODES: Final[frozenset[str]] = frozenset({"child_report_incomplete_or_truncated"})
_TRUNCATED_SUPERVISOR_FIELD_ALIASES: Final[tuple[str, ...]] = (
    "cash variance",
    "staffing issues",
    "stock issues affecting sales",
    "pricing or system issues",
    "exceptions escalated to ops manager",
    "supervisor confirmation",
    "supervisor confirmed",
    "exception type",
    "details",
    "action taken",
    "escalated by",
    "time",
)

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
    normalized_text_hash: str | None = None
    human_tolerance: dict[str, Any] = field(default_factory=dict)
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

    work_item = _with_human_tolerance(work_item)
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

    # Mixed-report detection and splitting must use the original raw text.
    # The human_tolerance layer may normalize/flatten WhatsApp text, which is
    # useful for field tolerance but unsafe for structural section splitting.
    split_source_text = _extract_raw_text(payload) or processing_text

    mixed_detection = detect_mixed_content(split_source_text)
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
        split_result = split_report(split_source_text, mixed_detection)
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

    ingress_policy_decision = evaluate_pre_specialist_policy(
        existing_metadata=raw_audit.existing_metadata,
        report_family=classification,
        report_type=specialist_report_type if isinstance(specialist_report_type, str) else None,
        target_agent=target_agent if isinstance(target_agent, str) else None,
        route_status=route_status if isinstance(route_status, str) else None,
    )
    duplicate_override = _duplicate_override_active(ingress_policy_decision)
    if duplicate_override:
        _archive_duplicate_for_disposal(
            raw_audit,
            source_message_id=_ingress_field(routed_work_item.payload, "message_id"),
            sender_phone=_ingress_field(routed_work_item.payload, "sender_phone"),
            branch=resolved_branch_hint or raw_audit.branch_hint,
            report_type=specialist_report_type if isinstance(specialist_report_type, str) else classification,
            report_date=_string_or_none(routing_payload.get("report_date")),
            duplicate_reason="duplicate_message",
            duplicate_basis=ingress_policy_decision.duplicate_basis,
            original_or_duplicate_of=_string_or_none(raw_audit.existing_metadata.get("raw_meta_path")),
        )
        policy_decision = evaluate_pre_specialist_policy(
            existing_metadata={},
            report_family=classification,
            report_type=specialist_report_type if isinstance(specialist_report_type, str) else None,
            target_agent=target_agent if isinstance(target_agent, str) else None,
            route_status=route_status if isinstance(route_status, str) else None,
        )
    else:
        policy_decision = ingress_policy_decision

    if policy_decision.action == "reject":
        if classification == "invalid_pricing_card_format":
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
        failure_result = _failure_result(
            routed_work_item,
            classification=classification,
            status="rejected",
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
        if duplicate_override:
            _annotate_duplicate_override_result(
                failure_result,
                raw_audit=raw_audit,
                duplicate_policy_decision=ingress_policy_decision,
            )
        _update_raw_metadata(
            raw_audit,
            detected_report_type=classification,
            routing_target=None,
            processing_status="rejected",
            branch_hint=resolved_branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
            policy_decision=policy_decision,
            governance_outcome=_governance_outcome_payload(
                status="rejected",
                reasons=[_policy_reason_to_governance_reason(policy_decision.reason)],
            ),
            extra_metadata=_result_metadata_extension(failure_result),
        )
        if not duplicate_override:
            _write_rejected_record(
                raw_audit,
                rejection_reason=rejection_reason,
                attempted_report_type=classification,
                attempted_agent=None,
                attempted_branch_hint=resolved_branch_hint,
                exception_message=None,
                policy_decision=policy_decision,
                extra_metadata={
                    "accountability": _build_generic_accountability(
                        payload={},
                        validation_outcome=None,
                        acceptance_outcome=None,
                        governance_outcome={
                            "status": "rejected",
                            "reasons": [route_reason],
                        },
                        warnings=[
                            _make_warning(
                                code=warning_code,
                                severity="warning",
                                message=warning_message,
                            )
                        ],
                        status="rejected",
                        route_reason=route_reason,
                        rejection_reason=rejection_reason,
                    )
                },
            )
        return failure_result

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
            if duplicate_override:
                _annotate_duplicate_override_result(
                    fallback_result,
                    raw_audit=raw_audit,
                    duplicate_policy_decision=ingress_policy_decision,
                )
            return fallback_result

    if isinstance(specialist_report_type, str):
        result = _finalize_specialist_candidate(
            routed_work_item=routed_work_item,
            candidate_result=result,
            specialist_report_type=specialist_report_type,
            raw_audit=raw_audit,
            duplicate_override=duplicate_override,
        )
    if duplicate_override:
        _annotate_duplicate_override_result(
            result,
            raw_audit=raw_audit,
            duplicate_policy_decision=ingress_policy_decision,
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
    if not duplicate_override and governed_status in {"rejected", "duplicate", "conflict_blocked", "invalid_input"}:
        if governed_status == "duplicate":
            _archive_duplicate_for_disposal(
                raw_audit,
                source_message_id=_result_source_message_id(result),
                sender_phone=_result_sender_phone(result),
                branch=_result_branch(result) or resolved_branch_hint or raw_audit.branch_hint,
                report_type=specialist_report_type if isinstance(specialist_report_type, str) else classification,
                report_date=_result_report_date(result) or _string_or_none(routing_payload.get("report_date")),
                duplicate_reason=_rejection_reason_from_result(result),
                duplicate_basis=_duplicate_basis_from_result(result),
                original_or_duplicate_of=_duplicate_reference_from_result(result),
            )
        else:
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

    duplicate_override = _duplicate_override_active(policy_decision)
    fallback_result = process_fallback_extraction_work_item(routed_work_item)
    fallback_payload = fallback_result.payload if isinstance(fallback_result.payload, dict) else {}
    normalized_report = fallback_payload.get("normalized_report")
    if not isinstance(normalized_report, Mapping):
        normalized_report = {}
    validation_payload = _fallback_validation_payload(
        normalized_report=normalized_report,
        routing_payload=routed_work_item.payload.get("routing") if isinstance(routed_work_item.payload, dict) else {},
        raw_text=_extract_raw_text(routed_work_item.payload) if isinstance(routed_work_item.payload, dict) else None,
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
    fallback_warnings = _fallback_warnings(fallback_payload=fallback_payload, validation_result=validation_result)
    fallback_accountability = _build_generic_accountability(
        payload=normalized_report,
        validation_outcome=validation_result.to_payload(),
        acceptance_outcome=acceptance_result.to_payload(),
        warnings=fallback_warnings,
        status="rejected" if acceptance_result.decision == "reject" else None,
    )
    fallback_validation_payload = _validation_payload_with_accountability(
        validation_result.to_payload(),
        accountability=fallback_accountability,
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
        result_policy = policy_decision if duplicate_override else fallback_policy
        fallback_metadata = _fallback_metadata(
            fallback_payload=fallback_payload,
            validation_payload=fallback_validation_payload,
            acceptance_payload=acceptance_result.to_payload(),
            review_queue_path=None,
            accountability=fallback_accountability,
        )
        _update_raw_metadata(
            raw_audit,
            detected_report_type=classification,
            routing_target=target_agent,
            processing_status="rejected",
            branch_hint=resolved_branch_hint,
            routing_metadata=_routing_metadata_from_payload(routing_payload),
            policy_decision=result_policy,
            governance_outcome=_governance_outcome_payload(status="rejected", reasons=["insufficient_structure"]),
            extra_metadata=fallback_metadata,
        )
        if not duplicate_override:
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
            validation_payload=fallback_validation_payload,
            acceptance_payload=acceptance_result.to_payload(),
            warnings=fallback_warnings,
            policy_decision=result_policy,
            status="invalid_input",
            review_queue_path=None,
            accountability=fallback_accountability,
        )

    review_queue_path: str | None = None
    if acceptance_result.decision == "review" and not duplicate_override:
        review_queue_path = write_review_item(
            routed_work_item,
            report_type=specialist_report_type,
            branch=_string_or_default(validation_result.normalized_payload.get("branch"), default="unknown"),
            report_date=_string_or_default(validation_result.normalized_payload.get("report_date"), default="unknown"),
            confidence=acceptance_result.confidence,
            warnings=fallback_warnings,
            reason=acceptance_result.reason,
            validation_outcome=fallback_validation_payload,
            acceptance_outcome=acceptance_result.to_payload(),
            governance_outcome={
                "status": "needs_review",
                "export_allowed": False,
                "reasons": [acceptance_result.reason],
            },
            candidate_payload={
                **dict(normalized_report),
                **({"accountability": fallback_accountability} if fallback_accountability is not None else {}),
                **({"confidence": fallback_payload.get("confidence")} if isinstance(fallback_payload.get("confidence"), (int, float)) else {}),
            },
            parser_used="fallback_extraction_agent",
            parse_mode="fallback",
        )

    fallback_metadata = _fallback_metadata(
        fallback_payload=fallback_payload,
        validation_payload=fallback_validation_payload,
        acceptance_payload=acceptance_result.to_payload(),
        review_queue_path=review_queue_path,
        accountability=fallback_accountability,
    )
    fallback_status = acceptance_result.governed_status(
        warning_codes=[warning.get("code", "") for warning in fallback_warnings]
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
        validation_payload=fallback_validation_payload,
        acceptance_payload=acceptance_result.to_payload(),
        warnings=fallback_warnings,
        policy_decision=policy_decision,
        status=fallback_status,
        review_queue_path=review_queue_path,
        accountability=fallback_accountability,
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
    if _is_intelligence_report_type(specialist_report_type):
        return False
    if specialist_status == "needs_review" and specialist_report_type == "staff_attendance":
        candidate_payload = result.payload if isinstance(result.payload, dict) else {}
        if validate_report(specialist_report_type, candidate_payload).accepted:
            return False
    classification_confidence = _classification_confidence(routed_work_item)
    if classification_confidence is None:
        return False
    return classification_confidence >= CLASSIFICATION_CONFIDENCE_MIN_FOR_FALLBACK


def _finalize_specialist_candidate(
    *,
    routed_work_item: WorkItem,
    candidate_result: AgentResult,
    specialist_report_type: str,
    raw_audit: RawAuditRecord,
    duplicate_override: bool = False,
) -> AgentResult:
    """Finalize one specialist result using strict or intelligence-only handling."""

    if _should_bypass_strict_validation(
        routed_work_item=routed_work_item,
        specialist_report_type=specialist_report_type,
    ):
        return _finalize_intelligence_candidate(
            routed_work_item=routed_work_item,
            candidate_result=candidate_result,
            duplicate_override=duplicate_override,
        )
    return _finalize_strict_candidate(
        routed_work_item=routed_work_item,
        candidate_result=candidate_result,
        specialist_report_type=specialist_report_type,
        raw_audit=raw_audit,
        duplicate_override=duplicate_override,
    )


def _finalize_intelligence_candidate(
    *,
    routed_work_item: WorkItem,
    candidate_result: AgentResult,
    duplicate_override: bool = False,
) -> AgentResult:
    """Persist intelligence candidates without shared strict validation or review output."""

    finalized_payload = dict(candidate_result.payload) if isinstance(candidate_result.payload, dict) else {}
    _hydrate_intelligence_scope(finalized_payload, routed_work_item)
    finalized_result = AgentResult(
        agent_name=candidate_result.agent_name,
        payload=finalized_payload,
        metadata=_intelligence_write_metadata(
            candidate_result=candidate_result,
            routed_work_item=routed_work_item,
        ),
    )
    if duplicate_override:
        return _finalize_duplicate_intelligence_result(finalized_result)

    write_result = _write_final_structured_result(finalized_result)
    if write_result is not None:
        _apply_final_governance(finalized_result, write_result)
        return finalized_result

    governance_status = _result_status(finalized_result)
    governance_reasons = _governance_reasons_without_write(finalized_result)
    finalized_result.payload["status"] = governance_status
    finalized_result.payload["export_allowed"] = False
    finalized_result.payload["governance"] = _governance_outcome_payload(
        status=governance_status,
        reasons=governance_reasons,
        export_allowed=False,
    )
    return finalized_result


def _finalize_strict_candidate(
    *,
    routed_work_item: WorkItem,
    candidate_result: AgentResult,
    specialist_report_type: str,
    raw_audit: RawAuditRecord,
    duplicate_override: bool = False,
) -> AgentResult:
    """Apply shared validation, acceptance, governance, and final action centrally."""

    candidate_payload = candidate_result.payload if isinstance(candidate_result.payload, dict) else {}
    validation_result = validate_report(
        specialist_report_type,
        _validation_payload_with_raw_text(
            payload=candidate_payload,
            routed_work_item=routed_work_item,
        ),
    )
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

    candidate_metadata = dict(candidate_result.metadata) if isinstance(candidate_result.metadata, Mapping) else {}
    _maybe_promote_attendance_auto_accept_candidate(
        specialist_report_type=specialist_report_type,
        finalized_payload=finalized_payload,
        candidate_metadata=candidate_metadata,
        acceptance_result=acceptance_result,
    )
    accountability = _extract_accountability(
        payload=finalized_payload,
        metadata=candidate_metadata,
    )

    finalized_result = AgentResult(
        agent_name=candidate_result.agent_name,
        payload=finalized_payload,
        metadata=_strict_write_metadata(
            candidate_metadata=candidate_metadata,
            routed_work_item=routed_work_item,
            validation_result=validation_result,
            acceptance_result=acceptance_result,
            accountability=accountability,
        ),
    )
    if duplicate_override:
        return _finalize_duplicate_strict_result(
            finalized_result,
            validation_result=validation_result,
            acceptance_result=acceptance_result,
        )

    write_result = _write_final_structured_result(finalized_result)
    if write_result is not None:
        _apply_final_governance(finalized_result, write_result)
        _ensure_result_accountability(
            result=finalized_result,
            validation_outcome=validation_result.to_payload(),
            acceptance_outcome=acceptance_result.to_payload(),
        )
        review_queue_path = _maybe_write_strict_review_item(
            routed_work_item=routed_work_item,
            result=finalized_result,
            validation_result=validation_result,
            acceptance_result=acceptance_result,
            raw_audit=raw_audit,
            candidate_payload=finalized_result.payload if isinstance(finalized_result.payload, dict) else finalized_payload,
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
    _ensure_result_accountability(
        result=finalized_result,
        validation_outcome=validation_result.to_payload(),
        acceptance_outcome=acceptance_result.to_payload(),
    )
    review_queue_path = _maybe_write_strict_review_item(
        routed_work_item=routed_work_item,
        result=finalized_result,
        validation_result=validation_result,
        acceptance_result=acceptance_result,
        raw_audit=raw_audit,
        candidate_payload=finalized_result.payload if isinstance(finalized_result.payload, dict) else finalized_payload,
    )
    if review_queue_path is not None:
        finalized_result.metadata["review_queue_path"] = review_queue_path
    return finalized_result


def _finalize_duplicate_intelligence_result(finalized_result: AgentResult) -> AgentResult:
    """Finalize one duplicate intelligence candidate without writing any records."""

    governance_status = _result_status(finalized_result)
    governance_reasons = [] if governance_status in {"accepted", "accepted_with_warning"} else _governance_reasons_without_write(
        finalized_result
    )
    finalized_result.payload["status"] = governance_status
    finalized_result.payload["export_allowed"] = False
    finalized_result.payload["governance"] = _governance_outcome_payload(
        status=governance_status,
        reasons=governance_reasons,
        export_allowed=False,
    )
    return finalized_result


def _finalize_duplicate_strict_result(
    finalized_result: AgentResult,
    *,
    validation_result,
    acceptance_result,
) -> AgentResult:
    """Finalize one duplicate strict candidate without writing or review enqueue."""

    governance_status = acceptance_result.governed_status(
        warning_codes=[warning.get("code", "") for warning in _result_warnings(finalized_result)]
    )
    governance_reasons = [acceptance_result.reason] if governance_status == "needs_review" else []
    finalized_result.payload["status"] = governance_status
    finalized_result.payload["export_allowed"] = False
    finalized_result.payload["governance"] = _governance_outcome_payload(
        status=governance_status,
        reasons=governance_reasons,
        export_allowed=False,
    )
    _ensure_result_accountability(
        result=finalized_result,
        validation_outcome=validation_result.to_payload(),
        acceptance_outcome=acceptance_result.to_payload(),
    )
    return finalized_result


def _strict_write_metadata(
    *,
    candidate_metadata: Mapping[str, Any] | None,
    routed_work_item: WorkItem,
    validation_result,
    acceptance_result,
    accountability: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return the central sidecar metadata for one strict candidate result."""

    metadata = dict(candidate_metadata) if isinstance(candidate_metadata, Mapping) else {}
    existing_validation = metadata.get("validation")
    if isinstance(existing_validation, Mapping):
        metadata["candidate_validation"] = dict(existing_validation)

    routed_payload = routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}
    validation_payload = _validation_payload_with_accountability(
        validation_result.to_payload(),
        accountability=accountability,
    )
    if isinstance(existing_validation, Mapping):
        candidate_details = existing_validation.get("details")
        if isinstance(candidate_details, Mapping):
            merged_details = dict(_mapping(validation_payload.get("details")))
            if candidate_details.get("parser_failure") is True:
                merged_details["parser_failure"] = True
                merged_details["candidate_final_status"] = candidate_details.get("final_status")
            candidate_accountability = _mapping(candidate_details.get("accountability"))
            if candidate_accountability:
                merged_details["accountability"] = dict(candidate_accountability)
            if merged_details:
                validation_payload["details"] = merged_details
    metadata["validation"] = validation_payload
    metadata["acceptance"] = acceptance_result.to_payload()
    metadata["governance_context"] = {
        **_mapping(metadata.get("governance_context")),
        **_routing_governance_context(routed_payload),
    }
    if accountability:
        metadata["accountability"] = dict(accountability)
    return metadata


def _maybe_promote_attendance_auto_accept_candidate(
    *,
    specialist_report_type: str,
    finalized_payload: dict[str, Any],
    candidate_metadata: dict[str, Any],
    acceptance_result,
) -> None:
    """Promote one attendance candidate to accepted when review has no blocking issue."""

    if specialist_report_type != "staff_attendance":
        return
    if getattr(acceptance_result, "decision", None) != "accept":
        return
    validation_error_code = _attendance_candidate_validation_error_code(
        payload=finalized_payload,
        metadata=candidate_metadata,
    )
    if is_blocking_accountability_rule(validation_error_code):
        return

    warning_codes = [
        warning.get("code", "")
        for warning in finalized_payload.get("warnings", [])
        if isinstance(warning, Mapping) and isinstance(warning.get("code"), str)
    ]
    finalized_payload["status"] = acceptance_result.governed_status(warning_codes=warning_codes)
    _clear_attendance_review_state(finalized_payload, candidate_metadata)


def _attendance_candidate_validation_error_code(
    *,
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any] | None,
) -> str | None:
    """Return the most specific attendance validation error code on one candidate."""

    direct_code = _string_or_none(payload.get("validation_error_code"))
    if direct_code is not None:
        return direct_code

    accountability = _extract_accountability(payload=payload, metadata=metadata)
    if isinstance(accountability, Mapping):
        accountability_code = _string_or_none(accountability.get("validation_error_code")) or _string_or_none(
            accountability.get("failing_rule")
        )
        if accountability_code is not None:
            return accountability_code

    validation = _mapping(_mapping(metadata).get("validation"))
    details = _mapping(validation.get("details"))
    return _string_or_none(details.get("validation_error_code"))


def _clear_attendance_review_state(
    payload: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    """Remove non-blocking review-only attendance markers from finalized output."""

    for field_name in ("accountability", "validation_error_code", "validation_error_message"):
        payload.pop(field_name, None)

    metadata.pop("accountability", None)
    validation = _mapping(metadata.get("validation"))
    if validation:
        validation_payload = dict(validation)
        details = dict(_mapping(validation_payload.get("details")))
        for field_name in ("accountability", "validation_error_code", "validation_error_message"):
            details.pop(field_name, None)
        if details:
            validation_payload["details"] = details
        else:
            validation_payload.pop("details", None)
        metadata["validation"] = validation_payload


def _intelligence_write_metadata(
    *,
    candidate_result: AgentResult,
    routed_work_item: WorkItem,
) -> dict[str, Any]:
    """Return sidecar metadata for intelligence records without strict orchestrator validation."""

    metadata = dict(candidate_result.metadata) if isinstance(candidate_result.metadata, dict) else {}
    routed_payload = routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}
    existing_validation = _mapping(metadata.get("validation"))
    validation_payload = dict(existing_validation) if existing_validation else {
        "stage": AGENT_NAME,
        "status": "bypassed",
        "accepted": True,
        "rejections": [],
        "reason_codes": [],
        "normalization": {},
    }
    validation_details = dict(_mapping(validation_payload.get("details")))
    validation_details["orchestrator_validation_bypassed"] = True
    validation_details["candidate_status"] = _result_status(candidate_result)
    validation_payload["details"] = validation_details
    metadata["validation"] = validation_payload
    metadata.pop("acceptance", None)
    metadata["governance_context"] = {
        **_mapping(metadata.get("governance_context")),
        **_routing_governance_context(routed_payload),
    }
    return metadata


def _hydrate_intelligence_scope(
    finalized_payload: dict[str, Any],
    routed_work_item: WorkItem,
) -> None:
    """Fill missing intelligence branch/report_date values from routed context."""

    routed_payload = routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}
    routing = _mapping(routed_payload.get("routing"))
    branch = _string_or_none(finalized_payload.get("branch"))
    if branch is None:
        branch_hint = _string_or_none(routing.get("branch_hint"))
        if branch_hint is not None:
            finalized_payload["branch"] = branch_hint

    report_date = _string_or_none(finalized_payload.get("report_date"))
    if report_date is None:
        for field_name in ("report_date", "normalized_report_date", "raw_report_date"):
            candidate = _string_or_none(routing.get(field_name))
            if candidate is not None:
                finalized_payload["report_date"] = candidate
                break


def _should_bypass_strict_validation(
    *,
    routed_work_item: WorkItem,
    specialist_report_type: str,
) -> bool:
    """Return whether the shared strict validation layer should be skipped."""

    if _is_intelligence_report_type(specialist_report_type):
        return True
    routed_payload = routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}
    classification = _mapping(routed_payload.get("classification"))
    return _is_intelligence_report_family(classification.get("report_family"))


def _routing_governance_context(routed_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return governance context from the routed work item without losing raw links."""

    routing = _mapping(routed_payload.get("routing"))
    governance_context = {
        "classified_report_family": _string_or_default(
            _mapping(routed_payload.get("classification")).get("report_family"),
            default="unknown",
        ),
        "classified_report_type": _string_or_default(
            _mapping(routed_payload.get("classification")).get("report_type"),
            default="unknown",
        ),
    }
    for field_name in ("branch_hint", "report_date", "normalized_report_date", "raw_report_date"):
        value = routing.get(field_name)
        if isinstance(value, str) and value.strip():
            governance_context[field_name] = value.strip()
    raw_record = routed_payload.get("raw_record")
    if isinstance(raw_record, Mapping):
        for field_name in ("raw_txt_path", "raw_meta_path", "raw_sha256"):
            value = raw_record.get(field_name)
            if isinstance(value, str) and value.strip():
                governance_context[field_name] = value.strip()
    ingress_envelope = routed_payload.get("ingress_envelope")
    if isinstance(ingress_envelope, Mapping):
        payload = ingress_envelope.get("payload")
        if isinstance(payload, Mapping):
            for field_name in ("message_id", "sender_phone"):
                value = payload.get(field_name)
                if isinstance(value, str) and value.strip():
                    governance_context[field_name] = value.strip()
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
    if getattr(write_result, "persisted", True) is not True:
        for field_name in (
            "structured_output_path",
            "output_path",
            "output_paths",
            "derived_output_paths",
            "outputs",
        ):
            result.payload.pop(field_name, None)
        return
    written_path = getattr(write_result, "path", None)
    if isinstance(written_path, Path):
        output_path = _display_structured_path(written_path)
        result.payload["structured_output_path"] = output_path
        result.payload["output_path"] = output_path
        result.payload["output_paths"] = [output_path]
        result.payload["derived_output_paths"] = [output_path]
        result.payload["outputs"] = [output_path]


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
    accountability = _ensure_result_accountability(
        result=result,
        validation_outcome=validation_result.to_payload(),
        acceptance_outcome=acceptance_result.to_payload(),
    )
    validation_payload = _validation_payload_with_accountability(
        validation_result.to_payload(),
        accountability=accountability,
    )
    return write_review_item(
        routed_work_item,
        report_type=specialist_report_type_from_payload(payload),
        branch=_string_or_default(payload.get("branch"), default="unknown"),
        report_date=_string_or_default(payload.get("report_date"), default="unknown"),
        confidence=_result_confidence(result),
        warnings=_result_warnings(result),
        reason=review_reason,
        validation_outcome=validation_payload,
        acceptance_outcome=acceptance_result.to_payload(),
        governance_outcome=governance,
        candidate_payload=dict(result.payload) if isinstance(result.payload, dict) else dict(candidate_payload),
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
    raw_text: str | None = None,
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
    if isinstance(raw_text, str) and raw_text.strip():
        payload["raw_text"] = raw_text
    return payload


def _validation_payload_with_raw_text(
    *,
    payload: Mapping[str, Any],
    routed_work_item: WorkItem,
) -> dict[str, Any]:
    """Return one validation payload augmented with the segment raw text."""

    validation_payload = dict(payload)
    routed_payload = routed_work_item.payload if isinstance(routed_work_item.payload, dict) else {}
    raw_text = _extract_raw_text(routed_payload)
    if raw_text:
        validation_payload["raw_text"] = raw_text
    return validation_payload


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
                    "report_type": segment.detected_report_family,
                    "report_family": segment.detected_report_family,
                    "report_family_label": getattr(segment, "report_family_label", segment.detected_report_family),
                    "status": "needs_review",
                    "blocks_transactional_processing": getattr(segment, "blocks_transactional_processing", True),
                    "warnings": [
                        _make_warning(
                            code="unsupported_segment",
                            severity="warning",
                            message=f"Mixed child family `{segment.detected_report_family}` has no configured specialist route.",
                        )
                    ],
                    "errors": [],
                    "metrics": {},
                    "validation": {},
                    "output_paths": [],
                    "child_index": segment.segment_index + 1,
                    "header_line": getattr(segment, "header_line", None),
                    "response_report_type": _mixed_child_response_report_type(
                        report_family=segment.detected_report_family,
                        specialist_report_type=None,
                        header_line=getattr(segment, "header_line", None),
                    ),
                    "response_status": "review",
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
        prechecked_summary = _maybe_prechecked_mixed_child_review_summary(
            child_work_item=child_work_item,
            segment=segment,
            route=route,
        )
        if prechecked_summary is not None:
            child_summaries.append(prechecked_summary)
            continue
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
                    "report_type": segment.detected_report_family,
                    "report_family": segment.detected_report_family,
                    "report_family_label": getattr(segment, "report_family_label", segment.detected_report_family),
                    "status": "invalid_input",
                    "blocks_transactional_processing": getattr(segment, "blocks_transactional_processing", True),
                    "warnings": [],
                    "errors": [
                        _make_warning(
                            code="routing_failure",
                            severity="error",
                            message=f"Mixed child routing failed for {segment.detected_report_family}: {exc}",
                        )
                    ],
                    "metrics": {},
                    "validation": {},
                    "output_paths": [],
                    "child_index": segment.segment_index + 1,
                    "header_line": getattr(segment, "header_line", None),
                    "response_report_type": _mixed_child_response_report_type(
                        report_family=segment.detected_report_family,
                        specialist_report_type=route.specialist_type,
                        header_line=getattr(segment, "header_line", None),
                    ),
                    "response_status": "review",
                    "validation_error_code": "routing_failure",
                    "validation_error_message": f"Mixed child routing failed for {segment.detected_report_family}: {exc}",
                    "reason": f"Mixed child routing failed for {segment.detected_report_family}: {exc}",
                    "lineage": dict(child_work_item.payload.get("lineage", {})),
                    "segment_id": segment.segment_id,
                    "segment_range": {"start_line": segment.start_line, "end_line": segment.end_line},
                    "error": str(exc),
                }
            )
            continue

        child_result = _finalize_specialist_candidate(
            routed_work_item=child_work_item,
            candidate_result=child_result,
            specialist_report_type=route.specialist_type,
            raw_audit=raw_audit,
        )

        child_results.append(child_result)
        child_output_paths = _structured_output_paths_from_result(child_result)
        output_paths.extend(child_output_paths)
        child_payload = dict(child_result.payload) if isinstance(child_result.payload, dict) else {}
        child_warnings = _result_warnings(child_result)
        child_errors = [
            warning
            for warning in child_warnings
            if _string_or_none(warning.get("severity")) == "error"
        ]
        child_validation_error = _mixed_child_validation_error_fields(
            payload=child_payload,
            validation=_result_validation_outcome(child_result),
            warnings=child_warnings,
        )
        child_summaries.append(
            {
                "agent_name": child_result.agent_name,
                "report_type": segment.detected_report_family,
                "report_family": segment.detected_report_family,
                "report_family_label": getattr(segment, "report_family_label", segment.detected_report_family),
                "branch": _result_branch(child_result),
                "report_date": _result_report_date(child_result),
                "status": _result_status(child_result),
                "blocks_transactional_processing": getattr(segment, "blocks_transactional_processing", True),
                "warnings": child_warnings,
                "errors": child_errors,
                "metrics": dict(_mapping(child_payload.get("metrics"))),
                "validation": _result_validation_outcome(child_result),
                "output_paths": child_output_paths,
                "child_index": segment.segment_index + 1,
                "header_line": getattr(segment, "header_line", None),
                "response_report_type": _mixed_child_response_report_type(
                    report_family=segment.detected_report_family,
                    specialist_report_type=route.specialist_type,
                    header_line=getattr(segment, "header_line", None),
                ),
                "response_status": _mixed_child_response_status(_result_status(child_result)),
                "reason": _mixed_child_reason(
                    result=child_result,
                    specialist_report_type=route.specialist_type,
                ),
                **child_validation_error,
                "lineage": dict(child_work_item.payload.get("lineage", {})),
                "segment_id": segment.segment_id,
                "segment_range": {"start_line": segment.start_line, "end_line": segment.end_line},
                "split_confidence": segment.split_confidence,
                "payload": child_payload,
            }
        )

    parent_status, parent_reason = _mixed_parent_decision(
        child_results=child_results,
        child_summaries=child_summaries,
    )
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
        review_reason=parent_reason if parent_status == "needs_review" else None,
        specialist_report_type=None,
        split_strategy="explicit_report_headers",
        child_report_types=[child["report_family"] for child in child_summaries],
        child_count=len(child_summaries),
    )

    accepted_parent_statuses = {"accepted", "accepted_with_warning"}
    terminal_duplicate_parent_statuses = {"duplicate"}
    governance_reasons = [parent_reason] if parent_reason is not None else []

    _update_raw_metadata(
        raw_audit,
        detected_report_type="mixed",
        routing_target="fan_out",
        processing_status=(
            "processed"
            if parent_status in accepted_parent_statuses
            else "duplicate"
            if parent_status in terminal_duplicate_parent_statuses
            else "rejected"
        ),
        branch_hint=branch_hint,
        routing_metadata=_routing_metadata_from_payload(routing_payload),
        policy_decision=policy_decision,
        governance_outcome=_governance_outcome_payload(
            status=(
                parent_status
                if parent_status in accepted_parent_statuses | terminal_duplicate_parent_statuses
                else "needs_review"
            ),
            reasons=governance_reasons,
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
        "governance": _governance_outcome_payload(
            status=(
                parent_status
                if parent_status in accepted_parent_statuses | terminal_duplicate_parent_statuses
                else "needs_review"
            ),
            reasons=governance_reasons,
            export_allowed=False,
        ),
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
    human_tolerance = _sanitize_human_tolerance(payload.get("human_tolerance"))
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
        normalized_text_hash=_sanitize_optional_text(human_tolerance.get("normalized_text_hash")),
        human_tolerance=human_tolerance,
        existing_metadata=_load_existing_metadata_for_dedup(
            meta_path=meta_path,
            raw_sha256=raw_sha256,
            normalized_text_hash=_sanitize_optional_text(human_tolerance.get("normalized_text_hash")),
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
    if not isinstance(payload.get("accountability"), Mapping):
        candidate_warnings = _mapping(payload.get("candidate_payload")).get("warnings")
        payload["accountability"] = _build_generic_accountability(
            payload=_mapping(payload.get("candidate_payload")),
            validation_outcome=_mapping(payload.get("validation")),
            acceptance_outcome=_mapping(payload.get("acceptance")),
            governance_outcome={
                "status": "rejected",
                "reasons": [rejection_reason],
            },
            warnings=candidate_warnings if isinstance(candidate_warnings, list) else _provenance_warnings(payload if isinstance(payload, dict) else None),
            status="rejected",
            rejection_reason=rejection_reason,
        )
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


def _archive_duplicate_for_disposal(
    audit: RawAuditRecord,
    *,
    source_message_id: str | None,
    sender_phone: str | None,
    branch: str | None,
    report_type: str | None,
    report_date: str | None,
    duplicate_reason: str,
    duplicate_basis: str | None,
    original_or_duplicate_of: str | None,
) -> Path:
    """Archive one duplicate event under the disposable duplicates store."""

    return archive_duplicate_record(
        source_message_id=source_message_id,
        sender_phone=sender_phone,
        branch=branch,
        report_type=report_type,
        report_date=report_date,
        raw_txt_path=str(audit.text_path),
        raw_meta_path=str(audit.meta_path),
        duplicate_reason=duplicate_reason,
        duplicate_basis=duplicate_basis,
        original_or_duplicate_of=original_or_duplicate_of,
        output_root=audit.text_path.parents[4],
    )


def _build_routed_work_item(work_item: WorkItem) -> WorkItem:
    """Create the minimal safe routed work item for exactly one specialist agent."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    human_tolerance = _sanitize_human_tolerance(payload.get("human_tolerance"))
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
    human_tolerance_raw_date = _human_tolerance_raw_value(human_tolerance, field_name="date")
    if human_tolerance_raw_date is not None:
        normalized_raw_report_date = human_tolerance_raw_date
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
            "report_family": _classification_report_family_label(
                detected_report_type=routing_decision.detected_report_type,
                specialist_report_type=routing_decision.specialist_report_type,
            ),
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

    if human_tolerance:
        routed_payload["human_tolerance"] = human_tolerance

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
        "raw_txt_path": str(audit.text_path),
        "raw_meta_path": str(audit.meta_path),
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
    if audit.human_tolerance:
        payload["human_tolerance"] = dict(audit.human_tolerance)
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


def _ingress_field(payload: Mapping[str, Any], field_name: str) -> str | None:
    """Return one field from the ingress envelope payload when present."""

    ingress_payload = _mapping(_mapping(payload.get("ingress_envelope")).get("payload"))
    return _string_or_none(ingress_payload.get(field_name))


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


def _sanitize_human_tolerance(value: object) -> dict[str, Any]:
    """Keep one human-tolerance payload only when it follows the expected shape."""

    if not isinstance(value, Mapping):
        return {}
    return dict(value)


def _human_tolerance_raw_value(human_tolerance: Mapping[str, Any], *, field_name: str) -> str | None:
    """Return one original raw correction value from human-tolerance metadata."""

    corrections = human_tolerance.get("corrections")
    if not isinstance(corrections, list):
        return None
    for correction in corrections:
        if not isinstance(correction, Mapping):
            continue
        field = correction.get("field")
        raw_value = correction.get("raw_value")
        if field == field_name and isinstance(raw_value, str) and raw_value.strip():
            return raw_value.strip()
    return None


def _with_human_tolerance(work_item: WorkItem) -> WorkItem:
    """Attach normalized human-intake metadata before duplicate checks or routing."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return work_item

    raw_text = raw_message.get("text")
    if not isinstance(raw_text, str) or not raw_text.strip():
        return work_item

    normalized_text = raw_message.get("normalized_text")
    human_tolerance = _sanitize_human_tolerance(payload.get("human_tolerance"))
    if not isinstance(normalized_text, str) or not normalized_text.strip() or not human_tolerance:
        analyzed = analyze_human_whatsapp_text(raw_text)
        normalized_text = analyzed.normalized_text or raw_text
        human_tolerance = analyzed.to_payload()

    safe_raw_message = dict(raw_message)
    safe_raw_message["normalized_text"] = normalized_text
    safe_payload = dict(payload)
    safe_payload["raw_message"] = safe_raw_message
    safe_payload["human_tolerance"] = human_tolerance
    return WorkItem(kind=work_item.kind, payload=safe_payload)


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
    normalized_text_hash: str | None,
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
            normalized_text_hash=normalized_text_hash,
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
            normalized_text_hash=normalized_text_hash,
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
    normalized_text_hash: str | None,
    current_received_at: datetime,
) -> tuple[datetime, dict[str, Any]] | None:
    """Return duplicate-candidate metadata when the hash matches inside the dedup window."""

    existing_raw_sha256 = _sanitize_optional_text(payload.get("raw_sha256"))
    existing_human_tolerance = _mapping(payload.get("human_tolerance"))
    existing_normalized_hash = _sanitize_optional_text(existing_human_tolerance.get("normalized_text_hash"))
    raw_match = existing_raw_sha256 == raw_sha256
    normalized_match = normalized_text_hash is not None and existing_normalized_hash == normalized_text_hash
    if not raw_match and not normalized_match:
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
    raw_message = payload.get("raw_message")
    if isinstance(raw_message, Mapping):
        normalized_text = raw_message.get("normalized_text")
        if isinstance(normalized_text, str):
            return normalized_text
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
    branch_hint = _string_or_none(routing.get("branch_hint"))
    resolved_report_date = _string_or_none(routing.get("normalized_report_date")) or _string_or_none(
        routing.get("report_date")
    )
    raw_report_date = _string_or_none(routing.get("raw_report_date"))
    accountability = _build_generic_accountability(
        payload={},
        validation_outcome=None,
        acceptance_outcome=None,
        governance_outcome={
            "status": "duplicate" if status == "duplicate" else "rejected" if status in {"rejected", "invalid_input"} else status,
            "reasons": [route_reason],
        },
        warnings=warnings,
        status=status,
        route_reason=route_reason,
    )

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "source": source,
            "classification": {"report_type": classification},
            "branch": branch_hint,
            "report_date": resolved_report_date,
            "routing": _build_routing_payload(
                classification=classification,
                target_agent=target_agent,
                status=status,
                route_reason=route_reason,
                branch_hint=branch_hint,
                report_date=resolved_report_date,
                normalized_report_date=resolved_report_date,
                raw_report_date=raw_report_date,
                confidence=routing.get("confidence") if isinstance(routing.get("confidence"), (int, float)) else None,
                evidence=list(routing.get("evidence")) if isinstance(routing.get("evidence"), list) else None,
                normalized_header_candidates=(
                    list(routing.get("normalized_header_candidates"))
                    if isinstance(routing.get("normalized_header_candidates"), list)
                    else None
                ),
                review_reason=_string_or_none(routing.get("review_reason")),
                specialist_report_type=_string_or_none(routing.get("specialist_report_type")),
                split_strategy=_string_or_none(routing.get("split_strategy")),
                child_report_types=list(routing.get("child_report_types"))
                if isinstance(routing.get("child_report_types"), list)
                else None,
                child_count=routing.get("child_count") if isinstance(routing.get("child_count"), int) else None,
            ),
            "raw_message": safe_raw_message,
            "metadata": metadata,
            "confidence": 0.0,
            "metrics": {},
            "items": [],
            "warnings": warnings,
            **_validation_error_fields(accountability),
            **({"accountability": accountability} if accountability is not None else {}),
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
    validation_payload: dict[str, Any],
    acceptance_payload: dict[str, Any],
    warnings: list[dict[str, str]],
    policy_decision: PolicyDecision,
    status: RouteStatus,
    review_queue_path: str | None,
    accountability: Mapping[str, Any] | None,
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
    normalized_report = (
        dict(fallback_payload.get("normalized_report", {}))
        if isinstance(fallback_payload.get("normalized_report"), Mapping)
        else {}
    )
    fallback_branch = _string_or_none(normalized_report.get("branch"))
    fallback_report_date = _string_or_none(normalized_report.get("report_date"))
    if fallback_branch is not None:
        result.payload["branch"] = fallback_branch
    if fallback_report_date is not None:
        result.payload["report_date"] = fallback_report_date
    fallback_routing = _mapping(result.payload.get("routing"))
    if fallback_routing:
        if fallback_branch is not None:
            fallback_routing["branch_hint"] = fallback_branch
        if fallback_report_date is not None:
            fallback_routing["report_date"] = fallback_report_date
            fallback_routing["normalized_report_date"] = fallback_report_date
        result.payload["routing"] = dict(fallback_routing)
    result.payload["fallback"] = {
        "parse_mode": fallback_payload.get("parse_mode"),
        "confidence": fallback_payload.get("confidence"),
        "warnings": list(fallback_payload.get("warnings", [])) if isinstance(fallback_payload.get("warnings"), list) else [],
        "provenance": dict(fallback_payload.get("provenance", {})) if isinstance(fallback_payload.get("provenance"), Mapping) else {},
        "normalized_report": normalized_report,
        "validation": validation_payload,
        "acceptance": acceptance_payload,
        "review_queue_path": review_queue_path,
    }
    result.payload["confidence"] = fallback_payload.get("confidence", 0.0)
    if accountability is not None:
        result.payload["accountability"] = dict(accountability)
        result.payload.update(_validation_error_fields(accountability))
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
    validation_payload: dict[str, Any],
    acceptance_payload: dict[str, Any],
    review_queue_path: str | None,
    accountability: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return fallback metadata for raw and rejected audit records."""

    return {
        "fallback_parse_mode": fallback_payload.get("parse_mode"),
        "fallback_confidence": fallback_payload.get("confidence"),
        "fallback_status": fallback_payload.get("status"),
        "fallback_validation": validation_payload,
        "fallback_acceptance": acceptance_payload,
        "fallback_review_queue_path": review_queue_path,
        "fallback_provenance": dict(fallback_payload.get("provenance", {})) if isinstance(fallback_payload.get("provenance"), Mapping) else {},
        **({"accountability": dict(accountability)} if accountability is not None else {}),
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


def _validation_payload_with_accountability(
    validation_outcome: Mapping[str, Any] | None,
    *,
    accountability: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return a validation payload enriched with accountability details."""

    payload = dict(validation_outcome) if isinstance(validation_outcome, Mapping) else {"status": "not_run"}
    details = dict(_mapping(payload.get("details")))
    if accountability is not None:
        details["accountability"] = dict(accountability)
        details.update(_validation_error_fields(accountability))
    if details:
        payload["details"] = details
    return payload


def _extract_accountability(
    *,
    payload: Mapping[str, Any] | None,
    metadata: Mapping[str, Any] | None = None,
    validation_outcome: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Return an existing accountability payload when one is already present."""

    if isinstance(payload, Mapping):
        accountability = payload.get("accountability")
        if isinstance(accountability, Mapping):
            return dict(accountability)

    if isinstance(validation_outcome, Mapping):
        validation_details = _mapping(validation_outcome.get("details"))
        accountability = validation_details.get("accountability")
        if isinstance(accountability, Mapping):
            return dict(accountability)

    if isinstance(metadata, Mapping):
        accountability = metadata.get("accountability")
        if isinstance(accountability, Mapping):
            return dict(accountability)
        validation = _mapping(metadata.get("validation"))
        validation_details = _mapping(validation.get("details"))
        accountability = validation_details.get("accountability")
        if isinstance(accountability, Mapping):
            return dict(accountability)

    return None


def _ensure_result_accountability(
    *,
    result: AgentResult,
    validation_outcome: Mapping[str, Any] | None,
    acceptance_outcome: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Attach accountability to a finalized result when review or rejection requires it."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    metadata = dict(result.metadata) if isinstance(result.metadata, dict) else {}
    accountability = _extract_accountability(
        payload=payload,
        metadata=metadata,
        validation_outcome=validation_outcome,
    )
    if accountability is None:
        accountability = _build_generic_accountability(
            payload=payload,
            validation_outcome=validation_outcome,
            acceptance_outcome=acceptance_outcome,
            governance_outcome=_result_governance(result),
            warnings=_result_warnings(result),
            status=_result_status(result),
        )
    if accountability is None:
        return None

    payload["accountability"] = dict(accountability)
    payload.update(_validation_error_fields(accountability))
    base_validation = _mapping(metadata.get("validation")) or _mapping(validation_outcome)
    metadata["validation"] = _validation_payload_with_accountability(
        base_validation,
        accountability=accountability,
    )
    metadata["accountability"] = dict(accountability)
    result.metadata = metadata
    return dict(accountability)


def _build_generic_accountability(
    *,
    payload: Mapping[str, Any] | None,
    validation_outcome: Mapping[str, Any] | None,
    acceptance_outcome: Mapping[str, Any] | None,
    governance_outcome: Mapping[str, Any] | None = None,
    warnings: list[dict[str, str]] | None = None,
    status: str | None = None,
    route_reason: str | None = None,
    rejection_reason: str | None = None,
) -> dict[str, Any] | None:
    """Return a generic accountability block when a result is reviewed or rejected upstream."""

    validation = _mapping(validation_outcome)
    acceptance = _mapping(acceptance_outcome)
    governance = _mapping(governance_outcome)
    governance_reasons = [
        reason
        for reason in governance.get("reasons", [])
        if isinstance(reason, str) and reason.strip()
    ] if isinstance(governance.get("reasons"), list) else []
    current_status = _string_or_none(status) or _string_or_none(governance.get("status"))
    decision = _string_or_none(acceptance.get("decision"))

    if (
        current_status not in {"needs_review", "conflict_blocked", "rejected", "invalid_input", "duplicate"}
        and decision not in {"review", "reject"}
        and validation.get("accepted") is not False
        and rejection_reason is None
        and route_reason is None
        and not governance_reasons
    ):
        return None

    failing_layer = "orchestrator_routing"
    failing_rule = route_reason or rejection_reason or current_status or "review_required"
    reason_detail = _first_warning_message(warnings)

    if current_status == "conflict_blocked" or "conflicting_record_same_scope" in governance_reasons:
        failing_layer = "orchestrator_governance"
        failing_rule = "conflicting_record_same_scope"
        reason_detail = "A record already exists for this branch and report date."
    else:
        rejections = validation.get("rejections")
        if validation.get("accepted") is False and isinstance(rejections, list) and rejections:
            first_rejection = next((item for item in rejections if isinstance(item, Mapping)), None)
            if first_rejection is not None:
                failing_layer = "orchestrator_validation"
                failing_rule = _string_or_none(first_rejection.get("code")) or "validation_failed"
                reason_detail = _string_or_none(first_rejection.get("message")) or "Validation rejected the normalized report."
        elif decision in {"review", "reject"}:
            failing_layer = "orchestrator_acceptance"
            failing_rule = _string_or_none(acceptance.get("reason")) or failing_rule
            if reason_detail is None:
                reason_detail = (
                    f"Acceptance policy returned `{failing_rule}`."
                    if failing_rule
                    else "Acceptance policy required manual review."
                )

    if failing_rule == "branch_unresolved":
        reason_detail = "Branch could not be resolved from the raw report before specialist routing."
    elif failing_rule == "date_unresolved":
        reason_detail = "Report date could not be resolved from the raw report before specialist routing."

    if reason_detail is None:
        if governance_reasons:
            reason_detail = f"Governance returned {governance_reasons[0]}."
        elif current_status is not None:
            reason_detail = f"Upstream processing ended with status `{current_status}`."
        else:
            reason_detail = "Manual review is required."

    return {
        "failing_layer": failing_layer,
        "failing_rule": failing_rule,
        "validation_error_code": failing_rule,
        "validation_error_message": reason_detail,
        "normalized_values_attempted": _generic_normalization_attempts(validation),
        "calculated_totals": _generic_calculated_totals(payload),
        "declared_totals": _generic_declared_totals(payload),
        "final_confidence_score": _generic_confidence(payload, acceptance),
        "reason_detail": reason_detail,
        "recommended_correction": _generic_recommended_correction(failing_rule, failing_layer),
    }


def _generic_normalization_attempts(validation_outcome: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return normalization attempts from a shared validation payload when present."""

    attempts: list[dict[str, Any]] = []
    normalization = validation_outcome.get("normalization")
    if not isinstance(normalization, Mapping):
        return attempts
    for field_name, field_payload in normalization.items():
        if not isinstance(field_payload, Mapping):
            continue
        raw_value = field_payload.get("raw") if "raw" in field_payload else field_payload.get("raw_value")
        normalized_value = (
            field_payload.get("normalized")
            if "normalized" in field_payload
            else field_payload.get("normalized_value")
        )
        if raw_value is None and normalized_value is None:
            continue
        attempts.append(
            {
                "field": str(field_name),
                "layer": "orchestrator_validation",
                "raw_value": raw_value,
                "normalized_value": normalized_value,
            }
        )
    return attempts


def _generic_calculated_totals(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return total-like numeric metrics when the specialist payload exposes them."""

    metrics = _mapping(_mapping(payload).get("metrics"))
    totals: dict[str, Any] = {}
    for field_name, value in metrics.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if field_name.startswith("total_") or field_name.endswith("_count") or field_name.endswith("_qty") or field_name.endswith("_amount"):
                totals[str(field_name)] = value
    return totals


def _generic_declared_totals(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return declared totals when a specialist payload already exposes them."""

    declared = _mapping(_mapping(payload).get("declared_totals"))
    return dict(declared) if declared else {}


def _generic_confidence(payload: Mapping[str, Any] | None, acceptance_outcome: Mapping[str, Any]) -> float:
    """Return the best available confidence figure for accountability output."""

    payload_confidence = _mapping(payload).get("confidence")
    if isinstance(payload_confidence, (int, float)) and not isinstance(payload_confidence, bool):
        return float(payload_confidence)
    acceptance_confidence = acceptance_outcome.get("confidence")
    if isinstance(acceptance_confidence, (int, float)) and not isinstance(acceptance_confidence, bool):
        return float(acceptance_confidence)
    return 0.0


def _generic_recommended_correction(failing_rule: str | None, failing_layer: str) -> str:
    """Return a pragmatic correction hint for generic orchestrator accountability."""

    if failing_rule == "conflicting_record_same_scope":
        return "Resolve the existing branch/date record conflict before resubmitting or approving the replacement."
    if failing_rule == "branch_unresolved":
        return "Add a recognizable branch header or branch line before resubmitting the report."
    if failing_rule == "date_unresolved":
        return "Add a recognizable report date before resubmitting the report."
    if failing_rule in {"missing_raw_text", "invalid_raw_message"}:
        return "Send one non-empty report message with clear branch, date, and body fields."
    if failing_rule in {"mixed_report", "mixed_report_rejected", "mixed_report_split_not_safe"}:
        return "Send each report type in its own WhatsApp message."
    if failing_rule in {"parser_failure", "routing_failure", "fallback_validation_failed"}:
        return "Resend the report in plain text with clearer field structure."
    if failing_layer == "orchestrator_validation":
        return "Correct the rejected fields named in validation and resend the report."
    if failing_layer == "orchestrator_acceptance":
        return "Correct the flagged fields so the report can clear manual review."
    return "Correct the flagged fields and resend the report."


def _first_warning_message(warnings: list[dict[str, str]] | None) -> str | None:
    """Return the first warning message when present."""

    if not isinstance(warnings, list):
        return None
    for warning in warnings:
        if not isinstance(warning, Mapping):
            continue
        message = _string_or_none(warning.get("message"))
        if message is not None:
            return message
    return None


def _validation_error_fields(accountability: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return exact validation error fields from one accountability payload."""

    if not isinstance(accountability, Mapping):
        return {}

    code = (
        _string_or_none(accountability.get("validation_error_code"))
        or _string_or_none(accountability.get("failing_rule"))
    )
    message = (
        _string_or_none(accountability.get("validation_error_message"))
        or _string_or_none(accountability.get("reason_detail"))
    )
    payload: dict[str, Any] = {}
    if code is not None:
        payload["validation_error_code"] = code
    if message is not None:
        payload["validation_error_message"] = message
    return payload


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


def _result_accountability(result: AgentResult) -> dict[str, Any] | None:
    """Return one accountability payload for reviewed or rejected results."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    metadata = result.metadata if isinstance(result.metadata, dict) else {}
    validation = _result_validation_outcome(result)
    accountability = _extract_accountability(
        payload=payload,
        metadata=metadata,
        validation_outcome=validation,
    )
    if accountability is not None:
        return accountability
    return _build_generic_accountability(
        payload=payload,
        validation_outcome=validation,
        acceptance_outcome=_result_acceptance_outcome(result),
        governance_outcome=_result_governance(result),
        warnings=_result_warnings(result),
        status=_result_status(result),
    )


def _result_metadata_extension(result: AgentResult) -> dict[str, Any] | None:
    """Return extra raw-metadata fields derived from the finalized result."""

    metadata = result.metadata if isinstance(result.metadata, dict) else {}
    payload = result.payload if isinstance(result.payload, dict) else {}
    accountability = _result_accountability(result)
    extra: dict[str, Any] = {
        "validation": _result_validation_outcome(result),
        "acceptance": _result_acceptance_outcome(result),
        "candidate_payload": dict(payload),
        **({"accountability": dict(accountability)} if accountability is not None else {}),
    }
    performance_alerts = payload.get("performance_alerts")
    if isinstance(performance_alerts, list):
        extra["performance_alerts"] = [
            dict(alert)
            for alert in performance_alerts
            if isinstance(alert, Mapping)
        ]
    for field_name in ("alert_type", "alert_level", "alert_message", "alert_category"):
        value = _string_or_none(payload.get(field_name))
        if value is not None:
            extra[field_name] = value
    review_queue_path = metadata.get("review_queue_path")
    if isinstance(review_queue_path, str) and review_queue_path.strip():
        extra["review_queue_path"] = review_queue_path.strip()
    duplicate_handling = payload.get("duplicate_handling")
    if isinstance(duplicate_handling, Mapping):
        extra["duplicate_handling"] = dict(duplicate_handling)
    return extra


def _duplicate_override_active(policy_decision: PolicyDecision | None) -> bool:
    """Return whether duplicate intake should keep processing in no-write mode."""

    return (
        policy_decision is not None
        and policy_decision.action == "reject"
        and policy_decision.reason == "duplicate_message"
    )


def _annotate_duplicate_override_result(
    result: AgentResult,
    *,
    raw_audit: RawAuditRecord,
    duplicate_policy_decision: PolicyDecision,
) -> None:
    """Attach duplicate no-write audit context to one live result."""

    duplicate_handling = _duplicate_handling_payload(
        existing_metadata=raw_audit.existing_metadata,
        current_status=_result_status(result),
        current_governance=_result_governance(result),
        duplicate_basis=duplicate_policy_decision.duplicate_basis,
        duplicate_reason=duplicate_policy_decision.reason,
    )
    if isinstance(result.payload, dict):
        result.payload["duplicate_handling"] = duplicate_handling
    if isinstance(result.metadata, dict):
        result.metadata["duplicate_handling"] = dict(duplicate_handling)


def _duplicate_handling_payload(
    *,
    existing_metadata: Mapping[str, Any],
    current_status: str,
    current_governance: Mapping[str, Any],
    duplicate_basis: str | None,
    duplicate_reason: str,
) -> dict[str, Any]:
    """Return the stable duplicate no-write payload projected into results."""

    previous_status = _string_or_none(existing_metadata.get("governance_status")) or _string_or_none(
        existing_metadata.get("processing_status")
    )
    previous_reasons = existing_metadata.get("governance_reasons")
    current_reasons = current_governance.get("reasons")
    return {
        "duplicate": True,
        "write_suppressed": True,
        "reason": duplicate_reason,
        "duplicate_basis": duplicate_basis,
        "previous_governance_status": previous_status,
        "previous_governance_reasons": list(previous_reasons) if isinstance(previous_reasons, list) else [],
        "current_governance_status": current_status,
        "current_governance_reasons": list(current_reasons) if isinstance(current_reasons, list) else [],
    }


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


def _classification_report_family_label(
    *,
    detected_report_type: str,
    specialist_report_type: str | None,
) -> str:
    """Return the routed report family label exposed to downstream governance."""

    if _is_intelligence_report_type(specialist_report_type):
        return INTELLIGENCE_REPORT_FAMILY
    return detected_report_type


def _is_intelligence_report_type(report_type: str | None) -> bool:
    """Return whether one specialist report type is governed as intelligence."""

    return isinstance(report_type, str) and report_type in INTELLIGENCE_SPECIALIST_REPORT_TYPES


def _is_intelligence_report_family(report_family: object) -> bool:
    """Return whether one mixed child family is intelligence-only."""

    return isinstance(report_family, str) and report_family in {"intelligence", "supervisor_control"}


def _has_intelligence_segment(segments: list[object]) -> bool:
    """Return whether one split result contains an intelligence child segment."""

    return any(
        _is_intelligence_report_family(getattr(segment, "report_family_label", None))
        or _is_intelligence_report_family(getattr(segment, "detected_report_family", None))
        for segment in segments
    )


def _can_safely_split_mixed_report(*, mixed_detection, split_result) -> bool:
    """Return whether mixed content can be safely fanned out into children."""

    detected_family_count = len(set(mixed_detection.detected_families))
    if detected_family_count < 2:
        return False
    if len(split_result.segments) < detected_family_count:
        return False
    if any(not segment.raw_text.strip() for segment in split_result.segments):
        return False
    if not _mixed_segments_have_required_scope(split_result.segments):
        return False

    if _has_intelligence_segment(split_result.segments):
        transactional_segments = [
            segment
            for segment in split_result.segments
            if getattr(segment, "blocks_transactional_processing", not _is_intelligence_report_family(segment.detected_report_family))
        ]
        return bool(transactional_segments) and all(
            segment.split_confidence >= MIXED_SPLIT_CONFIDENCE_MIN
            for segment in transactional_segments
        )

    if split_result.split_confidence < MIXED_SPLIT_CONFIDENCE_MIN:
        return False
    return all(segment.split_confidence >= MIXED_SPLIT_CONFIDENCE_MIN for segment in split_result.segments)


def _mixed_segments_have_required_scope(segments: Sequence[object]) -> bool:
    """Return whether each split child has an approved title and resolvable scope."""

    return bool(segments) and all(_mixed_segment_has_required_scope(segment) for segment in segments)


def _mixed_segment_has_required_scope(segment: object) -> bool:
    """Return whether one split segment can stand alone with inherited scope."""

    if not _mixed_segment_has_approved_title(segment):
        return False
    raw_text = getattr(segment, "raw_text", None)
    if not isinstance(raw_text, str) or not raw_text.strip():
        return False
    header_result = normalize_headers(raw_text, max_lines=12)
    branch_resolution = resolve_branch(header_result)
    date_resolution = resolve_report_date(header_result)
    return branch_resolution.branch_hint is not None and date_resolution.iso_date is not None


def _mixed_segment_has_approved_title(segment: object) -> bool:
    """Return whether one split segment starts with an approved report title."""

    report_family = getattr(segment, "detected_report_family", None)
    header_line = getattr(segment, "header_line", None)
    if not isinstance(report_family, str) or not isinstance(header_line, str):
        return False
    approved_titles = APPROVED_MIXED_SPLIT_TITLES.get(report_family)
    if not approved_titles:
        return False
    normalized_header = _normalize_mixed_child_text(header_line)
    return normalized_header in {_normalize_mixed_child_text(title) for title in approved_titles}


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
    duplicate_handling = _mapping(payload.get("duplicate_handling"))
    if duplicate_handling.get("write_suppressed") is True:
        return []
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


def _result_source_message_id(result: AgentResult) -> str | None:
    """Return the source WhatsApp message id for one result when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    return _string_or_none(_mapping(_mapping(payload.get("ingress_envelope")).get("payload")).get("message_id"))


def _result_sender_phone(result: AgentResult) -> str | None:
    """Return the sender phone for one result when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    return _string_or_none(_mapping(_mapping(payload.get("ingress_envelope")).get("payload")).get("sender_phone"))


def _result_report_date(result: AgentResult) -> str | None:
    """Return one result report date when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    report_date = payload.get("report_date")
    if isinstance(report_date, str) and report_date.strip():
        return report_date.strip()
    return None


def _duplicate_basis_from_result(result: AgentResult) -> str:
    """Return one stable duplicate basis from a governed duplicate result."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    policy_guard = _mapping(payload.get("policy_guard"))
    candidate = _string_or_none(policy_guard.get("duplicate_basis"))
    if candidate is not None:
        return candidate

    governance = _mapping(payload.get("governance"))
    reasons = governance.get("reasons")
    primary_reason = None
    if isinstance(reasons, list):
        for reason in reasons:
            primary_reason = _string_or_none(reason)
            if primary_reason is not None:
                break
    if primary_reason == "duplicate_message_id":
        message_id = _string_or_none(governance.get("message_id")) or _result_source_message_id(result)
        if message_id is not None:
            return f"message_id:{message_id}"
    if primary_reason == "duplicate_raw_sha256":
        raw_sha256 = _string_or_none(governance.get("raw_sha256"))
        if raw_sha256 is not None:
            return f"raw_sha256:{raw_sha256}"
    if primary_reason == "duplicate_semantic":
        semantic_sha256 = _string_or_none(governance.get("semantic_sha256"))
        if semantic_sha256 is not None:
            return f"semantic_sha256:{semantic_sha256}"
    reference = _string_or_none(governance.get("duplicate_of"))
    if reference is not None:
        return f"reference:{reference}"
    return primary_reason or "duplicate"


def _duplicate_reference_from_result(result: AgentResult) -> str | None:
    """Return the original duplicate reference from one governed result when present."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    governance = _mapping(payload.get("governance"))
    return _string_or_none(governance.get("duplicate_of"))


def _display_structured_path(path: Path) -> str:
    """Return a stable repo-style structured path for result reporting."""

    parts = path.parts
    if "records" in parts:
        return str(Path(*parts[parts.index("records") :]))
    return str(path)


def _maybe_prechecked_mixed_child_review_summary(
    *,
    child_work_item: WorkItem,
    segment,
    route,
) -> dict[str, Any] | None:
    """Return a review summary when a mixed child is visibly truncated before dispatch."""

    review_reason = _prechecked_mixed_child_review_reason(segment=segment, specialist_report_type=route.specialist_type)
    if review_reason is None:
        return None

    code, message = review_reason
    warning_payload = _make_warning(code=code, severity="warning", message=message)
    routing = _mapping(child_work_item.payload.get("routing"))
    report_date = (
        _string_or_none(routing.get("report_date"))
        or _string_or_none(routing.get("normalized_report_date"))
        or _string_or_none(routing.get("raw_report_date"))
    )
    branch = _string_or_none(routing.get("branch_hint"))
    payload = {
        "signal_type": route.specialist_type,
        "branch": branch,
        "report_date": report_date,
        "status": "needs_review",
        "warnings": [warning_payload],
        "validation_error_code": code,
        "validation_error_message": message,
    }
    validation = {
        "stage": AGENT_NAME,
        "status": "review",
        "accepted": False,
        "reason_codes": [code],
        "rejections": [
            {
                "reason_code": code,
                "reason_detail": message,
            }
        ],
        "details": {
            "validation_error_code": code,
            "validation_error_message": message,
        },
    }
    return {
        "agent_name": route.target_agent,
        "report_type": segment.detected_report_family,
        "report_family": segment.detected_report_family,
        "report_family_label": getattr(segment, "report_family_label", segment.detected_report_family),
        "branch": branch,
        "report_date": report_date,
        "status": "needs_review",
        "blocks_transactional_processing": getattr(segment, "blocks_transactional_processing", True),
        "warnings": [warning_payload],
        "errors": [],
        "metrics": {},
        "validation": validation,
        "output_paths": [],
        "child_index": segment.segment_index + 1,
        "header_line": getattr(segment, "header_line", None),
        "response_report_type": _mixed_child_response_report_type(
            report_family=segment.detected_report_family,
            specialist_report_type=route.specialist_type,
            header_line=getattr(segment, "header_line", None),
        ),
        "response_status": "review",
        "reason": message,
        "validation_error_code": code,
        "validation_error_message": message,
        "lineage": dict(child_work_item.payload.get("lineage", {})),
        "segment_id": segment.segment_id,
        "segment_range": {"start_line": segment.start_line, "end_line": segment.end_line},
        "split_confidence": segment.split_confidence,
        "payload": payload,
    }


def _prechecked_mixed_child_review_reason(
    *,
    segment,
    specialist_report_type: str | None,
) -> tuple[str, str] | None:
    """Return an explicit review code/message for one obviously incomplete child."""

    if specialist_report_type != "supervisor_control":
        return None
    truncated_snippet = _truncated_supervisor_field_snippet(getattr(segment, "raw_text", ""))
    if truncated_snippet is None:
        return None
    return (
        "child_report_incomplete_or_truncated",
        f'incomplete after "{truncated_snippet}"',
    )


def _truncated_supervisor_field_snippet(raw_text: object) -> str | None:
    """Return the trailing truncated supervisor field snippet when one is obvious."""

    if not isinstance(raw_text, str):
        return None
    lines = [line.strip() for line in raw_text.splitlines() if isinstance(line, str) and line.strip()]
    for line in reversed(lines):
        normalized_line = _normalize_mixed_child_text(line)
        if normalized_line in {"supervisor control report", "supervisor control summary"}:
            continue
        if not (line.endswith("...") or line.endswith("\u2026")):
            return None
        prefix = _normalize_mixed_child_text(line.rstrip(".\u2026").rstrip(":"))
        if not prefix:
            return None
        if any(alias.startswith(prefix) and alias != prefix for alias in _TRUNCATED_SUPERVISOR_FIELD_ALIASES):
            return line.rstrip(".\u2026").rstrip() + "\u2026"
        return None
    return None


def _normalize_mixed_child_text(value: object) -> str:
    """Return a loose ASCII-safe normalization for mixed-child display keys."""

    if not isinstance(value, str):
        return ""
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


def _mixed_child_response_report_type(
    *,
    report_family: str | None,
    specialist_report_type: str | None,
    header_line: str | None,
) -> str:
    """Return the response-facing report type for one mixed child summary."""

    normalized_header = _normalize_mixed_child_text(header_line)
    if report_family == "sales_income" or specialist_report_type == "sales":
        return "day_end_sales"
    if report_family == "supervisor_control" or specialist_report_type == "supervisor_control":
        if "summary" in normalized_header:
            return "supervisor_control_summary"
        return "supervisor_control"
    return report_family or specialist_report_type or "unknown"


def _mixed_child_response_status(status: str | None) -> str | None:
    """Return the response-facing child status token."""

    if status == "needs_review":
        return "review"
    return status


def _mixed_child_reason(
    *,
    result: AgentResult,
    specialist_report_type: str | None,
) -> str | None:
    """Return a short accountability reason for one mixed child result."""

    status = _result_status(result)
    validation = _result_validation_outcome(result)
    if specialist_report_type == "sales" and status in {"accepted", "accepted_with_warning"} and validation.get("accepted") is True:
        return "totals_reconciled"

    payload = result.payload if isinstance(result.payload, dict) else {}
    validation_error = _mixed_child_validation_error_fields(
        payload=payload,
        validation=validation,
        warnings=_result_warnings(result),
    )
    return _string_or_none(validation_error.get("validation_error_message")) or _string_or_none(validation_error.get("validation_error_code"))


def _mixed_child_validation_error_fields(
    *,
    payload: Mapping[str, Any],
    validation: Mapping[str, Any],
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return the most specific validation error fields for one mixed child summary."""

    code = _string_or_none(payload.get("validation_error_code"))
    message = _string_or_none(payload.get("validation_error_message"))
    details = _mapping(validation.get("details"))
    if code is None:
        code = _string_or_none(details.get("validation_error_code"))
    if message is None:
        message = _string_or_none(details.get("validation_error_message"))

    rejections = validation.get("rejections")
    if isinstance(rejections, list):
        first_rejection = next((rejection for rejection in rejections if isinstance(rejection, Mapping)), None)
        if first_rejection is not None:
            if code is None:
                code = _string_or_none(first_rejection.get("reason_code")) or _string_or_none(first_rejection.get("code"))
            if message is None:
                message = _string_or_none(first_rejection.get("reason_detail")) or _string_or_none(first_rejection.get("message"))

    if code is None or message is None:
        for warning in warnings:
            warning_code = _string_or_none(warning.get("code"))
            if warning_code not in MIXED_CHILD_REVIEW_WARNING_CODES and _string_or_none(warning.get("severity")) != "error":
                continue
            if code is None:
                code = warning_code
            if message is None:
                message = _string_or_none(warning.get("message"))
            break

    validation_error: dict[str, Any] = {}
    if code is not None:
        validation_error["validation_error_code"] = code
    if message is not None:
        validation_error["validation_error_message"] = message
    return validation_error


def _summary_blocks_transactional_processing(summary: Mapping[str, Any]) -> bool:
    """Return whether one mixed child summary should block transactional acceptance."""

    blocks = summary.get("blocks_transactional_processing")
    if isinstance(blocks, bool):
        return blocks
    report_family = summary.get("report_family")
    if _is_intelligence_report_family(report_family):
        return False
    return not _is_intelligence_report_family(summary.get("report_family_label"))


def _child_summary_has_warnings(summary: Mapping[str, Any]) -> bool:
    """Return whether one mixed child summary carries warning entries."""

    payload = _mapping(summary.get("payload"))
    warnings = payload.get("warnings")
    return isinstance(warnings, list) and any(isinstance(warning, Mapping) for warning in warnings)


def _is_sales_supervisor_mixed_pair(child_summaries: Sequence[Mapping[str, Any]]) -> bool:
    """Return whether the mixed child set is exactly one sales child and one supervisor child."""

    if len(child_summaries) != 2:
        return False
    detected_families = {
        _string_or_none(summary.get("report_family")) or _string_or_none(summary.get("report_type"))
        for summary in child_summaries
        if isinstance(summary, Mapping)
    }
    return detected_families == {"sales_income", "supervisor_control"}


def _sales_supervisor_mixed_parent_decision(
    child_summaries: Sequence[Mapping[str, Any]],
    *,
    success_statuses: set[str],
    warning_statuses: set[str],
) -> tuple[str, str | None] | None:
    """Return the mixed parent decision for the exact sales + supervisor pair."""

    if not _is_sales_supervisor_mixed_pair(child_summaries):
        return None

    sales_summary: Mapping[str, Any] | None = None
    supervisor_summary: Mapping[str, Any] | None = None
    for summary in child_summaries:
        report_family = _string_or_none(summary.get("report_family")) or _string_or_none(
            summary.get("report_type")
        )
        if report_family == "sales_income":
            sales_summary = summary
            continue
        if report_family == "supervisor_control":
            supervisor_summary = summary

    if sales_summary is None or supervisor_summary is None:
        return "needs_review", "mixed_child_requires_review"

    sales_status = _string_or_none(sales_summary.get("status"))
    supervisor_status = _string_or_none(supervisor_summary.get("status"))
    if sales_status is None or supervisor_status is None:
        return "needs_review", "mixed_child_requires_review"

    sales_succeeded = sales_status in success_statuses
    supervisor_succeeded = supervisor_status in success_statuses
    if not sales_succeeded:
        return "needs_review", "mixed_child_requires_review"
    if not supervisor_succeeded:
        return "accepted_with_warning", None

    any_child_has_warning = (
        sales_status in warning_statuses
        or supervisor_status in warning_statuses
        or _child_summary_has_warnings(sales_summary)
        or _child_summary_has_warnings(supervisor_summary)
    )
    if any_child_has_warning:
        return "accepted_with_warning", None
    return "accepted", None


def _mixed_parent_decision(
    *,
    child_results: list[AgentResult],
    child_summaries: list[dict[str, Any]],
) -> tuple[str, str | None]:
    """Return aggregate status and reason for a mixed parent result."""

    del child_results

    if not child_summaries:
        return "invalid_input", None

    child_statuses = [
        summary.get("status")
        for summary in child_summaries
        if isinstance(summary.get("status"), str)
    ]
    if not child_statuses or len(child_statuses) != len(child_summaries):
        return "needs_review", "mixed_child_requires_review"
    duplicate_reason = _mixed_parent_duplicate_reason(child_summaries)
    if duplicate_reason is not None:
        return "duplicate", duplicate_reason

    success_statuses = {"accepted", "accepted_with_warning"}
    warning_statuses = {"accepted_with_warning"}

    transactional_summaries = [
        summary
        for summary in child_summaries
        if _summary_blocks_transactional_processing(summary)
    ]
    intelligence_summaries = [
        summary
        for summary in child_summaries
        if not _summary_blocks_transactional_processing(summary)
    ]

    transactional_statuses = [
        summary.get("status")
        for summary in transactional_summaries
        if isinstance(summary.get("status"), str)
    ]
    if len(transactional_statuses) != len(transactional_summaries):
        return "needs_review", "mixed_child_requires_review"

    intelligence_statuses = [
        summary.get("status")
        for summary in intelligence_summaries
        if isinstance(summary.get("status"), str)
    ]
    if len(intelligence_statuses) != len(intelligence_summaries):
        return "needs_review", "mixed_child_requires_review"

    sales_supervisor_decision = _sales_supervisor_mixed_parent_decision(
        child_summaries,
        success_statuses=success_statuses,
        warning_statuses=warning_statuses,
    )
    if sales_supervisor_decision is not None:
        return sales_supervisor_decision

    if any(status not in success_statuses for status in transactional_statuses):
        return "needs_review", "mixed_child_requires_review"
    if any(status not in success_statuses for status in intelligence_statuses):
        return "needs_review", "mixed_child_requires_review"
    if any(_child_summary_has_warnings(summary) for summary in intelligence_summaries):
        return "accepted_with_warning", "mixed_intelligence_warning"
    if any(status in warning_statuses for status in transactional_statuses):
        return "accepted_with_warning", None
    return "accepted", None


def _mixed_parent_duplicate_reason(child_summaries: Sequence[Mapping[str, Any]]) -> str | None:
    """Return one duplicate reason when every mixed child resolved as duplicate."""

    if not child_summaries:
        return None
    if any(_string_or_none(summary.get("status")) != "duplicate" for summary in child_summaries):
        return None
    prioritized_reasons = (
        "duplicate_raw_sha256",
        "duplicate_message_id",
        "duplicate_semantic",
        "duplicate_message",
    )
    reasons = [_mixed_child_duplicate_reason(summary) for summary in child_summaries]
    for candidate in prioritized_reasons:
        if candidate in reasons:
            return candidate
    return next((reason for reason in reasons if reason is not None), "duplicate_raw_sha256")


def _mixed_child_duplicate_reason(summary: Mapping[str, Any]) -> str | None:
    """Return the duplicate governance reason carried by one mixed child summary."""

    payload = _mapping(summary.get("payload"))
    governance = _mapping(payload.get("governance"))
    reasons = governance.get("reasons")
    if isinstance(reasons, list):
        for reason in reasons:
            cleaned = _string_or_none(reason)
            if cleaned is not None and cleaned.startswith("duplicate_"):
                return cleaned
    for field_name in ("validation_error_code", "reason"):
        cleaned = _string_or_none(summary.get(field_name))
        if cleaned is not None and cleaned.startswith("duplicate_"):
            return cleaned
    return None


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
        "report_family": _classification_report_family_label(
            detected_report_type=segment.detected_report_family,
            specialist_report_type=specialist_report_type,
        ),
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
