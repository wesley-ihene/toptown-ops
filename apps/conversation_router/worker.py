"""Route known outcomes into one deterministic conversational classification."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from apps.conversation_context import resolve_context_flags
from apps.conversation_policy.rules import ALLOWED_RESPONSE_TYPES, normalize_response_reason
from packages.validation import build_feedback_diagnostics
from packages.signal_contracts.agent_result import AgentResult

_ACCEPTED_STATUSES = {"accepted", "accepted_with_warning", "accepted_split", "ready"}
_REVIEW_STATUSES = {"needs_review", "review", "conflict_blocked"}
_REJECTED_STATUSES = {"rejected", "invalid_input"}
_DUPLICATE_STATUSES = {"duplicate"}
_UNKNOWN_REASONS = {
    "unknown_report_type",
    "unsupported_payload_kind",
    "empty_input",
    "missing_raw_text",
    "unknown_report_family",
}
_REVIEW_REASONS = {
    "route_requires_review",
    "confidence_missing",
    "confidence_between_review_and_accept_thresholds",
    "strict_candidate_below_reject_threshold",
    "mixed_report_split_not_safe",
    "mixed_child_requires_review",
    "review_queue_output",
}


def route_conversation_response(
    outcome: AgentResult | Mapping[str, Any] | None = None,
    *,
    source_message_id: str | None = None,
    sender_phone: str | None = None,
    channel: str = "whatsapp",
    conversation_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return one normalized Phase C1 response context."""

    payload, metadata = _payload_and_metadata_from_outcome(outcome)
    response = _base_response_context(channel=channel)

    governance_status = _governance_status(payload)
    source_status = _string_or_none(payload.get("status"))
    processing_status = _processing_status(payload, metadata)
    classification = _classification(payload, metadata)
    review_queue_path = _review_queue_path(payload, metadata)
    structured_output_path = _structured_output_path(payload, metadata)
    report_type = (
        _string_or_none(payload.get("report_type"))
        or _string_or_none(payload.get("signal_subtype"))
        or _string_or_none(payload.get("signal_type"))
        or _string_or_none(payload.get("attempted_report_type"))
        or _string_or_none(classification.get("report_type"))
    )
    branch = (
        _string_or_none(payload.get("branch"))
        or _string_or_none(payload.get("branch_hint"))
        or _string_or_none(metadata.get("branch"))
        or _string_or_none(_mapping(metadata.get("governance_context")).get("branch_hint"))
        or _string_or_none(_mapping(payload.get("routing")).get("branch_hint"))
        or _string_or_none(_mapping(payload.get("metadata")).get("branch_hint"))
    )
    report_date = (
        _string_or_none(payload.get("report_date"))
        or _string_or_none(metadata.get("report_date"))
        or _string_or_none(_mapping(payload.get("normalized_report")).get("report_date"))
        or _string_or_none(_mapping(payload.get("rejection_feedback")).get("report_date"))
        or _string_or_none(_mapping(payload.get("raw_record")).get("report_date"))
        or _string_or_none(_mapping(metadata.get("governance_context")).get("report_date"))
        or _string_or_none(_mapping(metadata.get("governance_context")).get("normalized_report_date"))
        or _string_or_none(_mapping(metadata.get("governance_context")).get("raw_report_date"))
        or _string_or_none(_mapping(payload.get("routing")).get("report_date"))
        or _string_or_none(_mapping(payload.get("routing")).get("normalized_report_date"))
        or _string_or_none(_mapping(payload.get("routing")).get("raw_report_date"))
    )
    reason = _reason_from_outcome(payload, metadata)
    is_replay = _is_replay(payload, metadata)
    base_response_type = _classify_response_type(
        payload=payload,
        metadata=metadata,
        governance_status=governance_status,
        source_status=source_status,
        processing_status=processing_status,
        report_type=report_type,
        reason=reason,
        review_queue_path=review_queue_path,
        structured_output_path=structured_output_path,
    )
    context_flags = resolve_context_flags(
        current_response_context={
            "response_type": base_response_type,
            "report_type": report_type,
            "branch": branch,
            "report_date": report_date,
            "is_replay": is_replay,
        },
        stored_context=conversation_context,
    )
    response_type = _contextual_response_type(base_response_type, context_flags)
    normalized_reason = normalize_response_reason(response_type, reason) if response_type is not None else None

    response.update(
        {
            "source_message_id": _first_text(
                source_message_id,
                _string_or_none(payload.get("source_message_id")),
                _string_or_none(metadata.get("source_message_id")),
                _string_or_none(_mapping(_mapping(payload.get("ingress_envelope")).get("payload")).get("message_id")),
                _string_or_none(_mapping(_mapping(payload.get("metadata")).get("message")).get("id")),
            ),
            "sender_phone": _first_text(
                sender_phone,
                _string_or_none(payload.get("sender_phone")),
                _string_or_none(metadata.get("sender_phone")),
                _string_or_none(_mapping(_mapping(payload.get("ingress_envelope")).get("payload")).get("sender_phone")),
                _string_or_none(_mapping(_mapping(payload.get("metadata")).get("message")).get("from")),
            ),
            "branch": branch,
            "report_type": report_type,
            "report_date": report_date,
            "governance_status": governance_status or source_status,
            "reason": normalized_reason,
            "review_queue_path": review_queue_path,
            "structured_output_path": structured_output_path,
            "is_replay": is_replay,
            "response_type": response_type,
            "context_flags": context_flags,
            "should_reply": response_type in ALLOWED_RESPONSE_TYPES,
            "feedback_context": _feedback_context(
                payload=payload,
                metadata=metadata,
                report_type=report_type,
                branch=branch,
                report_date=report_date,
                reason=normalized_reason,
                structured_output_path=structured_output_path,
                review_queue_path=review_queue_path,
            ),
        }
    )
    feedback_context = response.get("feedback_context")
    if isinstance(feedback_context, Mapping):
        diagnostics = build_feedback_diagnostics(
            response_type=response_type,
            report_type=report_type,
            branch=branch,
            report_date=report_date,
            reason=normalized_reason,
            governance_status=response.get("governance_status"),
            feedback_context=feedback_context,
        )
        if diagnostics is not None:
            updated_feedback_context = dict(feedback_context)
            updated_feedback_context["diagnostics"] = diagnostics
            response["feedback_context"] = updated_feedback_context
    return response


def _contextual_response_type(
    response_type: str | None,
    context_flags: Mapping[str, Any],
) -> str | None:
    if response_type == "accepted_ack" and context_flags.get("correction_success") is True:
        return "correction_accepted_ack"
    if response_type == "review_ack" and context_flags.get("correction_success") is True:
        return "correction_review_ack"
    if response_type == "rejected_fix_request" and context_flags.get("repeat_failure") is True:
        return "correction_repeat_fix_request"
    return response_type


def _classify_response_type(
    *,
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any],
    governance_status: str | None,
    source_status: str | None,
    processing_status: str | None,
    report_type: str | None,
    reason: str | None,
    review_queue_path: str | None,
    structured_output_path: str | None,
) -> str | None:
    status = governance_status or source_status
    duplicate_reason = _duplicate_reason(payload, metadata, reason)
    duplicate_override = _duplicate_override_active(payload, metadata)
    if processing_status in _DUPLICATE_STATUSES or status in _DUPLICATE_STATUSES:
        return _duplicate_response_type(duplicate_reason)
    if duplicate_reason is not None and not duplicate_override:
        return _duplicate_response_type(duplicate_reason)
    if reason in _UNKNOWN_REASONS:
        return "unknown_message_guidance"
    if report_type == "unknown":
        return "unknown_message_guidance"
    if status in _ACCEPTED_STATUSES and (structured_output_path is not None or duplicate_override):
        return "accepted_ack"
    if review_queue_path is not None:
        return "review_ack"
    if status in _REVIEW_STATUSES or reason in _REVIEW_REASONS:
        return "review_ack"
    if status in _REJECTED_STATUSES:
        return "rejected_fix_request"
    if _string_or_none(_mapping(_mapping(payload.get("routing")).get("classification")).get("report_type")) == "unknown":
        return "unknown_message_guidance"
    return None


def _duplicate_response_type(duplicate_reason: str | None) -> str:
    """Return the outbound duplicate response type for the current duplicate reason."""

    if duplicate_reason == "duplicate_raw_sha256":
        return "duplicate_ack"
    return "duplicate_notice"


def _reason_from_outcome(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    governance = _mapping(payload.get("governance"))
    reasons = governance.get("reasons")
    if isinstance(reasons, list):
        for item in reasons:
            cleaned = _string_or_none(item)
            if cleaned is not None:
                return cleaned

    validation = _mapping(payload.get("validation"))
    validation_reasons = validation.get("reasons")
    if isinstance(validation_reasons, list):
        for item in validation_reasons:
            if isinstance(item, Mapping):
                cleaned = _string_or_none(item.get("code"))
                if cleaned is not None:
                    return cleaned

    metadata_validation = _mapping(metadata.get("validation"))
    metadata_reason_codes = metadata_validation.get("reason_codes")
    if isinstance(metadata_reason_codes, list):
        for item in metadata_reason_codes:
            cleaned = _string_or_none(item)
            if cleaned is not None:
                return cleaned
    metadata_rejections = metadata_validation.get("rejections")
    if isinstance(metadata_rejections, list):
        for item in metadata_rejections:
            if isinstance(item, Mapping):
                cleaned = _string_or_none(item.get("reason_code")) or _string_or_none(item.get("code"))
                if cleaned is not None:
                    return cleaned

    pre_ingestion = _mapping(payload.get("pre_ingestion_validation"))
    pre_reasons = pre_ingestion.get("reasons")
    if isinstance(pre_reasons, list):
        for item in pre_reasons:
            if isinstance(item, Mapping):
                cleaned = _string_or_none(item.get("code"))
                if cleaned is not None:
                    return cleaned

    acceptance = _mapping(payload.get("acceptance"))
    cleaned = _string_or_none(acceptance.get("reason"))
    if cleaned is not None:
        return cleaned

    fallback = _mapping(payload.get("fallback"))
    acceptance = _mapping(fallback.get("acceptance"))
    cleaned = _string_or_none(acceptance.get("reason"))
    if cleaned is not None:
        return cleaned

    routing = _mapping(payload.get("routing"))
    cleaned = _string_or_none(routing.get("review_reason")) or _string_or_none(routing.get("route_reason"))
    if cleaned is not None:
        return cleaned

    policy_guard = _mapping(payload.get("policy_guard"))
    cleaned = _string_or_none(policy_guard.get("reason"))
    if cleaned is not None:
        return cleaned

    cleaned = _string_or_none(metadata.get("reason"))
    if cleaned is not None:
        return cleaned

    return None


def _governance_status(payload: Mapping[str, Any]) -> str | None:
    governance = _mapping(payload.get("governance"))
    return _string_or_none(governance.get("status"))


def _classification(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    classification = payload.get("classification")
    if isinstance(classification, Mapping):
        return classification
    routing = _mapping(payload.get("routing"))
    routed_classification = routing.get("classification")
    if isinstance(routed_classification, Mapping):
        return routed_classification
    if isinstance(routed_classification, str):
        return {"report_type": routed_classification}
    metadata_classification = metadata.get("classification")
    if isinstance(metadata_classification, Mapping):
        return metadata_classification
    return {}


def _processing_status(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    for candidate in (
        payload.get("processing_status"),
        _mapping(payload.get("raw_record")).get("processing_status"),
        _mapping(metadata.get("raw_record")).get("processing_status"),
    ):
        cleaned = _string_or_none(candidate)
        if cleaned is not None:
            return cleaned
    return None


def _payload_and_metadata_from_outcome(
    outcome: AgentResult | Mapping[str, Any] | None,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if isinstance(outcome, AgentResult):
        payload = outcome.payload if isinstance(outcome.payload, Mapping) else {}
        metadata = outcome.metadata if isinstance(outcome.metadata, Mapping) else {}
        return payload, metadata
    if isinstance(outcome, Mapping):
        return outcome, {}
    return {}, {}


def _review_queue_path(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    candidates = [
        payload.get("review_queue_path"),
        metadata.get("review_queue_path"),
        _mapping(payload.get("fallback")).get("review_queue_path"),
        _mapping(payload.get("fallback")).get("fallback_review_queue_path"),
    ]
    for candidate in candidates:
        cleaned = _string_or_none(candidate)
        if cleaned is not None:
            return cleaned
    return None


def _structured_output_path(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    explicit = _string_or_none(payload.get("structured_output_path")) or _string_or_none(metadata.get("structured_output_path"))
    if explicit is not None:
        return explicit
    outputs = payload.get("outputs")
    if isinstance(outputs, list):
        for item in outputs:
            cleaned = _string_or_none(item)
            if cleaned is not None:
                return cleaned
    downstream = _mapping(metadata.get("downstream_references"))
    for field_name in ("structured_path", "structured_output_path", "source_record_path"):
        cleaned = _string_or_none(downstream.get(field_name))
        if cleaned is not None and "/structured/" in cleaned:
            return cleaned
    return None


def _duplicate_reason(payload: Mapping[str, Any], metadata: Mapping[str, Any], reason: str | None) -> str | None:
    if _mapping(payload.get("policy_guard")).get("duplicate") is True:
        return _string_or_none(reason) or "duplicate_message"
    duplicate_handling = _first_mapping(payload.get("duplicate_handling"), metadata.get("duplicate_handling"))
    cleaned = _string_or_none(duplicate_handling.get("reason"))
    if cleaned is not None:
        return cleaned
    governance = _mapping(payload.get("governance"))
    governance_reasons = governance.get("reasons")
    if isinstance(governance_reasons, list):
        for item in governance_reasons:
            cleaned = _string_or_none(item)
            if cleaned is not None and cleaned.startswith("duplicate_"):
                return cleaned
    cleaned = _string_or_none(metadata.get("duplicate_reason"))
    if cleaned is not None:
        return cleaned
    if isinstance(reason, str) and reason.startswith("duplicate_"):
        return reason
    return None


def _duplicate_override_active(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> bool:
    """Return whether duplicate handling should preserve the current routed decision."""

    duplicate_handling = _first_mapping(payload.get("duplicate_handling"), metadata.get("duplicate_handling"))
    return duplicate_handling.get("write_suppressed") is True and duplicate_handling.get("duplicate") is True


def _is_replay(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> bool:
    replay = payload.get("replay")
    if isinstance(replay, Mapping) and replay.get("is_replay") is True:
        return True
    ingress_payload = _mapping(_mapping(payload.get("ingress_envelope")).get("payload"))
    replay = ingress_payload.get("replay")
    if isinstance(replay, Mapping) and replay.get("is_replay") is True:
        return True
    return metadata.get("is_replay") is True


def _base_response_context(*, channel: str) -> dict[str, Any]:
    return {
        "response_type": None,
        "channel": channel,
        "source_message_id": None,
        "sender_phone": None,
        "branch": None,
        "report_type": None,
        "report_date": None,
        "governance_status": None,
        "reason": None,
        "review_queue_path": None,
        "structured_output_path": None,
        "context_flags": {
            "previous_response_type": None,
            "correction_attempt": False,
            "repeat_failure": False,
            "correction_success": False,
        },
        "should_reply": False,
        "is_replay": False,
        "feedback_context": None,
    }


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _feedback_context(
    *,
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any],
    report_type: str | None,
    branch: str | None,
    report_date: str | None,
    reason: str | None,
    structured_output_path: str | None,
    review_queue_path: str | None,
) -> dict[str, Any] | None:
    acceptance = _first_mapping(
        metadata.get("acceptance"),
        payload.get("acceptance"),
        _mapping(payload.get("fallback")).get("acceptance"),
        _mapping(payload.get("fallback")).get("fallback_acceptance"),
    )
    validation = _first_mapping(
        metadata.get("validation"),
        payload.get("validation"),
        payload.get("pre_ingestion_validation"),
    )
    candidate_validation = _first_mapping(metadata.get("candidate_validation"))
    raw_record = _mapping(payload.get("raw_record"))
    ingress_payload = _mapping(_mapping(payload.get("ingress_envelope")).get("payload"))
    raw_message = _mapping(payload.get("raw_message"))
    governance_context = _mapping(metadata.get("governance_context"))
    routing = _mapping(payload.get("routing"))
    routing_classification = routing.get("classification")
    warnings = payload.get("warnings")
    items = payload.get("items")
    metrics = _mapping(payload.get("metrics"))
    governance = _mapping(payload.get("governance"))
    fanout = _mapping(payload.get("fanout"))
    mixed_children = _mapping_list(fanout.get("children"))

    feedback = {
        "report_type": report_type,
        "branch": branch or _string_or_none(governance_context.get("branch_hint")) or _string_or_none(routing.get("branch_hint")),
        "report_date": (
            report_date
            or _string_or_none(governance_context.get("report_date"))
            or _string_or_none(governance_context.get("normalized_report_date"))
            or _string_or_none(governance_context.get("raw_report_date"))
            or _string_or_none(routing.get("report_date"))
            or _string_or_none(routing.get("normalized_report_date"))
            or _string_or_none(routing.get("raw_report_date"))
        ),
        "normalized_report_date": _string_or_none(governance_context.get("normalized_report_date"))
        or _string_or_none(routing.get("normalized_report_date")),
        "raw_report_date": _string_or_none(governance_context.get("raw_report_date"))
        or _string_or_none(routing.get("raw_report_date")),
        "route": _string_or_none(routing_classification)
        or _string_or_none(_mapping(routing_classification).get("report_type"))
        or report_type,
        "route_status": _string_or_none(routing.get("route_status")),
        "reason": reason,
        "status": _string_or_none(payload.get("status")),
        "governance_status": _string_or_none(governance.get("status")),
        "confidence": _first_number(
            payload.get("confidence"),
            acceptance.get("confidence"),
        ),
        "acceptance": dict(acceptance) if acceptance else None,
        "validation": dict(validation) if validation else None,
        "candidate_validation": dict(candidate_validation) if candidate_validation else None,
        "warnings": _mapping_list(warnings),
        "metrics": dict(metrics) if metrics else None,
        "items": _mapping_list(items),
        "agent_name": _string_or_none(payload.get("source_agent")),
        "raw_text": _first_text(
            _string_or_none(raw_message.get("text")),
            _string_or_none(raw_message.get("normalized_text")),
            _string_or_none(governance_context.get("raw_text")),
            _string_or_none(metadata.get("raw_text")),
        ),
        "raw_txt_path": _first_text(
            raw_record.get("raw_txt_path"),
            ingress_payload.get("raw_txt_path"),
            governance_context.get("raw_txt_path"),
        ),
        "raw_meta_path": _first_text(
            raw_record.get("raw_meta_path"),
            ingress_payload.get("raw_meta_path"),
            governance_context.get("raw_meta_path"),
        ),
        "human_tolerance": dict(_mapping(governance_context.get("human_tolerance")))
        or dict(_mapping(payload.get("human_tolerance")))
        or None,
        "structured_output_path": structured_output_path,
        "review_queue_path": review_queue_path,
        "mixed_detection": dict(_mapping(payload.get("mixed_detection"))) or None,
        "mixed_children": mixed_children or None,
    }

    if not any(
        value
        for key, value in feedback.items()
        if key not in {"warnings", "items"} or value
    ):
        return None
    return feedback


def _first_mapping(*values: object) -> Mapping[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return value
    return {}


def _mapping_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            normalized.append(dict(item))
    return normalized


def _first_number(*values: object) -> float | None:
    for value in values:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _first_text(*values: object) -> str | None:
    for value in values:
        cleaned = _string_or_none(value)
        if cleaned is not None:
            return cleaned
    return None
