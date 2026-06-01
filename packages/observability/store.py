"""Persist lightweight daily observability metrics and summary artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping as MappingABC
from pathlib import Path
from typing import Any, Mapping

from packages.branch_registry import canonical_branch_slug_or_none
from packages.feedback_store import build_action_feedback_state
import packages.record_store.paths as record_paths
from packages.record_store.naming import safe_segment
from packages.record_store.writer import write_json_file
from packages.validation import (
    append_latency_event,
    append_replay_event,
    build_pipeline_health,
    merge_consistency_snapshot,
)


def record_processing_event(
    *,
    report_date: str,
    branch: str,
    report_type: str,
    outcome: str,
    parse_mode: str,
    parser_used: str,
    confidence: float | None,
    warnings: list[dict[str, Any]],
    received_at_utc: str | None = None,
    completed_at_utc: str | None = None,
    duration_ms: int | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Update daily observability metrics for one processing outcome."""

    branch = _observability_branch(branch)
    summary = _load_summary(report_date, output_root=output_root)
    normalized_outcome = _normalized_processing_outcome(outcome)
    summary["summary"]["intake_volume"] += 1
    if normalized_outcome == "accepted":
        summary["summary"]["accept_count"] += 1
    elif normalized_outcome == "review":
        summary["summary"]["review_count"] += 1
    elif normalized_outcome == "rejected":
        summary["summary"]["reject_count"] += 1

    if parse_mode == "fallback":
        summary["summary"]["fallback_activation_count"] += 1

    agent_metrics = summary["agents"].setdefault(
        parser_used,
        {
            "processed_count": 0,
            "failure_count": 0,
            "failure_rate": 0.0,
        },
    )
    agent_metrics["processed_count"] += 1
    if normalized_outcome == "rejected":
        agent_metrics["failure_count"] += 1
    agent_metrics["failure_rate"] = _ratio(agent_metrics["failure_count"], agent_metrics["processed_count"])

    branch_metrics = summary["branches"].setdefault(
        branch,
        {
            "processed_count": 0,
            "accept_count": 0,
            "review_count": 0,
            "reject_count": 0,
            "warning_record_count": 0,
            "warning_total": 0,
            "low_confidence_count": 0,
            "data_quality_score": 1.0,
        },
    )
    branch_metrics["processed_count"] += 1
    if normalized_outcome == "accepted":
        branch_metrics["accept_count"] += 1
    elif normalized_outcome == "review":
        branch_metrics["review_count"] += 1
    elif normalized_outcome == "rejected":
        branch_metrics["reject_count"] += 1
    warning_count = len(warnings)
    if warning_count:
        branch_metrics["warning_record_count"] += 1
        branch_metrics["warning_total"] += warning_count
    if isinstance(confidence, (int, float)) and not isinstance(confidence, bool) and float(confidence) < 0.7:
        branch_metrics["low_confidence_count"] += 1
    branch_metrics["data_quality_score"] = _branch_quality_score(branch_metrics)

    summary["summary"]["fallback_activation_rate"] = _ratio(
        summary["summary"]["fallback_activation_count"],
        summary["summary"]["intake_volume"],
    )
    _write_summary(report_date, summary, output_root=output_root)
    _write_daily_artifact(
        "pipeline_health",
        report_date,
        build_pipeline_health(report_date, summary),
        output_root=output_root,
    )
    _record_latency_artifact(
        report_date=report_date,
        event_type="processing",
        branch=branch,
        report_type=report_type,
        duration_ms=duration_ms,
        started_at_utc=received_at_utc,
        finished_at_utc=completed_at_utc,
        output_root=output_root,
    )
    return str(_summary_path(report_date, output_root=output_root))


def record_export_event(
    *,
    report_date: str,
    branch: str,
    success: bool,
    manifest_summary: dict[str, Any] | None = None,
    error: str | None = None,
    started_at_utc: str | None = None,
    finished_at_utc: str | None = None,
    duration_ms: int | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Update daily observability metrics for one colony export outcome."""

    branch = _observability_branch(branch)
    summary = _load_summary(report_date, output_root=output_root)
    exports = summary["exports"]
    if success:
        exports["success_count"] += 1
    else:
        exports["failure_count"] += 1
    branch_exports = exports["by_branch"].setdefault(
        branch,
        {"success_count": 0, "failure_count": 0},
    )
    if success:
        branch_exports["success_count"] += 1
    else:
        branch_exports["failure_count"] += 1
    if manifest_summary is not None:
        exports["last_manifest_summary"] = dict(manifest_summary)
    if error is not None:
        exports["last_error"] = error
    _write_summary(report_date, summary, output_root=output_root)
    _write_daily_artifact(
        "pipeline_health",
        report_date,
        build_pipeline_health(report_date, summary),
        output_root=output_root,
    )
    _record_latency_artifact(
        report_date=report_date,
        event_type="export",
        branch=branch,
        report_type="colony_export",
        duration_ms=duration_ms,
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
        output_root=output_root,
    )
    return str(_summary_path(report_date, output_root=output_root))


def record_replay_event(
    *,
    report_date: str,
    mode: str,
    source: str,
    branch: str | None,
    validation_mode: str,
    result: dict[str, Any],
    duration_ms: int | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Append one replay result into the daily replay audit artifact."""

    canonical_branch = _observability_branch(branch)
    payload = load_daily_artifact("replay_audit", report_date, output_root=output_root) or {}
    updated = append_replay_event(
        payload,
        report_date=report_date,
        mode=mode,
        source=source,
        branch=canonical_branch,
        validation_mode=validation_mode,
        result=result,
    )
    _write_daily_artifact("replay_audit", report_date, updated, output_root=output_root)
    _record_latency_artifact(
        report_date=report_date,
        event_type="replay",
        branch=canonical_branch,
        report_type=mode,
        duration_ms=duration_ms,
        output_root=output_root,
    )
    return str(_daily_artifact_path("replay_audit", report_date, output_root=output_root))


def record_consistency_snapshot(
    *,
    report_date: str,
    branch: str,
    snapshot: dict[str, Any],
    output_root: str | Path | None = None,
) -> str:
    """Merge one consistency snapshot into the daily artifact."""

    branch = _observability_branch(branch)
    payload = load_daily_artifact("consistency", report_date, output_root=output_root) or {}
    updated = merge_consistency_snapshot(
        payload,
        report_date=report_date,
        branch=branch,
        snapshot=snapshot,
    )
    _write_daily_artifact("consistency", report_date, updated, output_root=output_root)
    return str(_daily_artifact_path("consistency", report_date, output_root=output_root))


def record_pre_ingestion_validation_event(
    *,
    report_date: str,
    received_at: str | None,
    message_id: str | None,
    payload_kind: str,
    result: Mapping[str, Any],
    raw_txt_path: str | None = None,
    raw_meta_path: str | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Append one pre-ingestion validation outcome into the daily artifact."""

    payload = load_daily_artifact("pre_ingestion_validation", report_date, output_root=output_root) or {
        "report_date": report_date,
        "summary": {
            "accepted": 0,
            "cleaned": 0,
            "rejected": 0,
            "mixed_report_risk": 0,
            "empty_input": 0,
            "unsupported_payload_kind": 0,
        },
        "events": [],
    }

    summary = payload.setdefault("summary", {})
    for field_name in ("accepted", "cleaned", "rejected", "mixed_report_risk", "empty_input", "unsupported_payload_kind"):
        summary[field_name] = int(summary.get(field_name, 0))

    status = result.get("status")
    if status in {"accepted", "cleaned", "rejected"}:
        summary[status] += 1

    detected_risks = result.get("detected_risks")
    if isinstance(detected_risks, list):
        for risk in detected_risks:
            if isinstance(risk, str) and risk in summary:
                summary[risk] += 1

    reasons = result.get("reasons")
    if isinstance(reasons, list):
        for reason in reasons:
            if not isinstance(reason, Mapping):
                continue
            code = reason.get("code")
            if isinstance(code, str) and code in summary:
                summary[code] += 1

    events = payload.setdefault("events", [])
    if isinstance(events, list):
        reason_codes: list[str] = []
        if isinstance(reasons, list):
            for reason in reasons:
                if isinstance(reason, Mapping):
                    code = reason.get("code")
                    if isinstance(code, str) and code.strip():
                        reason_codes.append(code.strip())
        warnings = result.get("warnings")
        events.append(
            {
                "received_at": received_at,
                "message_id": message_id,
                "payload_kind": payload_kind,
                "status": status,
                "reason_codes": reason_codes,
                "detected_risks": list(detected_risks) if isinstance(detected_risks, list) else [],
                "validator_version": result.get("validator_version"),
                "raw_txt_path": raw_txt_path,
                "raw_meta_path": raw_meta_path,
                "warning_count": len(warnings) if isinstance(warnings, list) else 0,
            }
        )

    _write_daily_artifact("pre_ingestion_validation", report_date, payload, output_root=output_root)
    return str(_daily_artifact_path("pre_ingestion_validation", report_date, output_root=output_root))


def record_action_event(
    *,
    report_date: str,
    branch: str,
    signal_type: str,
    outcome: str,
    rule_code: str | None = None,
    priority: str | None = None,
    action_id: str | None = None,
    dedupe_key: str | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Append one autonomous-action observability event into the daily artifact."""

    payload = load_daily_artifact("autonomous_actions", report_date, output_root=output_root) or {
        "report_date": report_date,
        "summary": {
            "actions_generated": 0,
            "actions_skipped": 0,
            "actions_suppressed_replay": 0,
            "actions_by_rule": {},
            "actions_by_priority": {},
        },
        "events": [],
    }
    summary = payload.setdefault("summary", {})
    summary["actions_generated"] = int(summary.get("actions_generated", 0))
    summary["actions_skipped"] = int(summary.get("actions_skipped", 0))
    summary["actions_suppressed_replay"] = int(summary.get("actions_suppressed_replay", 0))
    actions_by_rule = summary.setdefault("actions_by_rule", {})
    actions_by_priority = summary.setdefault("actions_by_priority", {})

    if outcome == "generated":
        summary["actions_generated"] += 1
        if isinstance(rule_code, str) and rule_code.strip():
            actions_by_rule[rule_code.strip()] = int(actions_by_rule.get(rule_code.strip(), 0)) + 1
        if isinstance(priority, str) and priority.strip():
            actions_by_priority[priority.strip()] = int(actions_by_priority.get(priority.strip(), 0)) + 1
    elif outcome == "suppressed_replay":
        summary["actions_suppressed_replay"] += 1
    else:
        summary["actions_skipped"] += 1

    events = payload.setdefault("events", [])
    if isinstance(events, list):
        events.append(
            {
                "branch": branch,
                "signal_type": signal_type,
                "outcome": outcome,
                "rule_code": rule_code,
                "priority": priority,
                "action_id": action_id,
                "dedupe_key": dedupe_key,
            }
        )

    _write_daily_artifact("autonomous_actions", report_date, payload, output_root=output_root)
    return str(_daily_artifact_path("autonomous_actions", report_date, output_root=output_root))


def record_learning_event(
    *,
    report_date: str,
    branch: str,
    outcome: str,
    review_items_analyzed: int = 0,
    actions_analyzed: int = 0,
    threshold_recommendations_generated: int = 0,
    format_drift_patterns_detected: int = 0,
    output_root: str | Path | None = None,
) -> str:
    """Append one learning-layer observability event into the daily artifact."""

    payload = load_daily_artifact("learning", report_date, output_root=output_root) or {
        "report_date": report_date,
        "summary": {
            "learning_runs_completed": 0,
            "learning_runs_suppressed_replay": 0,
            "review_items_analyzed": 0,
            "actions_analyzed": 0,
            "threshold_recommendations_generated": 0,
            "format_drift_patterns_detected": 0,
        },
        "events": [],
    }
    summary = payload.setdefault("summary", {})
    summary["learning_runs_completed"] = int(summary.get("learning_runs_completed", 0))
    summary["learning_runs_suppressed_replay"] = int(summary.get("learning_runs_suppressed_replay", 0))
    summary["review_items_analyzed"] = int(summary.get("review_items_analyzed", 0))
    summary["actions_analyzed"] = int(summary.get("actions_analyzed", 0))
    summary["threshold_recommendations_generated"] = int(summary.get("threshold_recommendations_generated", 0))
    summary["format_drift_patterns_detected"] = int(summary.get("format_drift_patterns_detected", 0))

    if outcome == "completed":
        summary["learning_runs_completed"] += 1
    elif outcome == "suppressed_replay":
        summary["learning_runs_suppressed_replay"] += 1

    summary["review_items_analyzed"] += max(int(review_items_analyzed), 0)
    summary["actions_analyzed"] += max(int(actions_analyzed), 0)
    summary["threshold_recommendations_generated"] += max(int(threshold_recommendations_generated), 0)
    summary["format_drift_patterns_detected"] += max(int(format_drift_patterns_detected), 0)

    events = payload.setdefault("events", [])
    if isinstance(events, list):
        events.append(
            {
                "branch": branch,
                "outcome": outcome,
                "review_items_analyzed": max(int(review_items_analyzed), 0),
                "actions_analyzed": max(int(actions_analyzed), 0),
                "threshold_recommendations_generated": max(int(threshold_recommendations_generated), 0),
                "format_drift_patterns_detected": max(int(format_drift_patterns_detected), 0),
            }
        )

    _write_daily_artifact("learning", report_date, payload, output_root=output_root)

    summary_payload = _load_summary(report_date, output_root=output_root)
    learning_summary = summary_payload.setdefault("learning", {})
    learning_summary.update(
        {
            "learning_runs_completed": int(summary["learning_runs_completed"]),
            "learning_runs_suppressed_replay": int(summary["learning_runs_suppressed_replay"]),
            "review_items_analyzed": int(summary["review_items_analyzed"]),
            "actions_analyzed": int(summary["actions_analyzed"]),
            "threshold_recommendations_generated": int(summary["threshold_recommendations_generated"]),
            "format_drift_patterns_detected": int(summary["format_drift_patterns_detected"]),
        }
    )
    _write_summary(report_date, summary_payload, output_root=output_root)
    return str(_daily_artifact_path("learning", report_date, output_root=output_root))


def record_conversation_reply_event(
    *,
    report_date: str,
    branch: str | None,
    response_type: str,
    dispatch_status: str,
    source_message_id: str | None = None,
    channel: str = "whatsapp",
    governance_status: str | None = None,
    report_type: str | None = None,
    replay_suppressed: bool | None = None,
    reason: str | None = None,
    conversation_date: str | None = None,
    outcome: str | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Append one conversation-reply observability event into the daily artifact."""

    payload = load_daily_artifact("conversation_replies", report_date, output_root=output_root) or {
        "report_date": report_date,
        "summary": {
            "conversation_replies_generated": 0,
            "conversation_replies_sent": 0,
            "conversation_replies_failed": 0,
            "conversation_replies_suppressed": 0,
        },
        "events": [],
    }
    summary = payload.setdefault("summary", {})
    for field_name in (
        "conversation_replies_generated",
        "conversation_replies_sent",
        "conversation_replies_failed",
        "conversation_replies_suppressed",
    ):
        summary[field_name] = int(summary.get(field_name, 0))

    summary["conversation_replies_generated"] += 1
    normalized_dispatch_status = dispatch_status.strip() if isinstance(dispatch_status, str) else ""
    replay_flag = (
        bool(replay_suppressed)
        if replay_suppressed is not None
        else normalized_dispatch_status == "suppressed_replay"
    )
    suppressed = normalized_dispatch_status.startswith("suppressed") or replay_flag

    if normalized_dispatch_status == "sent":
        summary["conversation_replies_sent"] += 1
    elif normalized_dispatch_status == "failed":
        summary["conversation_replies_failed"] += 1
    elif suppressed:
        summary["conversation_replies_suppressed"] += 1

    events = payload.setdefault("events", [])
    if isinstance(events, list):
        events.append(
            {
                "branch": branch or "unknown",
                "response_type": response_type,
                "dispatch_status": normalized_dispatch_status,
                "outcome": outcome.strip() if isinstance(outcome, str) and outcome.strip() else _reply_outcome(normalized_dispatch_status),
                "source_message_id": source_message_id,
                "channel": channel.strip() if isinstance(channel, str) and channel.strip() else "whatsapp",
                "governance_status": governance_status.strip() if isinstance(governance_status, str) and governance_status.strip() else None,
                "report_type": report_type.strip() if isinstance(report_type, str) and report_type.strip() else None,
                "reason": reason.strip() if isinstance(reason, str) and reason.strip() else None,
                "date": conversation_date.strip() if isinstance(conversation_date, str) and conversation_date.strip() else None,
                "replay_suppressed": replay_flag,
            }
        )

    _write_daily_artifact("conversation_replies", report_date, payload, output_root=output_root)

    summary_payload = _load_summary(report_date, output_root=output_root)
    summary_payload["conversation_replies"] = {
        "conversation_replies_generated": int(summary["conversation_replies_generated"]),
        "conversation_replies_sent": int(summary["conversation_replies_sent"]),
        "conversation_replies_failed": int(summary["conversation_replies_failed"]),
        "conversation_replies_suppressed": int(summary["conversation_replies_suppressed"]),
    }
    _write_summary(report_date, summary_payload, output_root=output_root)
    return str(_daily_artifact_path("conversation_replies", report_date, output_root=output_root))


def _reply_outcome(dispatch_status: str) -> str:
    if dispatch_status == "failed":
        return "failed"
    if dispatch_status.startswith("suppressed"):
        return "fallback"
    return "success"


def record_conversation_llm_event(
    *,
    report_date: str,
    outcome: str,
    response_type: str | None = None,
    channel: str = "whatsapp",
    branch: str | None = None,
    report_type: str | None = None,
    event_report_date: str | None = None,
    replay_suppressed: bool = False,
    reason: str | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Append one conversation-LLM observability event into the daily artifact."""

    payload = load_daily_artifact("conversation_llm", report_date, output_root=output_root) or {
        "report_date": report_date,
        "summary": {
            "conversation_llm_calls": 0,
            "conversation_llm_failures": 0,
            "conversation_llm_fallbacks": 0,
            "conversation_llm_skipped": 0,
        },
        "events": [],
    }
    summary = payload.setdefault("summary", {})
    for field_name in (
        "conversation_llm_calls",
        "conversation_llm_failures",
        "conversation_llm_fallbacks",
        "conversation_llm_skipped",
    ):
        summary[field_name] = int(summary.get(field_name, 0))

    normalized_outcome = outcome.strip() if isinstance(outcome, str) else ""
    if normalized_outcome == "called":
        summary["conversation_llm_calls"] += 1
    elif normalized_outcome == "failed":
        summary["conversation_llm_failures"] += 1
    elif normalized_outcome == "fallback":
        summary["conversation_llm_fallbacks"] += 1
    elif normalized_outcome == "skipped":
        summary["conversation_llm_skipped"] += 1

    events = payload.setdefault("events", [])
    if isinstance(events, list):
        events.append(
            {
                "outcome": normalized_outcome,
                "response_type": response_type.strip() if isinstance(response_type, str) and response_type.strip() else None,
                "channel": channel.strip() if isinstance(channel, str) and channel.strip() else "whatsapp",
                "branch": branch.strip() if isinstance(branch, str) and branch.strip() else None,
                "report_type": report_type.strip() if isinstance(report_type, str) and report_type.strip() else None,
                "report_date": event_report_date.strip()
                if isinstance(event_report_date, str) and event_report_date.strip()
                else None,
                "replay_suppressed": bool(replay_suppressed),
                "reason": reason.strip() if isinstance(reason, str) and reason.strip() else None,
            }
        )

    _write_daily_artifact("conversation_llm", report_date, payload, output_root=output_root)

    summary_payload = _load_summary(report_date, output_root=output_root)
    summary_payload["conversation_llm"] = {
        "conversation_llm_calls": int(summary["conversation_llm_calls"]),
        "conversation_llm_failures": int(summary["conversation_llm_failures"]),
        "conversation_llm_fallbacks": int(summary["conversation_llm_fallbacks"]),
        "conversation_llm_skipped": int(summary["conversation_llm_skipped"]),
    }
    _write_summary(report_date, summary_payload, output_root=output_root)
    return str(_daily_artifact_path("conversation_llm", report_date, output_root=output_root))


def record_nl_intent_event(
    *,
    report_date: str,
    outcome: str,
    message_id: str | None = None,
    sender_phone: str | None = None,
    normalized_message: str | None = None,
    intent: str | None = None,
    confidence: float | None = None,
    reason: str | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Append one NL-intent observability event into the daily artifact."""

    payload = load_daily_artifact("nl_intent", report_date, output_root=output_root) or {
        "report_date": report_date,
        "summary": {
            "nl_intent_calls": 0,
            "nl_intent_success": 0,
            "nl_intent_rejected": 0,
            "nl_intent_low_confidence": 0,
            "nl_intent_skipped": 0,
            "nl_intent_mismatch": 0,
        },
        "events": [],
    }
    summary = payload.setdefault("summary", {})
    for field_name in (
        "nl_intent_calls",
        "nl_intent_success",
        "nl_intent_rejected",
        "nl_intent_low_confidence",
        "nl_intent_skipped",
        "nl_intent_mismatch",
    ):
        summary[field_name] = int(summary.get(field_name, 0))

    normalized_outcome = outcome.strip() if isinstance(outcome, str) else ""
    if normalized_outcome != "skipped":
        summary["nl_intent_calls"] += 1
    if normalized_outcome == "success":
        summary["nl_intent_success"] += 1
    elif normalized_outcome == "rejected":
        summary["nl_intent_rejected"] += 1
    elif normalized_outcome == "low_confidence":
        summary["nl_intent_low_confidence"] += 1
    elif normalized_outcome == "skipped":
        summary["nl_intent_skipped"] += 1
    elif normalized_outcome == "mismatch":
        summary["nl_intent_mismatch"] += 1

    events = payload.setdefault("events", [])
    if isinstance(events, list):
        events.append(
            {
                "outcome": normalized_outcome,
                "message_id": message_id.strip() if isinstance(message_id, str) and message_id.strip() else None,
                "sender_phone": sender_phone.strip() if isinstance(sender_phone, str) and sender_phone.strip() else None,
                "normalized_message": normalized_message.strip()
                if isinstance(normalized_message, str) and normalized_message.strip()
                else None,
                "intent": intent.strip() if isinstance(intent, str) and intent.strip() else None,
                "confidence": _bounded_confidence(confidence),
                "reason": reason.strip() if isinstance(reason, str) and reason.strip() else None,
            }
        )

    _write_daily_artifact("nl_intent", report_date, payload, output_root=output_root)

    summary_payload = _load_summary(report_date, output_root=output_root)
    summary_payload["nl_intent"] = {
        "nl_intent_calls": int(summary["nl_intent_calls"]),
        "nl_intent_success": int(summary["nl_intent_success"]),
        "nl_intent_rejected": int(summary["nl_intent_rejected"]),
        "nl_intent_low_confidence": int(summary["nl_intent_low_confidence"]),
        "nl_intent_skipped": int(summary["nl_intent_skipped"]),
        "nl_intent_mismatch": int(summary["nl_intent_mismatch"]),
    }
    _write_summary(report_date, summary_payload, output_root=output_root)
    return str(_daily_artifact_path("nl_intent", report_date, output_root=output_root))


def refresh_feedback_summary(
    *,
    report_date: str,
    branch: str | None = None,
    now_utc: str | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Recompute and persist the daily action and feedback summary artifact."""

    payload = build_action_feedback_state(
        report_date,
        branch=branch,
        output_root=output_root,
        now_utc=now_utc,
    )
    _write_daily_artifact("feedback_summary", report_date, payload, output_root=output_root)
    summary = _load_summary(report_date, output_root=output_root)
    summary["actions"] = {
        "actions_acknowledged": int(payload["summary"].get("actions_acknowledged", 0)),
        "actions_in_progress": int(payload["summary"].get("actions_in_progress", 0)),
        "actions_resolved": int(payload["summary"].get("actions_resolved", 0)),
        "actions_dismissed": int(payload["summary"].get("actions_dismissed", 0)),
        "review_linked_actions": int(payload["summary"].get("review_linked_actions", 0)),
        "stale_pending_actions": int(payload["summary"].get("stale_pending_actions", 0)),
    }
    _write_summary(report_date, summary, output_root=output_root)
    return payload


def load_daily_artifact(
    artifact_name: str,
    report_date: str,
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Load one daily observability artifact when present and valid."""

    path = _daily_artifact_path(artifact_name, report_date, output_root=output_root)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _load_summary(report_date: str, *, output_root: str | Path | None = None) -> dict[str, Any]:
    """Load or initialize the daily observability summary."""

    path = _summary_path(report_date, output_root=output_root)
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict):
            return payload
    return {
        "report_date": report_date,
        "summary": {
            "intake_volume": 0,
            "accept_count": 0,
            "review_count": 0,
            "reject_count": 0,
            "fallback_activation_count": 0,
            "fallback_activation_rate": 0.0,
        },
        "agents": {},
        "branches": {},
        "exports": {
            "success_count": 0,
            "failure_count": 0,
            "by_branch": {},
            "last_manifest_summary": None,
            "last_error": None,
        },
        "actions": {
            "actions_acknowledged": 0,
            "actions_in_progress": 0,
            "actions_resolved": 0,
            "actions_dismissed": 0,
            "review_linked_actions": 0,
            "stale_pending_actions": 0,
        },
        "learning": {
            "learning_runs_completed": 0,
            "learning_runs_suppressed_replay": 0,
            "review_items_analyzed": 0,
            "actions_analyzed": 0,
            "threshold_recommendations_generated": 0,
            "format_drift_patterns_detected": 0,
        },
        "conversation_replies": {
            "conversation_replies_generated": 0,
            "conversation_replies_sent": 0,
            "conversation_replies_failed": 0,
            "conversation_replies_suppressed": 0,
        },
        "conversation_llm": {
            "conversation_llm_calls": 0,
            "conversation_llm_failures": 0,
            "conversation_llm_fallbacks": 0,
            "conversation_llm_skipped": 0,
        },
        "nl_intent": {
            "nl_intent_calls": 0,
            "nl_intent_success": 0,
            "nl_intent_rejected": 0,
            "nl_intent_low_confidence": 0,
            "nl_intent_skipped": 0,
            "nl_intent_mismatch": 0,
        },
    }


def _write_summary(report_date: str, payload: dict[str, Any], *, output_root: str | Path | None = None) -> str:
    """Write the daily summary artifact."""

    path = _summary_path(report_date, output_root=output_root)
    write_json_file(path, payload)
    return str(path)


def _write_daily_artifact(
    artifact_name: str,
    report_date: str,
    payload: dict[str, Any],
    *,
    output_root: str | Path | None = None,
) -> str:
    path = _daily_artifact_path(artifact_name, report_date, output_root=output_root)
    write_json_file(path, payload)
    return str(path)


def _summary_path(report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the daily observability summary path for a root."""

    if output_root is None:
        return record_paths.get_observability_summary_path(report_date)
    return Path(output_root) / "records" / "observability" / "daily" / safe_segment(report_date) / "summary.json"


def _daily_artifact_path(
    artifact_name: str,
    report_date: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    if artifact_name == "summary":
        return _summary_path(report_date, output_root=output_root)
    if output_root is None:
        return record_paths.OBSERVABILITY_DIR / "daily" / safe_segment(report_date) / f"{safe_segment(artifact_name)}.json"
    return (
        Path(output_root)
        / "records"
        / "observability"
        / "daily"
        / safe_segment(report_date)
        / f"{safe_segment(artifact_name)}.json"
    )


def _ratio(left: int, right: int) -> float:
    """Return a rounded ratio."""

    if right <= 0:
        return 0.0
    return round(left / right, 4)


def _branch_quality_score(branch_metrics: dict[str, Any]) -> float:
    """Return a simple branch-wise data quality score."""

    processed = int(branch_metrics["processed_count"])
    if processed <= 0:
        return 1.0
    penalties = (
        branch_metrics["review_count"] * 0.2
        + branch_metrics["reject_count"] * 0.4
        + branch_metrics["warning_record_count"] * 0.1
    )
    return round(max(0.0, 1.0 - (penalties / processed)), 4)


def _normalized_processing_outcome(outcome: str) -> str:
    """Collapse final governance statuses into observability outcome buckets."""

    if outcome in {"accepted", "accepted_with_warning", "accepted_split", "ready"}:
        return "accepted"
    if outcome in {"review", "needs_review"}:
        return "review"
    if outcome in {"rejected", "invalid_input", "duplicate", "conflict_blocked"}:
        return "rejected"
    return outcome


def _bounded_confidence(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        return None
    if confidence < 0.0:
        return 0.0
    if confidence > 1.0:
        return 1.0
    return round(confidence, 4)


def _record_latency_artifact(
    *,
    report_date: str,
    event_type: str,
    branch: str,
    report_type: str | None,
    duration_ms: int | None = None,
    started_at_utc: str | None = None,
    finished_at_utc: str | None = None,
    output_root: str | Path | None = None,
) -> None:
    payload = load_daily_artifact("pipeline_latency", report_date, output_root=output_root) or {}
    updated = append_latency_event(
        payload,
        report_date=report_date,
        event_type=event_type,
        branch=branch,
        report_type=report_type,
        duration_ms=duration_ms,
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
    )
    _write_daily_artifact("pipeline_latency", report_date, updated, output_root=output_root)


def _observability_branch(branch: str | None) -> str:
    """Return the canonical observability branch key or ``unknown``."""

    return canonical_branch_slug_or_none(branch) or "unknown"
