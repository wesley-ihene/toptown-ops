"""Persist lightweight daily observability metrics and summary artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

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

    payload = load_daily_artifact("replay_audit", report_date, output_root=output_root) or {}
    updated = append_replay_event(
        payload,
        report_date=report_date,
        mode=mode,
        source=source,
        branch=branch,
        validation_mode=validation_mode,
        result=result,
    )
    _write_daily_artifact("replay_audit", report_date, updated, output_root=output_root)
    _record_latency_artifact(
        report_date=report_date,
        event_type="replay",
        branch=branch or "unknown",
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
