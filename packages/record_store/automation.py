"""Post-write automation for analytics rebuilds and governance-gated Colony export."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
from time import perf_counter
from typing import Any

from analytics.phase3 import (
    build_branch_comparison,
    build_branch_daily_analytics,
    build_section_productivity,
    build_staff_leaderboard,
    write_branch_comparison_json,
    write_branch_daily_analytics_json,
    write_section_productivity_json,
    write_staff_leaderboard_json,
)
from apps.autonomous_control_engine import generate_control_actions
from apps.conversation_context import load_sender_context, store_sender_interaction
from apps.conversation_router import route_conversation_response
from apps.action_effectiveness_engine import analyze_action_effectiveness
from apps.format_drift_analyzer import analyze_format_drift
from apps.optimization_engine.worker import generate_optimization_proposals
from apps.review_learning_engine import analyze_review_queue
from apps.response_engine import render_whatsapp_response
from apps.threshold_recommendation_engine import generate_threshold_recommendations
from packages.action_store import write_action_record
from packages.common.executive_alerts import write_executive_alert_artifacts
from packages.common.paths import REPO_ROOT
from packages.observability import (
    record_action_event,
    record_conversation_reply_event,
    record_export_event,
    refresh_feedback_summary,
)
from packages.review_queue import write_action_follow_up_item
from packages.record_store.paths import get_structured_path_for_root
from packages.response_store import (
    load_response_artifact,
    update_response_artifact_dispatch,
    write_response_artifacts,
)
from packages.data_governance import read_governance_sidecar
from scripts.export_colony_signals import export_all_record_types

IOI_COLONY_ROOT_ENV_VAR = "TOPTOWN_IOI_COLONY_ROOT"
REPLAY_AUTOMATION_CONTEXT_ENV_VAR = "TOPTOWN_REPLAY_MODE"
ENABLE_REPLAY_ACTIONS_ENV_VAR = "TOPTOWN_ENABLE_REPLAY_ACTIONS"
ENABLE_REPLAY_LEARNING_ENV_VAR = "TOPTOWN_ENABLE_REPLAY_LEARNING"
ENABLE_REPLAY_RESPONSES_ENV_VAR = "TOPTOWN_ENABLE_REPLAY_RESPONSES"
WHATSAPP_RESPONSE_MODE_ENV_VAR = "TOPTOWN_WHATSAPP_RESPONSE_MODE"
WHATSAPP_OUTBOUND_MODE_ENV_VAR = "WHATSAPP_OUTBOUND_MODE"
DUPLICATE_KEEP_DAYS = 7
LOGGER = logging.getLogger(__name__)


def run_post_write_automation(
    signal_type: str,
    branch: str,
    report_date: str,
    *,
    source_root: str | Path | None = None,
    colony_root: str | Path | None = None,
) -> dict[str, Any]:
    """Rebuild affected analytics and export only governance-approved downstream signals."""

    affected_record_types = [signal_type]
    started_at = perf_counter()

    source_repo_root = Path(source_root) if source_root is not None else REPO_ROOT

    _log_event(
        "info",
        "analytics_rebuild_started",
        branch=branch,
        report_date=report_date,
        affected_record_types=affected_record_types,
        status="started",
    )
    branch_daily_payload = build_branch_daily_analytics(branch, report_date, root=source_repo_root)
    branch_daily_path = write_branch_daily_analytics_json(
        branch_daily_payload,
        output_root=source_repo_root,
        overwrite=True,
    )

    staff_payload = build_staff_leaderboard(branch, report_date, root=source_repo_root)
    staff_path = write_staff_leaderboard_json(
        staff_payload,
        output_root=source_repo_root,
        overwrite=True,
    )

    section_payload = build_section_productivity(branch, report_date, root=source_repo_root)
    section_path = write_section_productivity_json(
        section_payload,
        output_root=source_repo_root,
        overwrite=True,
    )
    _log_event(
        "info",
        "analytics_rebuild_completed",
        branch=branch,
        report_date=report_date,
        affected_record_types=affected_record_types,
        output_paths=[
            str(branch_daily_path),
            str(staff_path),
            str(section_path),
        ],
        duration_ms=_duration_ms(started_at),
        status="completed",
    )

    comparison_payload = build_branch_comparison(report_date, root=source_repo_root)
    comparison_path = write_branch_comparison_json(
        comparison_payload,
        output_root=source_repo_root,
        overwrite=True,
    )
    _log_event(
        "info",
        "branch_comparison_rebuild_completed",
        branch=branch,
        report_date=report_date,
        affected_record_types=affected_record_types,
        output_paths=[str(comparison_path)],
        duration_ms=_duration_ms(started_at),
        status="completed",
    )

    action_outputs: list[str] = []
    try:
        control_result = _run_autonomous_actions(
            signal_type=signal_type,
            branch=branch,
            report_date=report_date,
            source_root=source_repo_root,
            analytics_context={
                "branch_daily_path": str(branch_daily_path),
                "branch_comparison_path": str(comparison_path),
                "staff_daily_path": str(staff_path),
                "section_daily_path": str(section_path),
            },
        )
        action_outputs = control_result["output_paths"]
        if control_result["status"] == "generated":
            _log_event(
                "info",
                "autonomous_actions_completed",
                branch=branch,
                report_date=report_date,
                affected_record_types=affected_record_types,
                output_paths=action_outputs,
                action_count=len(action_outputs),
                status="completed",
            )
        elif control_result["status"] == "suppressed_replay":
            _log_event(
                "info",
                "autonomous_actions_suppressed",
                branch=branch,
                report_date=report_date,
                affected_record_types=affected_record_types,
                status="suppressed",
                reason=control_result["reason"],
            )
        else:
            _log_event(
                "info",
                "autonomous_actions_skipped",
                branch=branch,
                report_date=report_date,
                affected_record_types=affected_record_types,
                status="skipped",
                reason=control_result["reason"],
            )
    except Exception as exc:
        _log_event(
            "exception",
            "autonomous_actions_failed",
            branch=branch,
            report_date=report_date,
            affected_record_types=affected_record_types,
            status="failed",
            error=str(exc),
        )

    export_manifest: dict[str, Any] | None = None
    try:
        resolved_colony_root = resolve_colony_root(source_root=source_repo_root, colony_root=colony_root)
    except FileNotFoundError:
        resolved_colony_root = None
        _log_event(
            "info",
            "colony_export_skipped",
            branch=branch,
            report_date=report_date,
            affected_record_types=affected_record_types,
            status="skipped",
            reason="colony_root_unconfigured",
        )
    else:
        _log_event(
            "info",
            "colony_export_started",
            branch=branch,
            report_date=report_date,
            affected_record_types=affected_record_types,
            status="started",
        )
        export_manifest = export_all_record_types(
            branch,
            report_date,
            source_root=source_repo_root,
            colony_root=resolved_colony_root,
            overwrite=True,
        )
        export_paths = [
            str(Path(resolved_colony_root) / result["output_path"])
            for result in export_manifest["results"]
            if isinstance(result.get("output_path"), str)
        ]
        _log_event(
            "info",
            "colony_export_completed",
            branch=branch,
            report_date=report_date,
            affected_record_types=affected_record_types,
            output_paths=export_paths,
            duration_ms=_duration_ms(started_at),
            status="completed",
        )
        record_export_event(
            report_date=report_date,
            branch=branch,
            success=True,
            manifest_summary=export_manifest.get("summary") if isinstance(export_manifest, dict) else None,
            output_root=source_repo_root,
        )

    _log_event(
        "info",
        "executive_alerts_started",
        branch=branch,
        report_date=report_date,
        affected_record_types=affected_record_types,
        status="started",
    )
    alert_artifacts = write_executive_alert_artifacts(
        report_date,
        output_root=source_repo_root,
        overwrite=True,
    )
    alert_output_paths = [alert_artifacts["summary_path"], alert_artifacts["summary_whatsapp_path"]]
    alert_output_paths.extend(alert_artifacts["branch_paths"].values())
    alert_output_paths.extend(alert_artifacts["branch_whatsapp_paths"].values())
    _log_event(
        "info",
        "executive_alerts_completed",
        branch=branch,
        report_date=report_date,
        affected_record_types=affected_record_types,
        output_paths=alert_output_paths,
        duration_ms=_duration_ms(started_at),
        status="completed",
    )

    learning_outputs = _run_learning_automation(
        signal_type=signal_type,
        branch=branch,
        report_date=report_date,
        source_root=source_repo_root,
    )

    return {
        "branch_daily_path": str(branch_daily_path),
        "staff_daily_path": str(staff_path),
        "section_daily_path": str(section_path),
        "branch_comparison_path": str(comparison_path),
        "action_paths": action_outputs,
        "executive_alert_summary_path": alert_artifacts["summary_path"],
        "executive_alert_summary_whatsapp_path": alert_artifacts["summary_whatsapp_path"],
        "executive_alert_branch_paths": alert_artifacts["branch_paths"],
        "executive_alert_branch_whatsapp_paths": alert_artifacts["branch_whatsapp_paths"],
        "learning_outputs": learning_outputs,
        "export_manifest": export_manifest,
    }


def resolve_colony_root(
    *,
    source_root: str | Path | None = None,
    colony_root: str | Path | None = None,
) -> Path:
    """Resolve the downstream IOI Colony root without silent fallbacks."""

    if colony_root is not None:
        return Path(colony_root)

    configured = os.environ.get(IOI_COLONY_ROOT_ENV_VAR)
    if configured:
        return Path(configured)

    source_repo_root = Path(source_root) if source_root is not None else REPO_ROOT
    sibling_colony_root = source_repo_root.parent / "ioi-colony"
    if sibling_colony_root.is_dir():
        return sibling_colony_root

    raise FileNotFoundError(
        "Could not resolve IOI Colony root for automated signal export. "
        f"Set {IOI_COLONY_ROOT_ENV_VAR} or create a sibling `ioi-colony` repo."
    )


def log_post_write_failure(
    *,
    signal_type: str,
    branch: str,
    report_date: str,
    structured_path: Path,
    error: Exception,
) -> None:
    """Emit one explicit failure log for downstream automation errors."""

    record_export_event(
        report_date=report_date,
        branch=branch,
        success=False,
        error=str(error),
        output_root=structured_path.parents[4],
    )
    _log_event(
        "exception",
        "downstream_automation_failure",
        branch=branch,
        report_date=report_date,
        affected_record_types=[signal_type],
        output_paths=[str(structured_path)],
        status="failed",
        error=str(error),
    )


def run_post_duplicate_archive_automation(
    *,
    archive_path: str | Path,
    source_root: str | Path | None = None,
    keep_days: int = DUPLICATE_KEEP_DAYS,
    now: str | datetime | None = None,
) -> dict[str, Any]:
    """Generate duplicate analytics after one duplicate archive write."""

    source_repo_root = Path(source_root) if source_root is not None else REPO_ROOT
    archive_file = Path(archive_path)
    archive_payload = _read_json_or_empty(archive_file)
    archive_date = _duplicate_archive_date(archive_file, archive_payload)
    branch = _string_or_none(archive_payload.get("branch"))

    analytics_result: dict[str, Any]
    try:
        from scripts.analytics.duplicate_analytics import generate_duplicate_analytics

        analytics_result = generate_duplicate_analytics(
            archive_date,
            root=source_repo_root,
            branches=[branch] if branch is not None else None,
        )
        _log_event(
            "info",
            "duplicate_analytics_generated",
            status=analytics_result.get("status", "completed"),
            report_date=archive_date,
            branch=branch,
            source_archive_path=str(archive_file),
            output_paths=_duplicate_analytics_output_paths(analytics_result),
        )
    except Exception as exc:
        analytics_result = {
            "status": "failed",
            "error": str(exc),
            "report_date": archive_date,
            "branch": branch,
        }
        _log_event(
            "exception",
            "duplicate_analytics_generated",
            status="failed",
            report_date=archive_date,
            branch=branch,
            source_archive_path=str(archive_file),
            error=str(exc),
        )

    disposal_result = {
        "status": "not_invoked",
        "reason": "explicit_apply_required",
        "keep_days": keep_days,
        "requested_at": _utc_timestamp(),
    }
    _log_event(
        "info",
        "duplicate_disposal_skipped",
        status="not_invoked",
        report_date=archive_date,
        branch=branch,
        keep_days=keep_days,
        source_archive_path=str(archive_file),
        reason="explicit_apply_required",
    )

    return {
        "archive_path": str(archive_file),
        "report_date": archive_date,
        "branch": branch,
        "analytics": analytics_result,
        "disposal": disposal_result,
    }


def generate_whatsapp_conversation_reply(
    *,
    outcome: Mapping[str, Any] | Any,
    source_message_id: str | None,
    sender_phone: str | None,
    replay: bool,
    source_root: str | Path | None = None,
    mode: str | None = None,
    dispatcher: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any] | None:
    """Generate, persist, and optionally dispatch one Phase C1 WhatsApp reply."""

    stored_context = None
    if not replay:
        stored_context = load_sender_context(sender_phone, output_root=source_root)
    response_context = route_conversation_response(
        outcome,
        source_message_id=source_message_id,
        sender_phone=sender_phone,
        conversation_context=stored_context,
    )
    response_context = _with_mixed_children_feedback(response_context=response_context, outcome=outcome)
    if not replay:
        store_sender_interaction(
            sender_phone=sender_phone,
            response_context=response_context,
            output_root=source_root,
        )
    return dispatch_whatsapp_response(
        response_context=response_context,
        replay=replay,
        source_root=source_root,
        mode=mode,
        dispatcher=dispatcher,
    )


def _with_mixed_children_feedback(
    *,
    response_context: Mapping[str, Any],
    outcome: Mapping[str, Any] | Any,
) -> dict[str, Any]:
    """Attach mixed child summaries to feedback context when the outcome carries them."""

    payload = outcome.payload if hasattr(outcome, "payload") and isinstance(getattr(outcome, "payload"), Mapping) else outcome
    if not isinstance(payload, Mapping):
        return dict(response_context)

    fanout = payload.get("fanout")
    if not isinstance(fanout, Mapping):
        return dict(response_context)

    children = fanout.get("children")
    if not isinstance(children, list):
        return dict(response_context)

    mixed_children = [dict(child) for child in children if isinstance(child, Mapping)]
    if not mixed_children:
        return dict(response_context)

    updated_response_context = dict(response_context)
    feedback_context = updated_response_context.get("feedback_context")
    normalized_feedback_context = dict(feedback_context) if isinstance(feedback_context, Mapping) else {}
    normalized_feedback_context["mixed_children"] = mixed_children
    updated_response_context["feedback_context"] = normalized_feedback_context
    return updated_response_context


def dispatch_whatsapp_response(
    *,
    response_context: Mapping[str, Any],
    replay: bool | None = None,
    source_root: str | Path | None = None,
    mode: str | None = None,
    dispatcher: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any] | None:
    """Render, persist, and optionally dispatch one normalized WhatsApp response."""

    source_repo_root = Path(source_root) if source_root is not None else REPO_ROOT
    rendered = render_whatsapp_response(response_context)
    response_type = _string_or_none(response_context.get("response_type"))
    if response_context.get("should_reply") is not True or rendered.get("should_send") is not True or response_type is None:
        return {
            "response_id": None,
            "response_type": response_type,
            "dispatch_status": "skipped",
            "json_path": None,
            "text_path": None,
        }

    generated_at = _utc_timestamp()
    dispatch_payload = {
        **dict(response_context),
        **rendered,
        "generated_at": generated_at,
    }
    dispatch_payload["response_id"] = _response_id(dispatch_payload)
    resolved_mode = _conversation_response_mode(mode)
    is_replay = bool(replay) if replay is not None else bool(response_context.get("is_replay") is True)
    existing_artifact = _existing_response_artifact(dispatch_payload, output_root=source_repo_root)
    if existing_artifact is not None:
        existing_status = _artifact_dispatch_status(existing_artifact)
        if existing_status in {"sent", "duplicate"}:
            return {
                "response_id": existing_artifact["response_id"],
                "response_type": response_type,
                "dispatch_status": "duplicate",
                "json_path": existing_artifact["json_path"],
                "text_path": existing_artifact["text_path"],
            }
        if resolved_mode not in {"live", "dry_run"}:
            return {
                "response_id": existing_artifact["response_id"],
                "response_type": response_type,
                "dispatch_status": "skipped",
                "json_path": existing_artifact["json_path"],
                "text_path": existing_artifact["text_path"],
            }

    source_message_id = _string_or_none(response_context.get("source_message_id"))
    sender_phone = _string_or_none(response_context.get("sender_phone"))
    artifact = write_response_artifacts(
        {
            **dispatch_payload,
            "dispatch_status": "generated",
            "provider_message_id": None,
            "dispatch_error": None,
            "http_status": None,
        },
        output_root=source_repo_root,
        overwrite=existing_artifact is not None,
    )
    if response_type == "duplicate_notice":
        _log_event(
            "info",
            "duplicate_notice_generated",
            response_id=artifact["response_id"],
            source_message_id=source_message_id,
            sender_phone=sender_phone,
            governance_status=_string_or_none(response_context.get("governance_status")),
            report_type=_string_or_none(response_context.get("report_type")),
            reason=_string_or_none(response_context.get("reason")),
        )

    dispatch_status = "generated"
    dispatch_error = None
    provider_message_id = None
    http_status = None
    artifact_updated = False

    if is_replay and not _replay_responses_enabled():
        dispatch_status = "suppressed"
    elif resolved_mode in {"disabled", "off"}:
        dispatch_status = "suppressed"
    elif resolved_mode == "write_only":
        dispatch_status = "generated"
    elif resolved_mode == "dry_run":
        if dispatcher is None:
            dispatch_status = "dry_run"
        else:
            try:
                dispatch_result = dispatcher(dispatch_payload)
            except Exception as exc:
                dispatch_status = "failed"
                dispatch_error = str(exc)
                _log_event(
                    "exception",
                    "conversation_reply_dispatch_failed",
                    source_message_id=source_message_id,
                    sender_phone=sender_phone,
                    response_type=response_type,
                    error=str(exc),
                )
            else:
                dispatch_status = _normalized_dispatch_status(dispatch_result)
                provider_message_id, dispatch_error, http_status = _dispatch_result_fields(dispatch_result)
                artifact_updated = _dispatch_result_artifact_updated(dispatch_result)
    elif resolved_mode == "live" and dispatcher is not None:
        try:
            dispatch_result = dispatcher(dispatch_payload)
        except Exception as exc:
            dispatch_status = "failed"
            dispatch_error = str(exc)
            _log_event(
                "exception",
                "conversation_reply_dispatch_failed",
                source_message_id=source_message_id,
                sender_phone=sender_phone,
                response_type=response_type,
                error=str(exc),
            )
        else:
            dispatch_status = _normalized_dispatch_status(dispatch_result)
            provider_message_id, dispatch_error, http_status = _dispatch_result_fields(dispatch_result)
            artifact_updated = _dispatch_result_artifact_updated(dispatch_result)
    elif resolved_mode == "live":
        dispatch_status = "generated"

    if artifact_updated:
        refreshed_artifact = load_response_artifact(artifact["response_id"], output_root=source_repo_root)
        if refreshed_artifact is not None:
            artifact = refreshed_artifact
    elif dispatch_status != "generated" or provider_message_id is not None or dispatch_error is not None or http_status is not None:
        artifact = update_response_artifact_dispatch(
            artifact["response_id"],
            dispatch_status=dispatch_status,
            provider_message_id=provider_message_id,
            dispatch_error=dispatch_error,
            http_status=http_status,
            output_root=source_repo_root,
        )

    record_conversation_reply_event(
        report_date=generated_at[:10],
        branch=_string_or_none(response_context.get("branch")),
        response_type=response_type,
        dispatch_status=dispatch_status,
        source_message_id=source_message_id,
        governance_status=_string_or_none(response_context.get("governance_status")),
        report_type=_string_or_none(response_context.get("report_type")),
        replay_suppressed=is_replay and dispatch_status == "suppressed",
        reason=_string_or_none(response_context.get("reason")),
        conversation_date=_string_or_none(response_context.get("report_date")),
        outcome=_string_or_none(response_context.get("observability_outcome")),
        output_root=source_repo_root,
    )
    return {
        "response_id": artifact["response_id"],
        "response_type": response_type,
        "dispatch_status": dispatch_status,
        "json_path": artifact["json_path"],
        "text_path": artifact["text_path"],
    }


def _log_event(level: str, event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    message = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    if level == "exception":
        LOGGER.exception(message)
        return
    getattr(LOGGER, level)(message)


def _duration_ms(started_at: float) -> int:
    return int((perf_counter() - started_at) * 1000)


def _duplicate_analytics_output_paths(result: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []
    global_path = _string_or_none(result.get("global_path"))
    if global_path is not None:
        paths.append(global_path)
    branch_paths = result.get("branch_paths")
    if isinstance(branch_paths, Mapping):
        for path in branch_paths.values():
            cleaned = _string_or_none(path)
            if cleaned is not None:
                paths.append(cleaned)
    trend_paths = result.get("trend_paths")
    if isinstance(trend_paths, Mapping):
        for path in trend_paths.values():
            cleaned = _string_or_none(path)
            if cleaned is not None:
                paths.append(cleaned)
    return paths


def _duplicate_archive_date(path: Path, payload: Mapping[str, Any]) -> str:
    created_at = _string_or_none(payload.get("created_at"))
    if created_at is not None and len(created_at) >= 10:
        candidate = created_at[:10]
        if (
            candidate[4] == "-"
            and candidate[7] == "-"
            and candidate[:4].isdigit()
            and candidate[5:7].isdigit()
            and candidate[8:10].isdigit()
        ):
            return candidate
    parent_name = path.parent.name
    if (
        len(parent_name) == 10
        and parent_name[4] == "-"
        and parent_name[7] == "-"
        and parent_name[:4].isdigit()
        and parent_name[5:7].isdigit()
        and parent_name[8:10].isdigit()
    ):
        return parent_name
    return _utc_timestamp()[:10]


def _run_autonomous_actions(
    *,
    signal_type: str,
    branch: str,
    report_date: str,
    source_root: Path,
    analytics_context: dict[str, Any],
) -> dict[str, Any]:
    """Generate and persist conservative autonomous actions without blocking export."""

    structured_path = get_structured_path_for_root(
        source_root / "records" / "structured",
        signal_type=signal_type,
        branch=branch,
        date=report_date,
    )
    if not structured_path.exists():
        record_action_event(
            report_date=report_date,
            branch=branch,
            signal_type=signal_type,
            outcome="skipped",
            output_root=source_root,
        )
        refresh_feedback_summary(report_date=report_date, branch=branch, output_root=source_root)
        return {"status": "skipped", "reason": "structured_record_missing", "output_paths": []}

    structured_payload = json.loads(structured_path.read_text(encoding="utf-8"))
    governance_sidecar = read_governance_sidecar(structured_path)
    replay = _is_replay_context(structured_payload)
    control_result = generate_control_actions(
        structured_payload=structured_payload,
        governance_sidecar=governance_sidecar,
        analytics_context=analytics_context,
        replay=replay,
        allow_replay=_replay_actions_enabled(),
        source_paths=[
            str(structured_path),
            str(structured_path.with_suffix(".governance.json")),
        ],
    )

    if control_result["status"] == "suppressed_replay":
        record_action_event(
            report_date=report_date,
            branch=branch,
            signal_type=signal_type,
            outcome="suppressed_replay",
            output_root=source_root,
        )
        refresh_feedback_summary(report_date=report_date, branch=branch, output_root=source_root)
        return {"status": "suppressed_replay", "reason": control_result["reason"], "output_paths": []}

    actions = control_result.get("actions")
    if not isinstance(actions, list) or not actions:
        record_action_event(
            report_date=report_date,
            branch=branch,
            signal_type=signal_type,
            outcome="skipped",
            output_root=source_root,
        )
        refresh_feedback_summary(report_date=report_date, branch=branch, output_root=source_root)
        return {"status": "skipped", "reason": control_result["reason"], "output_paths": []}

    output_paths: list[str] = []
    review_queue_paths: list[str] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        write_result = write_action_record(action, output_root=source_root)
        output_paths.extend([write_result["action_path"], write_result["preview_path"]])
        if action.get("requires_ack") is True:
            review_queue_paths.append(
                write_action_follow_up_item(
                    action,
                    source_action_path=write_result["action_path"],
                    output_root=source_root,
                )
            )
        record_action_event(
            report_date=report_date,
            branch=branch,
            signal_type=signal_type,
            outcome="generated",
            rule_code=action.get("rule_code"),
            priority=action.get("priority"),
            action_id=action.get("action_id"),
            dedupe_key=action.get("dedupe_key"),
            output_root=source_root,
        )
    refresh_feedback_summary(report_date=report_date, branch=branch, output_root=source_root)
    return {
        "status": "generated",
        "reason": control_result["reason"],
        "output_paths": output_paths,
        "review_queue_paths": review_queue_paths,
    }


def _run_learning_automation(
    *,
    signal_type: str,
    branch: str,
    report_date: str,
    source_root: Path,
) -> dict[str, Any]:
    """Run best-effort learning summaries without blocking the main pipeline."""

    learning_context = _learning_context(
        signal_type=signal_type,
        report_date=report_date,
        branch=branch,
        source_root=source_root,
    )
    if learning_context["replay_suppressed"]:
        _log_event(
            "info",
            "learning_automation_suppressed",
            branch=branch,
            report_date=report_date,
            status="suppressed",
            reason="replay_suppressed",
        )
        return {
            "status": "suppressed_replay",
            "output_paths": {},
            "failures": [],
        }

    _log_event(
        "info",
        "learning_automation_started",
        branch=branch,
        report_date=report_date,
        status="started",
    )
    outputs: dict[str, str] = {}
    failures: list[dict[str, str]] = []
    for engine_name, runner in (
        ("review_summary", analyze_review_queue),
        ("action_effectiveness", analyze_action_effectiveness),
        ("threshold_recommendations", generate_threshold_recommendations),
        ("format_drift", analyze_format_drift),
        ("optimization_proposals", generate_optimization_proposals),
    ):
        try:
            result = runner(
                report_date,
                output_root=source_root,
            )
        except Exception as exc:
            failures.append({"engine": engine_name, "error": str(exc)})
            _log_event(
                "exception",
                "learning_automation_engine_failed",
                branch=branch,
                report_date=report_date,
                status="failed",
                engine=engine_name,
                error=str(exc),
            )
            continue
        output_path = result.get("output_path")
        if isinstance(output_path, str) and output_path.strip():
            outputs[engine_name] = output_path

    _log_event(
        "info",
        "learning_automation_completed",
        branch=branch,
        report_date=report_date,
        status="completed" if not failures else "completed_with_failures",
        output_paths=[outputs[key] for key in sorted(outputs)],
        failure_count=len(failures),
    )
    return {
        "status": "completed" if not failures else "completed_with_failures",
        "output_paths": outputs,
        "failures": failures,
    }


def _learning_context(
    *,
    signal_type: str,
    report_date: str,
    branch: str,
    source_root: Path,
) -> dict[str, Any]:
    """Return whether learning should run for this automation pass."""

    structured_path = get_structured_path_for_root(
        source_root / "records" / "structured",
        signal_type=signal_type,
        branch=branch,
        date=report_date,
    )
    structured_payload = _read_json_or_empty(structured_path)
    replay = _is_replay_context(structured_payload)
    return {
        "replay_suppressed": replay and not _replay_learning_enabled(),
    }


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_replay_context(structured_payload: dict[str, Any]) -> bool:
    """Return whether autonomous actions should treat this automation run as replay."""

    if os.environ.get(REPLAY_AUTOMATION_CONTEXT_ENV_VAR) == "1":
        return True
    return structured_payload.get("source") == "replay"


def _replay_actions_enabled() -> bool:
    """Return whether replay runs may emit Phase 5B actions."""

    value = os.environ.get(ENABLE_REPLAY_ACTIONS_ENV_VAR, "")
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def _replay_learning_enabled() -> bool:
    """Return whether replay runs may emit Phase 6 learning artifacts."""

    value = os.environ.get(ENABLE_REPLAY_LEARNING_ENV_VAR, "")
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def _replay_responses_enabled() -> bool:
    """Return whether replay runs may emit outbound conversational replies."""

    value = os.environ.get(ENABLE_REPLAY_RESPONSES_ENV_VAR, "")
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def _conversation_response_mode(explicit_mode: str | None = None) -> str:
    """Return the active conversation response dispatch mode."""

    candidate = explicit_mode if isinstance(explicit_mode, str) else os.environ.get(
        WHATSAPP_OUTBOUND_MODE_ENV_VAR,
        os.environ.get(WHATSAPP_RESPONSE_MODE_ENV_VAR, "write_only"),
    )
    normalized = candidate.strip().casefold()
    if normalized in {"write_only", "dry_run", "live", "disabled", "off"}:
        return normalized
    return "write_only"


def _normalized_dispatch_status(result: Any) -> str:
    """Return one stable dispatch status from a dispatcher result."""

    if isinstance(result, Mapping):
        status = result.get("dispatch_status")
        if isinstance(status, str) and status.strip():
            cleaned = status.strip()
            if cleaned in {"generated", "suppressed", "sent", "failed", "skipped", "dry_run", "duplicate"}:
                return cleaned
    if result is True:
        return "sent"
    if result is False:
        return "failed"
    return "sent"


def _utc_timestamp() -> str:
    """Return a stable UTC timestamp for response artifacts."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _existing_response_artifact(
    payload: Mapping[str, Any],
    *,
    output_root: Path,
) -> dict[str, Any] | None:
    """Return one existing response artifact for the same deterministic response id."""

    response_id = _response_id(payload)
    return load_response_artifact(response_id, output_root=output_root)


def _response_id(payload: Mapping[str, Any]) -> str:
    """Return the deterministic response identity used by the response store."""

    stable_fields = {
        "source_message_id": _string_or_none(payload.get("source_message_id")),
        "sender_phone": _string_or_none(payload.get("sender_phone")),
        "response_type": _string_or_none(payload.get("response_type")),
        "governance_status": _string_or_none(payload.get("governance_status")),
        "report_type": _string_or_none(payload.get("report_type")),
        "branch": _string_or_none(payload.get("branch")),
        "reason": _string_or_none(payload.get("reason")),
    }
    digest = hashlib.sha256(
        json.dumps(stable_fields, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    return digest[:24]


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _artifact_dispatch_status(artifact: Mapping[str, Any]) -> str | None:
    payload = artifact.get("payload")
    if not isinstance(payload, Mapping):
        return None
    return _string_or_none(payload.get("dispatch_status"))


def _dispatch_result_fields(result: Any) -> tuple[str | None, str | None, int | None]:
    if not isinstance(result, Mapping):
        return None, None, None
    return (
        _string_or_none(result.get("provider_message_id")),
        _string_or_none(result.get("dispatch_error") or result.get("error")),
        _int_or_none(result.get("http_status")),
    )


def _dispatch_result_artifact_updated(result: Any) -> bool:
    if not isinstance(result, Mapping):
        return False
    return result.get("artifact_updated") is True


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None
