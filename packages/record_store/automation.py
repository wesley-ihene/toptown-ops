"""Post-write automation for analytics rebuilds and governance-gated Colony export."""

from __future__ import annotations

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
from packages.action_store import write_action_record
from packages.common.executive_alerts import write_executive_alert_artifacts
from packages.common.paths import REPO_ROOT
from packages.observability import record_action_event, record_export_event, refresh_feedback_summary
from packages.review_queue import write_action_follow_up_item
from packages.record_store.paths import get_structured_path_for_root
from packages.data_governance import read_governance_sidecar
from scripts.export_colony_signals import export_all_record_types

IOI_COLONY_ROOT_ENV_VAR = "TOPTOWN_IOI_COLONY_ROOT"
REPLAY_AUTOMATION_CONTEXT_ENV_VAR = "TOPTOWN_REPLAY_MODE"
ENABLE_REPLAY_ACTIONS_ENV_VAR = "TOPTOWN_ENABLE_REPLAY_ACTIONS"
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


def _log_event(level: str, event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    message = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    if level == "exception":
        LOGGER.exception(message)
        return
    getattr(LOGGER, level)(message)


def _duration_ms(started_at: float) -> int:
    return int((perf_counter() - started_at) * 1000)


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


def _is_replay_context(structured_payload: dict[str, Any]) -> bool:
    """Return whether autonomous actions should treat this automation run as replay."""

    if os.environ.get(REPLAY_AUTOMATION_CONTEXT_ENV_VAR) == "1":
        return True
    return structured_payload.get("source") == "replay"


def _replay_actions_enabled() -> bool:
    """Return whether replay runs may emit Phase 5B actions."""

    value = os.environ.get(ENABLE_REPLAY_ACTIONS_ENV_VAR, "")
    return value.strip().casefold() in {"1", "true", "yes", "on"}
