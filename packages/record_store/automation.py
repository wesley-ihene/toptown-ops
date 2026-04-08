"""Post-write automation for analytics rebuilds and Colony signal export."""

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
from packages.common.executive_alerts import write_executive_alert_artifacts
from packages.common.paths import REPO_ROOT
from scripts.export_colony_signals import export_all_record_types

IOI_COLONY_ROOT_ENV_VAR = "TOPTOWN_IOI_COLONY_ROOT"
LOGGER = logging.getLogger(__name__)


def run_post_write_automation(
    signal_type: str,
    branch: str,
    report_date: str,
    *,
    source_root: str | Path | None = None,
    colony_root: str | Path | None = None,
) -> dict[str, Any]:
    """Rebuild affected analytics and export fresh downstream signals."""

    affected_record_types = [signal_type]
    started_at = perf_counter()

    source_repo_root = Path(source_root) if source_root is not None else REPO_ROOT
    resolved_colony_root = resolve_colony_root(source_root=source_repo_root, colony_root=colony_root)

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
