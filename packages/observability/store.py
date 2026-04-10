"""Persist lightweight daily observability metrics and summary artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.record_store.naming import safe_segment
from packages.record_store.writer import write_json_file


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
    output_root: str | Path | None = None,
) -> str:
    """Update daily observability metrics for one processing outcome."""

    summary = _load_summary(report_date, output_root=output_root)
    summary["summary"]["intake_volume"] += 1
    if outcome == "accepted":
        summary["summary"]["accept_count"] += 1
    elif outcome == "review":
        summary["summary"]["review_count"] += 1
    elif outcome == "rejected":
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
    if outcome == "rejected":
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
    if outcome == "accepted":
        branch_metrics["accept_count"] += 1
    elif outcome == "review":
        branch_metrics["review_count"] += 1
    elif outcome == "rejected":
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
    return _write_summary(report_date, summary, output_root=output_root)


def record_export_event(
    *,
    report_date: str,
    branch: str,
    success: bool,
    manifest_summary: dict[str, Any] | None = None,
    error: str | None = None,
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
    return _write_summary(report_date, summary, output_root=output_root)


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
    }


def _write_summary(report_date: str, payload: dict[str, Any], *, output_root: str | Path | None = None) -> str:
    """Write the daily summary artifact."""

    path = _summary_path(report_date, output_root=output_root)
    write_json_file(path, payload)
    return str(path)


def _summary_path(report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the daily observability summary path for a root."""

    if output_root is None:
        return record_paths.get_observability_summary_path(report_date)
    return Path(output_root) / "records" / "observability" / "daily" / safe_segment(report_date) / "summary.json"


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
