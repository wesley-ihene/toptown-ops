"""Replay audit aggregation helpers."""

from __future__ import annotations

from typing import Any

from .types import utc_now_iso

VALIDATION_RESULT_STATUSES = (
    "stable",
    "drift_detected",
    "missing_expected",
    "unexpected_acceptance",
    "unexpected_rejection",
    "error",
)


def append_replay_event(
    payload: dict[str, Any] | None,
    *,
    report_date: str,
    mode: str,
    source: str,
    branch: str | None,
    validation_mode: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    """Append one replay result entry and refresh summary counts."""

    document = payload if isinstance(payload, dict) else _empty_payload(report_date)
    document.setdefault("results", []).append(
        {
            "recorded_at_utc": utc_now_iso(),
            "mode": mode,
            "source": source,
            "branch": branch,
            "validation_mode": validation_mode,
            "status": result.get("status"),
            "agent": result.get("agent"),
            "reason": result.get("reason"),
            "file": result.get("file"),
            "output_path": result.get("output_path"),
            "validation": result.get("validation"),
        }
    )
    statuses = {"structured_written": 0, "rejected": 0, "skipped": 0, "failed": 0}
    for entry in document["results"]:
        status = str(entry.get("status") or "")
        if status in statuses:
            statuses[status] += 1
    document["generated_at_utc"] = utc_now_iso()
    document["summary"] = {
        "total": len(document["results"]),
        **statuses,
    }
    return document


def _empty_payload(report_date: str) -> dict[str, Any]:
    return {
        "report_date": report_date,
        "generated_at_utc": utc_now_iso(),
        "results": [],
        "summary": {"total": 0, "structured_written": 0, "rejected": 0, "skipped": 0, "failed": 0},
    }


def build_validation_audit(
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    source: str,
    filters: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return one machine-readable replay validation audit document."""

    summary = {"total": len(results)}
    for status in VALIDATION_RESULT_STATUSES:
        summary[status] = 0
    for entry in results:
        status = str(entry.get("status") or "")
        if status in summary:
            summary[status] += 1

    return {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "generated_at_utc": utc_now_iso(),
        "mode": "validation",
        "source": source,
        "filters": dict(filters),
        "results": list(results),
        "summary": summary,
    }
