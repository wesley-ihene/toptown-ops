"""Helpers for daily pipeline latency aggregation."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .types import utc_now_iso


def append_latency_event(
    payload: dict[str, Any] | None,
    *,
    report_date: str,
    event_type: str,
    branch: str,
    report_type: str | None,
    duration_ms: int | None,
    started_at_utc: str | None = None,
    finished_at_utc: str | None = None,
) -> dict[str, Any]:
    """Append one latency event and refresh summary aggregates."""

    document = payload if isinstance(payload, dict) else _empty_payload(report_date)
    events = document.setdefault("events", [])
    normalized_duration = max(0, int(duration_ms or _duration_ms(started_at_utc, finished_at_utc) or 0))
    event = {
        "event_type": event_type,
        "branch": branch,
        "report_type": report_type,
        "duration_ms": normalized_duration,
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
        "recorded_at_utc": utc_now_iso(),
    }
    events.append(event)
    document["generated_at_utc"] = utc_now_iso()
    document["summary"] = _summarize(events)
    return document


def _empty_payload(report_date: str) -> dict[str, Any]:
    return {
        "report_date": report_date,
        "generated_at_utc": utc_now_iso(),
        "events": [],
        "summary": {},
    }


def _summarize(events: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, dict[str, Any]] = {}
    for event in events:
        event_type = str(event.get("event_type") or "unknown")
        bucket = by_type.setdefault(
            event_type,
            {"count": 0, "total_duration_ms": 0, "max_duration_ms": 0, "avg_duration_ms": 0.0},
        )
        duration_ms = int(event.get("duration_ms") or 0)
        bucket["count"] += 1
        bucket["total_duration_ms"] += duration_ms
        bucket["max_duration_ms"] = max(bucket["max_duration_ms"], duration_ms)
        bucket["avg_duration_ms"] = round(bucket["total_duration_ms"] / bucket["count"], 2)
    return {"event_types": by_type, "total_events": len(events)}


def _duration_ms(started_at_utc: str | None, finished_at_utc: str | None) -> int | None:
    if not started_at_utc or not finished_at_utc:
        return None
    try:
        started = _parse_iso8601(started_at_utc)
        finished = _parse_iso8601(finished_at_utc)
    except ValueError:
        return None
    return int(max(0.0, (finished - started).total_seconds()) * 1000)


def _parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
