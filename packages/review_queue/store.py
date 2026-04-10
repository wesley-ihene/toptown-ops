"""Explicit file-backed review queue persistence."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from packages.provenance_store import write_provenance_record
from packages.record_store.naming import safe_segment
from packages.record_store.paths import get_review_path
from packages.record_store.writer import write_json_file
from packages.signal_contracts.work_item import WorkItem


def write_review_item(
    work_item: WorkItem,
    *,
    report_type: str,
    branch: str,
    report_date: str,
    confidence: float | None,
    warnings: list[dict[str, str]],
    reason: str,
    validation_outcome: dict[str, Any] | None = None,
    acceptance_outcome: dict[str, Any] | None = None,
    parser_used: str = "orchestra_router",
    parse_mode: str = "strict",
) -> str:
    """Persist one review item and return its path as a string."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    source_message_hash = _string_or_none(payload.get("message_hash")) or _utc_stamp()
    review_path = get_review_path(report_date, branch, report_type) / f"{safe_segment(source_message_hash)}.json"
    review_payload = {
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "confidence": confidence,
        "warnings": warnings,
        "provenance": _provenance_payload(payload),
        "reason": reason,
    }
    write_json_file(review_path, review_payload)
    write_provenance_record(
        outcome="review",
        report_type=report_type,
        branch=branch,
        report_date=report_date,
        raw_message_hash=source_message_hash,
        parser_used=parser_used,
        parse_mode=parse_mode,
        confidence=confidence,
        warnings=warnings,
        validation_outcome=validation_outcome or {"status": "not_run"},
        acceptance_outcome=acceptance_outcome or {"status": "review", "reason": reason},
        downstream_references={
            "review_queue_path": str(review_path),
            "source_record_path": _source_record_path(payload),
        },
        extra={"provenance": _provenance_payload(payload)},
    )
    return str(review_path)


def _provenance_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return stable provenance metadata for one review item."""

    return {
        "received_at_utc": _string_or_none(payload.get("received_at_utc")),
        "source_message_hash": _string_or_none(payload.get("message_hash")),
        "source_record_path": _source_record_path(payload),
        "normalization": dict(payload.get("validation", {}).get("normalization", {}))
        if isinstance(payload.get("validation"), Mapping)
        else {},
        "classification": dict(payload.get("classification", {}))
        if isinstance(payload.get("classification"), Mapping)
        else {},
    }


def _source_record_path(payload: dict[str, Any]) -> str | None:
    """Return the best available source record path for review provenance."""

    raw_record = payload.get("raw_record")
    if isinstance(raw_record, Mapping):
        for field_name in ("text_path", "raw_txt_path", "path"):
            value = raw_record.get(field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _string_or_none(value: object) -> str | None:
    """Return a stripped string or None."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _utc_stamp() -> str:
    """Return a stable UTC stamp for fallback filenames."""

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
