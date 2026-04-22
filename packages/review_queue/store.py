"""Explicit file-backed review queue persistence."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
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
    governance_outcome: dict[str, Any] | None = None,
    candidate_payload: dict[str, Any] | None = None,
    raw_paths: dict[str, Any] | None = None,
    queue_type: str | None = None,
    linked_action_id: str | None = None,
    linked_action_path: str | None = None,
    ack_required: bool | None = None,
    resolution_status: str | None = None,
    parser_used: str = "orchestra_router",
    parse_mode: str = "strict",
    output_root: str | Path | None = None,
) -> str:
    """Persist one review item and return its path as a string."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    source_message_hash = _string_or_none(payload.get("message_hash")) or _utc_stamp()
    review_path = get_review_path(
        report_date,
        branch,
        report_type,
        output_root=output_root,
    ) / f"{safe_segment(source_message_hash)}.json"
    review_payload = {
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "queue_type": queue_type or "standard_review",
        "linked_action_id": _string_or_none(linked_action_id),
        "linked_action_path": _string_or_none(linked_action_path),
        "ack_required": bool(ack_required) if ack_required is not None else None,
        "resolution_status": _string_or_none(resolution_status) or "open",
        "confidence": confidence,
        "warnings": warnings,
        "provenance": _provenance_payload(payload),
        "reason": reason,
        "validation": validation_outcome or {"status": "not_run"},
        "acceptance": acceptance_outcome or {"status": "review", "reason": reason},
        "candidate_payload": dict(candidate_payload) if isinstance(candidate_payload, Mapping) else None,
        "raw_paths": dict(raw_paths) if isinstance(raw_paths, Mapping) else _raw_paths(payload),
        "governance": governance_outcome
        if isinstance(governance_outcome, Mapping)
        else {
            "status": "needs_review",
            "export_allowed": False,
            "reasons": [reason],
        },
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
            "linked_action_path": _string_or_none(linked_action_path),
        },
        extra={
            "provenance": _provenance_payload(payload),
            "governance_outcome": dict(governance_outcome) if isinstance(governance_outcome, Mapping) else None,
            "candidate_payload": dict(candidate_payload) if isinstance(candidate_payload, Mapping) else None,
            "raw_paths": dict(raw_paths) if isinstance(raw_paths, Mapping) else _raw_paths(payload),
            "queue_type": queue_type or "standard_review",
            "linked_action_id": _string_or_none(linked_action_id),
            "linked_action_path": _string_or_none(linked_action_path),
            "ack_required": bool(ack_required) if ack_required is not None else None,
            "resolution_status": _string_or_none(resolution_status) or "open",
        },
        received_at_utc=_string_or_none(payload.get("received_at_utc")),
        record_latency=False,
    )
    return str(review_path)


def write_action_follow_up_item(
    action_payload: Mapping[str, Any],
    *,
    source_action_path: str,
    linked_review_reason: str = "action_requires_operator_follow_up",
    output_root: str | Path | None = None,
) -> str:
    """Persist one review item linked to an autonomous action without side effects."""

    action_id = _required_text(action_payload.get("action_id"), field_name="action_id")
    branch = _required_text(action_payload.get("branch"), field_name="branch")
    report_date = _required_text(action_payload.get("report_date"), field_name="report_date")
    signal_type = _required_text(action_payload.get("signal_type"), field_name="signal_type")
    review_path = get_review_path(
        report_date,
        branch,
        signal_type,
        output_root=output_root,
    ) / f"{safe_segment(action_id)}.json"
    review_payload = {
        "report_type": signal_type,
        "branch": branch,
        "date": report_date,
        "queue_type": "operator_action_follow_up",
        "linked_action_id": action_id,
        "linked_action_path": source_action_path,
        "ack_required": action_payload.get("requires_ack") is True,
        "resolution_status": "open",
        "confidence": None,
        "warnings": [],
        "provenance": {
            "source_record_path": source_action_path,
            "action_rule_code": _string_or_none(action_payload.get("rule_code")),
            "action_priority": _string_or_none(action_payload.get("priority")),
        },
        "reason": linked_review_reason,
        "validation": {"status": "not_applicable"},
        "acceptance": {"status": "review", "reason": linked_review_reason},
        "candidate_payload": dict(action_payload),
        "raw_paths": {},
        "governance": {
            "status": "needs_review",
            "export_allowed": False,
            "reasons": [linked_review_reason],
        },
    }
    write_json_file(review_path, review_payload)
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


def _raw_paths(payload: dict[str, Any]) -> dict[str, Any]:
    """Return stable raw text/meta paths when present."""

    raw_record = payload.get("raw_record")
    if not isinstance(raw_record, Mapping):
        return {}
    output: dict[str, Any] = {}
    for field_name in ("raw_txt_path", "raw_meta_path", "raw_sha256"):
        value = raw_record.get(field_name)
        if isinstance(value, str) and value.strip():
            output[field_name] = value.strip()
    return output


def _string_or_none(value: object) -> str | None:
    """Return a stripped string or None."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _required_text(value: object, *, field_name: str) -> str:
    cleaned = _string_or_none(value)
    if cleaned is None:
        raise ValueError(f"review field `{field_name}` must be a non-empty string")
    return cleaned


def _utc_stamp() -> str:
    """Return a stable UTC stamp for fallback filenames."""

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
