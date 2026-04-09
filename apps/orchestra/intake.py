"""Minimal Orchestra intake helpers for raw inbound messages."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Final

from packages.common import paths
from packages.signal_contracts.work_item import WorkItem

INTAKE_STAGE: Final[str] = "intake"
RAW_WORK_ITEM_KIND: Final[str] = "raw_message"
RAW_INBOX_DIRNAME: Final[str] = "raw"

RawInboundMessage = str | Mapping[str, Any]


def utc_timestamp() -> str:
    """Return the current UTC timestamp in a stable ISO 8601 format."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_message_hash(raw_message: RawInboundMessage) -> str:
    """Return a stable SHA-256 hash for a raw inbound message."""

    canonical_message = _canonicalize_raw_message(raw_message)
    return hashlib.sha256(canonical_message.encode("utf-8")).hexdigest()


def ensure_inbox_directories() -> Path:
    """Ensure the raw inbox directory exists and return its path."""

    raw_inbox_dir = paths.INBOX_DIR / RAW_INBOX_DIRNAME
    raw_inbox_dir.mkdir(parents=True, exist_ok=True)
    return raw_inbox_dir


def create_raw_work_item(
    raw_message: RawInboundMessage,
    *,
    received_at_utc: str | None = None,
) -> WorkItem:
    """Build a minimal raw `WorkItem` from an inbound message."""

    timestamp = received_at_utc or utc_timestamp()
    normalized_message = _normalize_raw_message(raw_message)

    return WorkItem(
        kind=RAW_WORK_ITEM_KIND,
        payload={
            "received_at_utc": timestamp,
            "message_hash": stable_message_hash(normalized_message),
            "raw_message": normalized_message,
        },
    )


def persist_raw_work_item(work_item: WorkItem) -> Path:
    """Persist a raw work item as JSON inside `data/inbox/raw`."""

    raw_inbox_dir = ensure_inbox_directories()
    message_hash = str(work_item.payload["message_hash"])
    filename = f"{message_hash}.json"
    output_path = raw_inbox_dir / filename
    output_path.write_text(_serialize_work_item(work_item), encoding="utf-8")
    return output_path


def intake_raw_message(
    raw_message: RawInboundMessage,
    *,
    persist: bool = False,
    received_at_utc: str | None = None,
) -> WorkItem:
    """Create a raw work item and optionally persist it."""

    work_item = create_raw_work_item(
        raw_message,
        received_at_utc=received_at_utc,
    )
    if persist:
        persist_raw_work_item(work_item)
    return work_item


def create_rejection_feedback_work_item(
    source_work_item: WorkItem,
    *,
    report_type: str,
    rejections: list[dict[str, str]],
    dry_run: bool = True,
    channel: str = "whatsapp",
) -> WorkItem:
    """Build a feedback-agent work item from one rejected or split source item."""

    report_payload = _report_payload_from_work_item(source_work_item)
    return WorkItem(
        kind="rejection_feedback",
        payload={
            "report_type": report_type,
            "branch": _string_or_none(report_payload.get("branch")),
            "report_date": _string_or_none(report_payload.get("report_date")),
            "channel": channel,
            "dry_run": dry_run,
            "rejections": rejections,
            "source_record_path": _source_record_path_from_work_item(source_work_item),
            "source_message_hash": _string_or_none(source_work_item.payload.get("message_hash")),
        },
    )


def _normalize_raw_message(raw_message: RawInboundMessage) -> str | dict[str, Any]:
    """Normalize raw input into JSON-safe content for the work item payload."""

    if isinstance(raw_message, str):
        return raw_message
    return dict(raw_message)


def _canonicalize_raw_message(raw_message: RawInboundMessage) -> str:
    """Convert raw input into a stable string for hashing."""

    normalized_message = _normalize_raw_message(raw_message)
    if isinstance(normalized_message, str):
        return normalized_message
    return json.dumps(normalized_message, sort_keys=True, separators=(",", ":"))


def _serialize_work_item(work_item: WorkItem) -> str:
    """Serialize a work item into stable, human-readable JSON."""

    return json.dumps(
        {
            "kind": work_item.kind,
            "payload": work_item.payload,
        },
        indent=2,
        sort_keys=True,
    )


def _report_payload_from_work_item(work_item: WorkItem) -> Mapping[str, Any]:
    """Return the best available report-like payload from one work item."""

    normalized_report = work_item.payload.get("normalized_report")
    if isinstance(normalized_report, Mapping):
        return normalized_report

    raw_message = work_item.payload.get("raw_message")
    if isinstance(raw_message, Mapping):
        return raw_message
    return {}


def _source_record_path_from_work_item(work_item: WorkItem) -> str | None:
    """Return the best available source record path when present."""

    raw_record = work_item.payload.get("raw_record")
    if isinstance(raw_record, Mapping):
        for field_name in ("text_path", "raw_txt_path", "path"):
            value = raw_record.get(field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()

    replay = work_item.payload.get("replay")
    if isinstance(replay, Mapping):
        value = replay.get("original_path")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _string_or_none(value: Any) -> str | None:
    """Return a stripped string or None."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
