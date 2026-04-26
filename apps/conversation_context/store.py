"""Persist the last WhatsApp conversation interaction per sender."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.record_store.naming import safe_segment
from packages.record_store.writer import write_json_file

_CHANNEL = "whatsapp"
_VERSION = "v1"


def load_sender_context(
    sender_phone: str | None,
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Return the stored last interaction for one sender when present."""

    normalized_sender = _text_or_none(sender_phone)
    if normalized_sender is None:
        return None
    path = _context_path(normalized_sender, output_root=output_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def store_sender_interaction(
    *,
    sender_phone: str | None,
    response_context: Mapping[str, Any],
    output_root: str | Path | None = None,
    stored_at: str | None = None,
) -> dict[str, Any] | None:
    """Persist one sender's latest interaction using an atomic overwrite."""

    normalized_sender = _text_or_none(sender_phone) or _text_or_none(response_context.get("sender_phone"))
    if normalized_sender is None:
        return None

    payload = {
        "version": _VERSION,
        "channel": _CHANNEL,
        "sender_phone": normalized_sender,
        "stored_at": _normalized_timestamp(stored_at),
        "last_interaction": {
            "source_message_id": _text_or_none(response_context.get("source_message_id")),
            "response_type": _text_or_none(response_context.get("response_type")),
            "governance_status": _text_or_none(response_context.get("governance_status")),
            "report_type": _text_or_none(response_context.get("report_type")),
            "branch": _text_or_none(response_context.get("branch")),
            "report_date": _text_or_none(response_context.get("report_date")),
            "reason": _text_or_none(response_context.get("reason")),
            "review_queue_path": _text_or_none(response_context.get("review_queue_path")),
            "structured_output_path": _text_or_none(response_context.get("structured_output_path")),
            "should_reply": bool(response_context.get("should_reply") is True),
            "is_replay": bool(response_context.get("is_replay") is True),
        },
    }
    write_json_file(_context_path(normalized_sender, output_root=output_root), payload)
    return payload


def _context_root(*, output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.RECORDS_DIR / "context" / _CHANNEL
    return Path(output_root) / "records" / "context" / _CHANNEL


def _context_path(sender_phone: str, *, output_root: str | Path | None) -> Path:
    return _context_root(output_root=output_root) / f"{safe_segment(sender_phone)}.json"


def _normalized_timestamp(value: str | None) -> str:
    cleaned = _text_or_none(value)
    if cleaned is not None:
        return cleaned
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
