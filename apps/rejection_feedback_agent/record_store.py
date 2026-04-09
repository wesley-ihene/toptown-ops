"""Rejected feedback artifact writers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.record_store.naming import safe_segment
from packages.record_store.writer import write_json_file, write_text_file

_FEEDBACK_BUCKET = "feedback"


def feedback_root(report_type: str) -> Path:
    """Return the canonical rejected-feedback base path for one report type."""

    return record_paths.REJECTED_DIR / _FEEDBACK_BUCKET / safe_segment(report_type)


def write_feedback_record(
    *,
    report_type: str,
    channel: str,
    feedback_message: str,
    payload: dict[str, Any],
    dry_run: bool,
) -> dict[str, str | None]:
    """Write one deterministic rejected-feedback audit record and optional preview."""

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = f"{stamp}__{safe_segment(report_type)}__{safe_segment(channel)}__rejection_feedback"
    root = feedback_root(report_type)
    json_path = root / f"{stem}.json"
    write_json_file(json_path, payload)

    whatsapp_preview_path: Path | None = None
    if channel == "whatsapp" and not dry_run:
        whatsapp_preview_path = root / f"{stem}.whatsapp.txt"
        write_text_file(whatsapp_preview_path, f"{feedback_message}\n")

    return {
        "json_path": str(json_path),
        "whatsapp_preview_path": str(whatsapp_preview_path) if whatsapp_preview_path is not None else None,
    }
