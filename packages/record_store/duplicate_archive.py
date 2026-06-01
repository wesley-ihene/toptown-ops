"""Archive duplicate records for disposal without touching raw or structured data."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any

from packages.branch_registry import canonical_branch_slug_or_none

from .naming import safe_segment
from .paths import get_duplicate_archive_dir

LOGGER = logging.getLogger(__name__)


def archive_duplicate_record(
    *,
    source_message_id: str | None,
    sender_phone: str | None,
    branch: str | None,
    report_type: str | None,
    report_date: str | None,
    raw_txt_path: str | None,
    raw_meta_path: str | None,
    duplicate_reason: str | None,
    duplicate_basis: str | None,
    original_or_duplicate_of: str | None = None,
    created_at: str | None = None,
    output_root: str | Path | None = None,
) -> Path:
    """Persist one duplicate archive record under the disposable duplicates area."""

    resolved_raw_meta_path = _string_or_none(raw_meta_path)
    raw_metadata = _load_json_file(Path(resolved_raw_meta_path)) if resolved_raw_meta_path is not None else {}
    resolved_created_at = _string_or_none(created_at) or _utc_timestamp()
    resolved_source_message_id = _first_text(source_message_id, raw_metadata.get("message_id"))
    resolved_sender_phone = _first_text(sender_phone, raw_metadata.get("sender_phone"), raw_metadata.get("sender"))
    resolved_branch = _first_text(branch, raw_metadata.get("branch"), raw_metadata.get("branch_hint"), raw_metadata.get("group_name"))
    resolved_report_type = _first_text(
        report_type,
        raw_metadata.get("report_type"),
        raw_metadata.get("specialist_report_type"),
        raw_metadata.get("detected_report_type"),
    )
    resolved_report_date = _first_text(report_date, raw_metadata.get("report_date"), raw_metadata.get("resolved_report_date"))
    resolved_raw_txt_path = _first_text(raw_txt_path, raw_metadata.get("raw_txt_path"))
    resolved_original = _first_text(original_or_duplicate_of, raw_metadata.get("duplicate_of"))
    resolved_reason = _string_or_none(duplicate_reason) or "duplicate"
    resolved_basis = _string_or_none(duplicate_basis) or _default_duplicate_basis(
        duplicate_reason=resolved_reason,
        original_or_duplicate_of=resolved_original,
    )
    canonical_branch = canonical_branch_slug_or_none(resolved_branch) or "unknown"

    payload = {
        "source_message_id": resolved_source_message_id,
        "sender_phone": resolved_sender_phone,
        "branch": canonical_branch,
        "report_type": resolved_report_type,
        "report_date": resolved_report_date,
        "raw_txt_path": resolved_raw_txt_path,
        "raw_meta_path": resolved_raw_meta_path,
        "duplicate_reason": resolved_reason,
        "duplicate_basis": resolved_basis,
        "original_or_duplicate_of": resolved_original,
        "disposal_status": "ready_for_disposal",
        "disposal_allowed": True,
        "created_at": resolved_created_at,
    }

    archive_path = _duplicate_archive_path(
        archive_date=_archive_date(resolved_created_at),
        report_type=resolved_report_type,
        payload=payload,
        output_root=output_root,
    )
    _write_json_file(archive_path, payload)
    _log_event(
        "info",
        "duplicate_archived_for_disposal",
        archive_path=str(archive_path),
        source_message_id=resolved_source_message_id,
        sender_phone=resolved_sender_phone,
        branch=resolved_branch,
        report_type=resolved_report_type,
        report_date=resolved_report_date,
        duplicate_reason=resolved_reason,
        duplicate_basis=resolved_basis,
        original_or_duplicate_of=resolved_original,
    )
    from .automation import run_post_duplicate_archive_automation

    run_post_duplicate_archive_automation(
        archive_path=archive_path,
        source_root=output_root,
    )
    return archive_path


def _duplicate_archive_path(
    *,
    archive_date: str,
    report_type: str | None,
    payload: Mapping[str, Any],
    output_root: str | Path | None,
) -> Path:
    """Return a unique duplicate-archive JSON path for one duplicate event."""

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    digest = hashlib.sha256(
        json.dumps(dict(payload), sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()[:12]
    filename = f"{stamp}__{safe_segment(report_type or 'unknown')}__{digest}.json"
    return get_duplicate_archive_dir(archive_date, output_root=output_root) / filename


def _archive_date(created_at: str) -> str:
    """Return the UTC archive-date segment for one timestamp string."""

    if len(created_at) >= 10:
        candidate = created_at[:10]
        if (
            candidate[4] == "-"
            and candidate[7] == "-"
            and candidate[:4].isdigit()
            and candidate[5:7].isdigit()
            and candidate[8:10].isdigit()
        ):
            return candidate
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _default_duplicate_basis(*, duplicate_reason: str, original_or_duplicate_of: str | None) -> str:
    """Return a conservative duplicate basis when the caller has no richer evidence."""

    if original_or_duplicate_of is not None:
        return f"reference:{original_or_duplicate_of}"
    return duplicate_reason


def _load_json_file(path: Path) -> dict[str, Any]:
    """Return one JSON object from disk when available and well formed."""

    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_file(path: Path, payload: Mapping[str, Any]) -> Path:
    """Write one archive record atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    content = json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True)
    temporary_path.write_text(f"{content}\n", encoding="utf-8")
    os.replace(temporary_path, path)
    return path


def _utc_timestamp() -> str:
    """Return the current UTC timestamp with second precision."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _string_or_none(value: object) -> str | None:
    """Return one stripped string or ``None``."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _first_text(*values: object) -> str | None:
    """Return the first usable string from the candidate values."""

    for value in values:
        cleaned = _string_or_none(value)
        if cleaned is not None:
            return cleaned
    return None


def _log_event(level: str, event: str, **fields: Any) -> None:
    """Emit one compact JSON log line for duplicate archive activity."""

    message = json.dumps({"event": event, **fields}, sort_keys=True, ensure_ascii=True)
    getattr(LOGGER, level)(message)
