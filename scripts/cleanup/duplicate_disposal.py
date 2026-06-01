"""Dispose eligible duplicate archive records under an explicit retention policy."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
from typing import Any

from packages.common.paths import REPO_ROOT
from packages.record_store.naming import safe_segment

DEFAULT_KEEP_DAYS = 7
LOGGER = logging.getLogger(__name__)


def dispose_duplicate_archives(
    *,
    date: str | None = None,
    root: str | Path | None = None,
    keep_days: int = DEFAULT_KEEP_DAYS,
    dry_run: bool = True,
    now: str | datetime | None = None,
) -> dict[str, Any]:
    """Dispose eligible duplicate archive records under the duplicates store only."""

    source_root = Path(root) if root is not None else REPO_ROOT
    duplicates_root = source_root / "records" / "duplicates" / "whatsapp"
    retention_window = timedelta(days=max(keep_days, 0))
    current_time = _coerce_datetime(now)

    candidate_paths: list[str] = []
    deleted_paths: list[str] = []
    skipped: list[dict[str, str]] = []
    evaluated_dates = [date] if date is not None else []

    for date_dir in _date_directories(duplicates_root, date=date):
        if date is None:
            evaluated_dates.append(date_dir.name)
        for path in sorted(date_dir.glob("*.json")):
            eligible, reason = _is_disposable(path, current_time=current_time, retention_window=retention_window)
            if not eligible:
                skipped_item = {"path": str(path), "reason": reason}
                skipped.append(skipped_item)
                _log_event(
                    "duplicate_disposal_skipped",
                    path=str(path),
                    date=date_dir.name,
                    keep_days=keep_days,
                    reason=reason,
                )
                continue

            candidate_paths.append(str(path))
            if dry_run:
                continue

            path.unlink(missing_ok=False)
            deleted_paths.append(str(path))
        if not dry_run:
            _prune_empty_directory(date_dir)

    result = {
        "status": "dry_run" if dry_run else "applied",
        "date": date,
        "keep_days": keep_days,
        "candidate_count": len(candidate_paths),
        "candidate_paths": candidate_paths,
        "deleted_count": len(deleted_paths),
        "deleted_paths": deleted_paths,
        "skipped_count": len(skipped),
        "skipped": skipped,
        "evaluated_dates": evaluated_dates,
        "generated_at": current_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }

    if dry_run:
        _log_event(
            "duplicate_disposal_dry_run",
            date=date,
            keep_days=keep_days,
            candidate_count=len(candidate_paths),
            candidate_paths=candidate_paths,
            skipped_count=len(skipped),
        )
    elif deleted_paths:
        _log_event(
            "duplicate_disposed",
            date=date,
            keep_days=keep_days,
            deleted_count=len(deleted_paths),
            deleted_paths=deleted_paths,
        )
    else:
        _log_event(
            "duplicate_disposal_skipped",
            date=date,
            keep_days=keep_days,
            reason="no_eligible_records",
        )
    return result


def main(argv: list[str] | None = None) -> int:
    """Run duplicate disposal from the command line."""

    parser = argparse.ArgumentParser(description="Dispose expired duplicate archive records.")
    parser.add_argument("--date", help="Optional archive date in YYYY-MM-DD format.")
    parser.add_argument("--root", help="Repository root override.")
    parser.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS, help="Retention period for duplicate archives.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="List eligible duplicate archive records without deleting.")
    mode_group.add_argument("--apply", action="store_true", help="Delete eligible duplicate archive records.")
    args = parser.parse_args(argv)

    result = dispose_duplicate_archives(
        date=args.date,
        root=args.root,
        keep_days=args.keep_days,
        dry_run=not args.apply,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


def _date_directories(duplicates_root: Path, *, date: str | None) -> list[Path]:
    if not duplicates_root.exists():
        return []
    if date is not None:
        candidates: list[Path] = []
        for segment in (date, safe_segment(date)):
            target = duplicates_root / segment
            if target.is_dir() and target not in candidates:
                candidates.append(target)
        return candidates
    return sorted(path for path in duplicates_root.iterdir() if path.is_dir())


def _is_disposable(
    path: Path,
    *,
    current_time: datetime,
    retention_window: timedelta,
) -> tuple[bool, str]:
    payload = _read_json_file(path)
    if not payload:
        return False, "invalid_json"
    if payload.get("disposal_allowed") is not True:
        return False, "disposal_not_allowed"
    if _string_or_none(payload.get("disposal_status")) != "ready_for_disposal":
        return False, "disposal_not_ready"

    created_at = _parse_created_at(payload.get("created_at"))
    if created_at is None:
        return False, "missing_created_at"

    if current_time - created_at <= retention_window:
        return False, "within_retention_window"
    return True, "eligible"


def _prune_empty_directory(path: Path) -> None:
    if not path.exists():
        return
    if any(path.iterdir()):
        return
    path.rmdir()


def _parse_created_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    parsed = _parse_created_at(value)
    if parsed is not None:
        return parsed
    return datetime.now(timezone.utc)


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _log_event(event: str, **fields: Any) -> None:
    message = json.dumps({"event": event, **fields}, sort_keys=True, ensure_ascii=True)
    LOGGER.info(message)


if __name__ == "__main__":
    raise SystemExit(main())
