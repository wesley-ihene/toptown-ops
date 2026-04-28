"""Generate duplicate analytics from duplicate archive records."""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from packages.common.branch import canonical_branch_slug
from packages.common.paths import REPO_ROOT
from packages.record_store.naming import safe_segment

TOP_SENDERS_LIMIT = 5


def generate_duplicate_analytics(
    report_date: str,
    *,
    root: str | Path | None = None,
    branches: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Generate duplicate analytics for one archive date."""

    source_root = Path(root) if root is not None else REPO_ROOT
    duplicate_records = _load_duplicate_records_for_date(source_root=source_root, report_date=report_date)
    branch_scope = _branch_scope(duplicate_records, branches=branches)

    branch_paths: dict[str, str] = {}
    trend_paths: dict[str, str] = {}
    for branch in branch_scope:
        payload = build_branch_daily_duplicate_analytics(
            branch,
            report_date,
            duplicate_records=duplicate_records,
        )
        output_path = _branch_daily_path(source_root, branch, report_date)
        _write_json_file(output_path, payload)
        branch_paths[branch] = str(output_path)

        trend_payload = build_duplicate_trend(branch, root=source_root)
        trend_path = _trend_path(source_root, branch)
        _write_json_file(trend_path, trend_payload)
        trend_paths[branch] = str(trend_path)

    global_payload = build_global_daily_duplicate_analytics(
        report_date,
        duplicate_records=duplicate_records,
    )
    global_path = _global_daily_path(source_root, report_date)
    _write_json_file(global_path, global_payload)

    return {
        "status": "generated",
        "date": report_date,
        "branch_paths": branch_paths,
        "global_path": str(global_path),
        "trend_paths": trend_paths,
    }


def build_branch_daily_duplicate_analytics(
    branch: str,
    report_date: str,
    *,
    duplicate_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build one branch-scoped daily duplicate analytics payload."""

    branch_slug = _normalize_branch(branch)
    branch_duplicates = [record for record in duplicate_records if record.get("branch") == branch_slug]
    return {
        "branch": branch_slug,
        "date": report_date,
        "duplicates": len(branch_duplicates),
        "by_reason": _sorted_counter_dict(Counter(record["duplicate_reason"] for record in branch_duplicates)),
        "by_report_type": _sorted_counter_dict(Counter(record["report_type"] for record in branch_duplicates)),
        "top_senders": _top_senders(branch_duplicates),
        "generated_at": _utc_timestamp(),
    }


def build_global_daily_duplicate_analytics(
    report_date: str,
    *,
    duplicate_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build one global daily duplicate analytics payload."""

    by_branch = Counter(record["branch"] for record in duplicate_records)
    by_reason = Counter(record["duplicate_reason"] for record in duplicate_records)
    return {
        "date": report_date,
        "total_duplicates": len(duplicate_records),
        "branches": _sorted_counter_dict(by_branch),
        "dominant_reason": _top_counter_key(by_reason),
        "worst_branch": _top_counter_key(by_branch),
        "generated_at": _utc_timestamp(),
    }


def build_duplicate_trend(
    branch: str,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Build one duplicate trend payload from branch-daily analytics history."""

    source_root = Path(root) if root is not None else REPO_ROOT
    branch_slug = _normalize_branch(branch)
    branch_dir = source_root / "analytics" / "duplicates" / "branch_daily" / branch_slug
    trend: list[dict[str, Any]] = []
    if branch_dir.exists():
        for path in sorted(branch_dir.glob("*.json")):
            payload = _read_json_file(path)
            if not payload:
                continue
            day = _string_or_none(payload.get("date")) or path.stem
            trend.append(
                {
                    "date": day,
                    "duplicates": _int_or_zero(payload.get("duplicates")),
                }
            )

    return {
        "branch": branch_slug,
        "trend": trend,
        "generated_at": _utc_timestamp(),
    }


def main(argv: list[str] | None = None) -> int:
    """Run duplicate analytics generation from the command line."""

    parser = argparse.ArgumentParser(description="Generate duplicate analytics for one archive date.")
    parser.add_argument("--date", required=True, help="Archive date in YYYY-MM-DD format.")
    parser.add_argument("--root", help="Repository root override.")
    parser.add_argument(
        "--branch",
        action="append",
        dest="branches",
        help="Optional branch slug or label to limit branch-daily and trend refresh.",
    )
    args = parser.parse_args(argv)

    result = generate_duplicate_analytics(
        args.date,
        root=args.root,
        branches=args.branches,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


def _branch_scope(
    duplicate_records: Sequence[Mapping[str, Any]],
    *,
    branches: Sequence[str] | None,
) -> list[str]:
    requested = {_normalize_branch(branch) for branch in branches or [] if _string_or_none(branch) is not None}
    discovered = {record["branch"] for record in duplicate_records}
    return sorted(requested or discovered)


def _top_senders(duplicate_records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return the highest-volume duplicate senders for one scope."""

    counts = Counter(record["sender_phone"] for record in duplicate_records)
    return [
        {"sender_phone": sender_phone, "duplicates": count}
        for sender_phone, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:TOP_SENDERS_LIMIT]
    ]


def _load_duplicate_records_for_date(*, source_root: Path, report_date: str) -> list[dict[str, Any]]:
    """Load one day's duplicate archive records."""

    archive_dir = source_root / "records" / "duplicates" / "whatsapp" / report_date
    if not archive_dir.exists():
        return []

    records: list[dict[str, Any]] = []
    for path in sorted(archive_dir.glob("*.json")):
        payload = _read_json_file(path)
        if not payload:
            continue
        records.append(
            {
                "archive_path": str(path),
                "branch": _normalize_branch(payload.get("branch")),
                "report_type": _normalize_key(payload.get("report_type")),
                "duplicate_reason": _normalize_key(payload.get("duplicate_reason")),
                "sender_phone": _normalize_sender(payload.get("sender_phone")),
            }
        )
    return records


def _branch_daily_path(root: Path, branch: str, report_date: str) -> Path:
    return root / "analytics" / "duplicates" / "branch_daily" / _normalize_branch(branch) / f"{report_date}.json"


def _global_daily_path(root: Path, report_date: str) -> Path:
    return root / "analytics" / "duplicates" / "global_daily" / f"{report_date}.json"


def _trend_path(root: Path, branch: str) -> Path:
    return root / "analytics" / "duplicates" / "trends" / f"{_normalize_branch(branch)}.json"


def _normalize_branch(value: object) -> str:
    cleaned = _string_or_none(value)
    if cleaned is None:
        return "unknown"
    return safe_segment(canonical_branch_slug(cleaned))


def _normalize_key(value: object) -> str:
    cleaned = _string_or_none(value)
    if cleaned is None:
        return "unknown"
    return safe_segment(cleaned)


def _normalize_sender(value: object) -> str:
    cleaned = _string_or_none(value)
    return cleaned or "unknown"


def _top_counter_key(counter: Counter[str]) -> str | None:
    if not counter:
        return None
    return min(counter, key=lambda item: (-counter[item], item))


def _sorted_counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_file(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    content = json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True)
    temporary_path.write_text(f"{content}\n", encoding="utf-8")
    os.replace(temporary_path, path)
    return path


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _int_or_zero(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
