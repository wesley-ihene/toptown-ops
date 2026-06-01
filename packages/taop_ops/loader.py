"""Read structured TAOP operations records without reparsing raw inputs."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import json
from json import JSONDecodeError
from pathlib import Path
import re
from typing import Any

from packages.common.branch import canonical_branch_slug_or_none
from packages.common.paths import REPO_ROOT
from packages.taop_ops.schemas import OpsWarning

_PRIMARY_RECORD_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.json$")


def load_sales_records(
    date: str | None = None,
    branch: str | None = None,
    *,
    root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Load structured sales records for the requested filters."""

    return _load_records("sales_income", date=date, branch=branch, root=root)


def load_bale_release_records(
    date: str | None = None,
    branch: str | None = None,
    *,
    root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Load structured bale-release records for the requested filters."""

    return _load_records("pricing_stock_release", date=date, branch=branch, root=root)


def load_attendance_records(
    date: str | None = None,
    branch: str | None = None,
    *,
    root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Load structured attendance records for the requested filters."""

    return _load_records("hr_attendance", date=date, branch=branch, root=root)


def load_daily_operations(
    date: str,
    branch: str | None = None,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Load one read-only daily operations bundle from structured records."""

    iso_date = _validate_iso_date(date)
    canonical_branch = _canonical_branch_or_none(branch)
    sales_records = load_sales_records(date=iso_date, branch=canonical_branch, root=root)
    bale_release_records = load_bale_release_records(date=iso_date, branch=canonical_branch, root=root)
    attendance_records = load_attendance_records(date=iso_date, branch=canonical_branch, root=root)
    warnings: list[dict[str, Any]] = []
    if not sales_records:
        warnings.append(
            _missing_records_warning(
                category="sales",
                date=iso_date,
                branch=canonical_branch,
            )
        )
    if not bale_release_records:
        warnings.append(
            _missing_records_warning(
                category="bale_release",
                date=iso_date,
                branch=canonical_branch,
            )
        )
    if not attendance_records:
        warnings.append(
            _missing_records_warning(
                category="attendance",
                date=iso_date,
                branch=canonical_branch,
            )
        )
    return {
        "date": iso_date,
        "branch": canonical_branch,
        "sales_records": sales_records,
        "bale_release_records": bale_release_records,
        "attendance_records": attendance_records,
        "warnings": warnings,
    }


def structured_records_root(*, root: str | Path | None = None) -> Path:
    """Return the structured-records root for the repo or an override."""

    base_root = Path(root) if root is not None else REPO_ROOT
    return base_root / "records" / "structured"


def _load_records(
    record_type: str,
    *,
    date: str | None = None,
    branch: str | None = None,
    root: str | Path | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in _iter_record_paths(record_type, date=date, branch=branch, root=root):
        payload = _read_record(path)
        if payload is not None:
            records.append(payload)
    return records


def _iter_record_paths(
    record_type: str,
    *,
    date: str | None = None,
    branch: str | None = None,
    root: str | Path | None = None,
) -> list[Path]:
    records_root = structured_records_root(root=root) / record_type
    if not records_root.exists():
        return []
    iso_date = _validate_iso_date(date) if date is not None else None
    canonical_branch = _canonical_branch_or_none(branch)
    if branch is not None and canonical_branch is None:
        return []
    if canonical_branch is not None and iso_date is not None:
        direct_path = records_root / canonical_branch / f"{iso_date}.json"
        return [direct_path] if _is_primary_record_path(direct_path) else []
    if canonical_branch is not None:
        branch_root = records_root / canonical_branch
        if not branch_root.exists():
            return []
        return sorted(
            (path for path in branch_root.glob("*.json") if _is_primary_record_path(path)),
            key=_path_sort_key,
        )
    if iso_date is not None:
        filename = f"{iso_date}.json"
        return sorted(
            (
                branch_dir / filename
                for branch_dir in records_root.iterdir()
                if branch_dir.is_dir() and _is_primary_record_path(branch_dir / filename)
            ),
            key=_path_sort_key,
        )
    return sorted(
        (path for path in records_root.glob("*/*.json") if _is_primary_record_path(path)),
        key=_path_sort_key,
    )


def _is_primary_record_path(path: Path) -> bool:
    return path.is_file() and bool(_PRIMARY_RECORD_RE.fullmatch(path.name))


def _read_record(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (JSONDecodeError, OSError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _path_sort_key(path: Path) -> tuple[str, str]:
    return path.parent.name, path.stem


def _canonical_branch_or_none(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return canonical_branch_slug_or_none(cleaned)


def _validate_iso_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")


def _missing_records_warning(*, category: str, date: str, branch: str | None) -> dict[str, Any]:
    if branch is None:
        message = f"No structured {category.replace('_', ' ')} records were found for {date}."
    else:
        message = f"No structured {category.replace('_', ' ')} records were found for {branch} on {date}."
    return OpsWarning(
        code=f"missing_{category}_records",
        message=message,
        branch=branch,
        date=date,
        category=category,
    ).to_payload()
