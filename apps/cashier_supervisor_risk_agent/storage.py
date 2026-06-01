"""Storage helpers for cashier and supervisor risk intelligence."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from packages.common.branch import canonical_branch_slug_or_none
from packages.common.paths import REPO_ROOT
from packages.record_store.writer import write_json_file

_PRIMARY_RECORD_SUFFIX = ".json"
_IGNORED_RECORD_SUFFIXES = (".governance.json", ".validation.json")


def sales_records_root(output_root: str | Path | None = None) -> Path:
    """Return the structured sales record root."""

    base_root = Path(output_root) if output_root is not None else REPO_ROOT
    return base_root / "records" / "structured" / "sales_income"


def risk_root(output_root: str | Path | None = None) -> Path:
    """Return the canonical risk records root."""

    base_root = Path(output_root) if output_root is not None else REPO_ROOT
    return base_root / "records" / "risk"


def analytics_risk_root(output_root: str | Path | None = None) -> Path:
    """Return the analytics risk output root."""

    base_root = Path(output_root) if output_root is not None else REPO_ROOT
    return base_root / "analytics" / "risk"


def persistent_risk_path(output_root: str | Path | None = None) -> Path:
    """Return the aggregate risk datastore path."""

    return risk_root(output_root) / "cashier_supervisor_risk.json"


def daily_audit_path(report_date: str, output_root: str | Path | None = None) -> Path:
    """Return the daily risk audit path."""

    return risk_root(output_root) / "daily" / f"{report_date}.json"


def branch_audit_path(branch: str, output_root: str | Path | None = None) -> Path:
    """Return the branch risk audit path."""

    return risk_root(output_root) / "branch" / f"{branch}.json"


def dashboard_summary_path(output_root: str | Path | None = None) -> Path:
    """Return the dashboard-ready risk summary path."""

    return analytics_risk_root(output_root) / "cashier_supervisor_summary.json"


def load_sales_record_payloads(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    branch: str | None = None,
    output_root: str | Path | None = None,
) -> list[tuple[Path, dict[str, Any]]]:
    """Return structured sales payloads and paths for the requested filters."""

    records_root = sales_records_root(output_root)
    if not records_root.exists():
        return []

    selected_branch = canonical_branch_slug_or_none(branch) if branch else None
    branch_dirs = [records_root / selected_branch] if selected_branch else sorted(
        path for path in records_root.iterdir() if path.is_dir()
    )
    results: list[tuple[Path, dict[str, Any]]] = []
    for branch_dir in branch_dirs:
        if not branch_dir.exists():
            continue
        for path in sorted(branch_dir.glob(f"*{_PRIMARY_RECORD_SUFFIX}")):
            if _ignored_record_path(path):
                continue
            report_date = path.stem
            if start_date and report_date < start_date:
                continue
            if end_date and report_date > end_date:
                continue
            payload = _read_json_dict(path)
            if payload is None:
                continue
            results.append((path, payload))
    return sorted(results, key=lambda item: (item[1].get("branch", ""), item[1].get("report_date", "")))


def write_risk_payload(payload: Mapping[str, Any], *, output_root: str | Path | None = None) -> Path:
    """Persist the aggregate risk datastore."""

    return write_json_file(persistent_risk_path(output_root), dict(payload))


def write_daily_audit(report_date: str, payload: Mapping[str, Any], *, output_root: str | Path | None = None) -> Path:
    """Persist one daily risk audit payload."""

    return write_json_file(daily_audit_path(report_date, output_root), dict(payload))


def write_branch_audit(branch: str, payload: Mapping[str, Any], *, output_root: str | Path | None = None) -> Path:
    """Persist one branch risk audit payload."""

    return write_json_file(branch_audit_path(branch, output_root), dict(payload))


def write_dashboard_summary(payload: Mapping[str, Any], *, output_root: str | Path | None = None) -> Path:
    """Persist the dashboard-ready summary payload."""

    return write_json_file(dashboard_summary_path(output_root), dict(payload))


def iso_timestamp() -> str:
    """Return the current UTC timestamp in ISO format."""

    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _ignored_record_path(path: Path) -> bool:
    return any(path.name.endswith(suffix) for suffix in _IGNORED_RECORD_SUFFIXES)


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None
