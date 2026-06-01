"""TAOP read-only export helpers over structured operations summaries."""

from __future__ import annotations

import csv
from datetime import datetime
import io
from pathlib import Path

from packages.common.paths import REPO_ROOT
from packages.record_store.writer import write_json_file, write_text_file
from packages.taop_ops.loader import (
    load_attendance_records,
    load_bale_release_records,
    load_sales_records,
)
from packages.taop_ops.summaries import (
    build_daily_operations_summary,
    summarize_attendance,
    summarize_bale_release,
    summarize_sales,
)

_SALES_FIELDS = [
    "branch",
    "date",
    "total_cash",
    "total_card",
    "total_sales",
    "till_count",
    "traffic",
    "served",
    "conversion_rate",
    "sales_per_customer",
]
_BALE_RELEASE_FIELDS = [
    "branch",
    "date",
    "total_qty",
    "total_amount",
    "item_count",
    "bales_processed",
    "bales_released",
    "bales_pending_approval",
]
_ATTENDANCE_FIELDS = [
    "branch",
    "date",
    "present_count",
    "off_count",
    "leave_count",
    "absent_count",
    "staff_total",
]


def export_sales_csv(
    date: str,
    output_path: str | Path,
    *,
    branch: str | None = None,
    root: str | Path | None = None,
) -> Path:
    """Export read-only sales summaries to CSV."""

    summary = summarize_sales(load_sales_records(date=date, branch=branch, root=root))
    path = _resolve_output_path(output_path, "sales.csv")
    return write_text_file(path, _render_csv(summary["rows"], _SALES_FIELDS))


def export_bale_release_csv(
    date: str,
    output_path: str | Path,
    *,
    branch: str | None = None,
    root: str | Path | None = None,
) -> Path:
    """Export read-only bale-release summaries to CSV."""

    summary = summarize_bale_release(load_bale_release_records(date=date, branch=branch, root=root))
    path = _resolve_output_path(output_path, "bale_release.csv")
    return write_text_file(path, _render_csv(summary["rows"], _BALE_RELEASE_FIELDS))


def export_attendance_csv(
    date: str,
    output_path: str | Path,
    *,
    branch: str | None = None,
    root: str | Path | None = None,
) -> Path:
    """Export read-only attendance summaries to CSV."""

    summary = summarize_attendance(load_attendance_records(date=date, branch=branch, root=root))
    path = _resolve_output_path(output_path, "attendance.csv")
    return write_text_file(path, _render_csv(summary["rows"], _ATTENDANCE_FIELDS))


def export_daily_operations_json(
    date: str,
    output_path: str | Path,
    *,
    branch: str | None = None,
    root: str | Path | None = None,
) -> Path:
    """Export the combined read-only TAOP daily operations payload to JSON."""

    summary = build_daily_operations_summary(date, branch=branch, root=str(root) if root is not None else None)
    path = _resolve_output_path(output_path, "daily_operations.json")
    return write_json_file(path, summary)


def default_export_directory(
    date: str,
    *,
    branch: str | None = None,
    root: str | Path | None = None,
) -> Path:
    """Return the default TAOP ops export directory for a date and branch filter."""

    iso_date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
    base_root = Path(root) if root is not None else REPO_ROOT
    return base_root / "records" / "exports" / "taop_ops" / iso_date


def display_export_path(path: str | Path, *, root: str | Path | None = None) -> str:
    """Return one repo-relative export path when possible."""

    target = Path(path)
    base_root = Path(root) if root is not None else REPO_ROOT
    try:
        return str(target.relative_to(base_root))
    except ValueError:
        return str(target)


def _resolve_output_path(output_path: str | Path, filename: str) -> Path:
    path = Path(output_path)
    if path.suffix:
        return path
    return path / filename


def _render_csv(rows: list[dict[str, object]], fieldnames: list[str]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({fieldname: row.get(fieldname) for fieldname in fieldnames})
    return buffer.getvalue()
