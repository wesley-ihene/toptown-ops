"""Read-only TAOP operations summaries over structured records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from packages.common.branch import canonical_branch_slug_or_none
from packages.taop_ops.loader import load_daily_operations
from packages.taop_ops.schemas import (
    AttendanceSummaryRow,
    BaleReleaseSummaryRow,
    CategorySummary,
    DailyBranchOperations,
    DailyOperationsSummary,
    OpsWarning,
    SalesSummaryRow,
)


def summarize_sales(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Build read-only sales summaries from stored metrics."""

    rows = [
        SalesSummaryRow(
            branch=_record_branch(record),
            date=_record_date(record),
            total_cash=_to_float(_metric(record, "cash_sales")),
            total_card=_first_float(_metric(record, "eftpos_sales"), _metric(record, "card_sales")),
            total_sales=_first_float(_metric(record, "gross_sales"), _metric(record, "total_sales")),
            till_count=_first_float(_metric(record, "till_total"), _metric(record, "till_count")),
            traffic=_to_int(_metric(record, "traffic")),
            served=_to_int(_metric(record, "served")),
            conversion_rate=_to_float(_metric(record, "conversion_rate")),
            sales_per_customer=_to_float(_metric(record, "sales_per_customer")),
            status=_string_or_none(record.get("status")),
            governance=_mapping_or_none(record.get("governance")),
            warnings=_record_warnings(record),
        ).to_payload()
        for record in _sorted_records(records)
    ]
    warnings = [] if rows else [_category_missing_warning("sales")]
    return CategorySummary(category="sales", rows=rows, warnings=warnings).to_payload()


def summarize_bale_release(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Build read-only bale-release summaries from stored metrics."""

    rows = [
        BaleReleaseSummaryRow(
            branch=_record_branch(record),
            date=_record_date(record),
            total_qty=_to_int(_metric(record, "total_qty")),
            total_amount=_to_float(_metric(record, "total_amount")),
            item_count=_item_count(record),
            bales_processed=_to_int(_metric(record, "bales_processed")),
            bales_released=_to_int(_metric(record, "bales_released")),
            bales_pending_approval=_to_int(_metric(record, "bales_pending_approval")),
            status=_string_or_none(record.get("status")),
            governance=_mapping_or_none(record.get("governance")),
            warnings=_record_warnings(record),
        ).to_payload()
        for record in _sorted_records(records)
    ]
    warnings = [] if rows else [_category_missing_warning("bale_release")]
    return CategorySummary(category="bale_release", rows=rows, warnings=warnings).to_payload()


def summarize_attendance(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Build read-only attendance summaries from stored metrics."""

    rows = [
        AttendanceSummaryRow(
            branch=_record_branch(record),
            date=_record_date(record),
            present_count=_to_int(_metric(record, "present_count")),
            off_count=_first_int(_metric(record, "off_count"), _metric(record, "off_duty_count")),
            leave_count=_to_int(_metric(record, "leave_count")),
            absent_count=_to_int(_metric(record, "absent_count")),
            staff_total=_first_int(
                _metric(record, "total_staff_listed"),
                _metric(record, "total_staff_records"),
                _metric(record, "staff_total"),
            ),
            status=_string_or_none(record.get("status")),
            governance=_mapping_or_none(record.get("governance")),
            warnings=_record_warnings(record),
        ).to_payload()
        for record in _sorted_records(records)
    ]
    warnings = [] if rows else [_category_missing_warning("attendance")]
    return CategorySummary(category="attendance", rows=rows, warnings=warnings).to_payload()


def build_daily_operations_summary(
    date: str,
    branch: str | None = None,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Build one read-only daily operations payload from structured records."""

    bundle = load_daily_operations(date, branch=branch, root=root)
    sales_summary = summarize_sales(bundle["sales_records"])
    bale_release_summary = summarize_bale_release(bundle["bale_release_records"])
    attendance_summary = summarize_attendance(bundle["attendance_records"])
    requested_branch = bundle["branch"]
    branches = _build_branch_summaries(
        date=bundle["date"],
        requested_branch=requested_branch,
        sales_summary=sales_summary,
        bale_release_summary=bale_release_summary,
        attendance_summary=attendance_summary,
    )
    return DailyOperationsSummary(
        date=bundle["date"],
        branch=requested_branch,
        sales=sales_summary,
        bale_release=bale_release_summary,
        attendance=attendance_summary,
        branches=branches,
        warnings=bundle["warnings"],
    ).to_payload()


def _build_branch_summaries(
    *,
    date: str,
    requested_branch: str | None,
    sales_summary: Mapping[str, Any],
    bale_release_summary: Mapping[str, Any],
    attendance_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    sales_rows = _rows_by_branch(sales_summary)
    bale_rows = _rows_by_branch(bale_release_summary)
    attendance_rows = _rows_by_branch(attendance_summary)
    branches: set[str] = set()
    branches.update(sales_rows)
    branches.update(bale_rows)
    branches.update(attendance_rows)
    if requested_branch is not None:
        branches.add(requested_branch)
    summaries: list[dict[str, Any]] = []
    for branch in sorted(branches):
        sales_row = sales_rows.get(branch) or _empty_sales_row(branch=branch, date=date)
        bale_row = bale_rows.get(branch) or _empty_bale_release_row(branch=branch, date=date)
        attendance_row = attendance_rows.get(branch) or _empty_attendance_row(branch=branch, date=date)
        summaries.append(
            DailyBranchOperations(
                branch=branch,
                date=date,
                sales=sales_row,
                bale_release=bale_row,
                attendance=attendance_row,
                warnings=_merge_warning_lists(
                    sales_row.get("warnings", []),
                    bale_row.get("warnings", []),
                    attendance_row.get("warnings", []),
                    [] if branch in sales_rows else [_branch_missing_warning(branch, date, "sales")],
                    [] if branch in bale_rows else [_branch_missing_warning(branch, date, "bale_release")],
                    [] if branch in attendance_rows else [_branch_missing_warning(branch, date, "attendance")],
                ),
            ).to_payload()
        )
    return summaries


def _rows_by_branch(summary: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = summary.get("rows")
    if not isinstance(rows, Sequence):
        return {}
    by_branch: dict[str, dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            branch = _string_or_none(row.get("branch"))
            if branch is not None:
                by_branch[branch] = dict(row)
    return by_branch


def _empty_sales_row(*, branch: str, date: str) -> dict[str, Any]:
    return SalesSummaryRow(branch=branch, date=date).to_payload()


def _empty_bale_release_row(*, branch: str, date: str) -> dict[str, Any]:
    return BaleReleaseSummaryRow(branch=branch, date=date).to_payload()


def _empty_attendance_row(*, branch: str, date: str) -> dict[str, Any]:
    return AttendanceSummaryRow(branch=branch, date=date).to_payload()


def _branch_missing_warning(branch: str, date: str, category: str) -> dict[str, Any]:
    return OpsWarning(
        code=f"missing_{category}_records",
        message=f"No structured {category.replace('_', ' ')} records were found for {branch} on {date}.",
        branch=branch,
        date=date,
        category=category,
    ).to_payload()


def _category_missing_warning(category: str) -> dict[str, Any]:
    return OpsWarning(
        code=f"missing_{category}_records",
        message=f"No structured {category.replace('_', ' ')} records were found for the requested filters.",
        category=category,
    ).to_payload()


def _record_branch(record: Mapping[str, Any]) -> str:
    branch_slug = _string_or_none(record.get("branch_slug"))
    if branch_slug is not None:
        return branch_slug
    branch_value = _string_or_none(record.get("branch"))
    if branch_value is not None:
        canonical_branch = canonical_branch_slug_or_none(branch_value)
        if canonical_branch is not None:
            return canonical_branch
    governance = _mapping_or_none(record.get("governance")) or {}
    governance_branch = _string_or_none(governance.get("branch"))
    if governance_branch is not None:
        canonical_branch = canonical_branch_slug_or_none(governance_branch)
        if canonical_branch is not None:
            return canonical_branch
    return "unknown"


def _record_date(record: Mapping[str, Any]) -> str:
    report_date = _string_or_none(record.get("report_date"))
    if report_date is not None:
        return report_date
    governance = _mapping_or_none(record.get("governance")) or {}
    governed_date = _string_or_none(governance.get("report_date"))
    return governed_date or "unknown"


def _record_warnings(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    warnings = record.get("warnings")
    if not isinstance(warnings, Sequence):
        return []
    payloads: list[dict[str, Any]] = []
    for warning in warnings:
        if isinstance(warning, Mapping):
            payloads.append(dict(warning))
    return payloads


def _sorted_records(records: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(records, key=lambda record: (_record_branch(record), _record_date(record)))


def _metric(record: Mapping[str, Any], key: str) -> Any:
    metrics = _mapping_or_none(record.get("metrics"))
    if metrics is None:
        return None
    return metrics.get(key)


def _mapping_or_none(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _item_count(record: Mapping[str, Any]) -> int | None:
    items = record.get("items")
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes, bytearray)):
        return None
    return len(items)


def _merge_warning_lists(*warning_lists: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for warning_list in warning_lists:
        for warning in warning_list:
            if not isinstance(warning, Mapping):
                continue
            key = (
                warning.get("code"),
                warning.get("message"),
                warning.get("severity"),
                warning.get("branch"),
                warning.get("date"),
                warning.get("category"),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(dict(warning))
    return merged


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        converted = _to_float(value)
        if converted is not None:
            return converted
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _first_int(*values: Any) -> int | None:
    for value in values:
        converted = _to_int(value)
        if converted is not None:
            return converted
    return None


def _string_or_none(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
