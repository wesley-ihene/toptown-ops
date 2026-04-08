"""Deterministic Phase 3 analytics builders over structured upstream records."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from apps.hr_agent.scoring import compute_performance_score
from packages.branch_registry import canonical_branch_slug
from packages.common.paths import REPO_ROOT
from packages.record_store.naming import safe_segment
from packages.record_store.paths import get_structured_path, get_structured_path_for_root
from packages.record_store.reader import read_structured
from packages.record_store.writer import ensure_directory, write_json_file

_ISO_DATE_PATTERN = "YYYY-MM-DD"
_ACTIVE_DUTY_STATUSES = {"on_duty", "present"}
_INACTIVE_DUTY_STATUSES = {"off_duty", "sick"}
_LOW_CONVERSION_THRESHOLD = 0.5
_OPERATIONAL_SCORE_BASE = 100
_SEVERITY_PENALTIES = {
    "error": 30,
    "warning": 10,
    "info": 0,
}


def safe_div(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Return one safe division result or `None` when unavailable."""

    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def build_branch_daily_analytics(
    branch: str,
    report_date: str,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Build one branch daily analytics payload from structured inputs."""

    canonical_branch = _canonical_branch(branch)
    iso_date = _validate_iso_date(report_date)
    sales_signal = _load_signal("sales_income", canonical_branch, iso_date, root=root)
    staff_signal = _load_signal("hr_performance", canonical_branch, iso_date, root=root)
    source_records = {
        "sales_income": _structured_record_display_path("sales_income", canonical_branch, iso_date, root=root),
        "hr_performance": _structured_record_display_path("hr_performance", canonical_branch, iso_date, root=root),
    }

    warnings: list[dict[str, str]] = []
    operational_flags: list[dict[str, str]] = []

    sales_metrics = _sales_metrics(sales_signal)
    staff_rows = _staff_rows(staff_signal)
    active_staff_count = _count_matching_statuses(staff_rows, _ACTIVE_DUTY_STATUSES) if staff_signal else None
    off_count = _count_matching_statuses(staff_rows, {"off_duty"}) if staff_signal else None
    sick_count = _count_matching_statuses(staff_rows, {"sick"}) if staff_signal else None
    total_items_moved = _staff_total(staff_signal, "total_items_moved")
    total_assisting_count = _staff_total(staff_signal, "total_assisting_count")
    unresolved_section_count = _staff_total(staff_signal, "unresolved_section_count")

    if sales_signal is None:
        warning = _warning(
            code="missing_sales_record",
            severity="error",
            message="Structured sales record is missing for the requested branch/date.",
            source="sales_income",
        )
        warnings.append(warning)
        operational_flags.append(warning)
    if staff_signal is None:
        warning = _warning(
            code="missing_staff_record",
            severity="warning",
            message="Structured staff performance record is missing for the requested branch/date.",
            source="hr_performance",
        )
        warnings.append(warning)
        operational_flags.append(warning)

    if sales_signal is not None:
        sales_status = _string_or_none(sales_signal.get("status"))
        if sales_status not in {"ready", "accepted", "accepted_split"}:
            flag = _warning(
                code="sales_status_review",
                severity="warning",
                message=f"Sales record status is `{sales_status or 'unknown'}`.",
                source="sales_income",
            )
            warnings.append(flag)
            operational_flags.append(flag)
        if sales_metrics["labor_hours_source"] == "derived":
            warnings.append(
                _warning(
                    code="labor_hours_derived",
                    severity="info",
                    message="Labor hours were derived from gross sales and sales_per_labor_hour.",
                    source="sales_income",
                )
            )

    if staff_signal is not None:
        staff_status = _string_or_none(staff_signal.get("status"))
        if staff_status not in {"accepted", "ready"}:
            flag = _warning(
                code="staff_status_review",
                severity="warning",
                message=f"Staff performance record status is `{staff_status or 'unknown'}`.",
                source="hr_performance",
            )
            warnings.append(flag)
            operational_flags.append(flag)
        if unresolved_section_count not in (None, 0):
            flag = _warning(
                code="unresolved_sections_present",
                severity="warning",
                message=f"{int(unresolved_section_count)} staff record(s) still have unresolved sections.",
                source="hr_performance",
            )
            warnings.append(flag)
            operational_flags.append(flag)
        if active_staff_count == 0:
            flag = _warning(
                code="no_active_staff",
                severity="warning",
                message="No active staff were identified in the structured staff record.",
                source="hr_performance",
            )
            warnings.append(flag)
            operational_flags.append(flag)

    conversion_rate = sales_metrics["conversion_rate"]
    if conversion_rate is not None and conversion_rate < _LOW_CONVERSION_THRESHOLD:
        flag = _warning(
            code="low_conversion_rate",
            severity="warning",
            message=f"Conversion rate {conversion_rate:.2f} is below the branch review threshold.",
            source="sales_income",
        )
        warnings.append(flag)
        operational_flags.append(flag)

    gross_sales = sales_metrics["gross_sales"]
    labor_hours = sales_metrics["labor_hours"]
    payload = {
        "branch": canonical_branch,
        "report_date": iso_date,
        "source_records": source_records,
        "sources": {
            "sales_income": sales_signal is not None,
            "hr_performance": staff_signal is not None,
        },
        "gross_sales": _round_metric(gross_sales, 2),
        "traffic": _round_count(sales_metrics["traffic"]),
        "served": _round_count(sales_metrics["served"]),
        "labor_hours": _round_metric(labor_hours, 2),
        "active_staff_count": _round_count(active_staff_count),
        "off_count": _round_count(off_count),
        "sick_count": _round_count(sick_count),
        "total_items_moved": _round_count(total_items_moved),
        "total_assisting_count": _round_count(total_assisting_count),
        "sales_per_active_staff": _round_metric(safe_div(gross_sales, active_staff_count), 2),
        "items_per_active_staff": _round_metric(safe_div(total_items_moved, active_staff_count), 2),
        "assists_per_active_staff": _round_metric(safe_div(total_assisting_count, active_staff_count), 2),
        "sales_per_labor_hour": _round_metric(
            _first_number(sales_metrics["sales_per_labor_hour"], safe_div(gross_sales, labor_hours)),
            2,
        ),
        "conversion_rate": _round_metric(
            _first_number(conversion_rate, safe_div(sales_metrics["served"], sales_metrics["traffic"])),
            4,
        ),
        "operational_flags": operational_flags,
        "warnings": warnings,
        "traceability": {
            "sales_status": _string_or_none(sales_signal.get("status")) if isinstance(sales_signal, Mapping) else None,
            "staff_status": _string_or_none(staff_signal.get("status")) if isinstance(staff_signal, Mapping) else None,
            "sales_warning_count": _warning_count(sales_signal),
            "staff_warning_count": _warning_count(staff_signal),
        },
    }
    return payload


def build_staff_leaderboard(
    branch: str,
    report_date: str,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Build one staff leaderboard payload from structured staff performance data."""

    canonical_branch = _canonical_branch(branch)
    iso_date = _validate_iso_date(report_date)
    staff_signal = _load_signal("hr_performance", canonical_branch, iso_date, root=root)
    source_records = {
        "hr_performance": _structured_record_display_path("hr_performance", canonical_branch, iso_date, root=root),
    }

    warnings: list[dict[str, str]] = []
    rows = _staff_rows(staff_signal)
    if staff_signal is None:
        warnings.append(
            _warning(
                code="missing_staff_record",
                severity="warning",
                message="Structured staff performance record is missing for the requested branch/date.",
                source="hr_performance",
            )
        )

    top_items = sorted(rows, key=lambda row: (-row["items_moved"], row["staff_name"]))
    top_assisting = sorted(rows, key=lambda row: (-row["assisting_count"], row["staff_name"]))
    top_activity_score = sorted(rows, key=lambda row: (-row["activity_score"], row["staff_name"]))
    lowest_productivity = sorted(
        [row for row in rows if row["duty_status"] in _ACTIVE_DUTY_STATUSES],
        key=lambda row: (row["activity_score"], row["staff_name"]),
    )

    role_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    duty_status_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        role_buckets[row["role"] or "unassigned"].append(row)
        duty_status_buckets[row["duty_status"] or "unknown"].append(row)

    unresolved_section_count = _staff_total(staff_signal, "unresolved_section_count")
    if unresolved_section_count is None:
        unresolved_section_count = sum(1 for row in rows if row["section"] is None)

    summary_counts = {
        "total_staff_count": len(rows),
        "active_staff_count": _count_matching_statuses(rows, _ACTIVE_DUTY_STATUSES),
        "off_count": _count_matching_statuses(rows, {"off_duty"}),
        "sick_count": _count_matching_statuses(rows, {"sick"}),
        "unknown_duty_status_count": _count_matching_statuses(rows, {"unknown"}),
        "total_items_moved": sum(entry["items_moved"] for entry in rows),
        "total_assisting_count": sum(entry["assisting_count"] for entry in rows),
        "unresolved_section_count": unresolved_section_count,
    }

    role_summaries = [
        {
            "role": role,
            "staff_count": len(bucket),
            "active_staff_count": _count_matching_statuses(bucket, _ACTIVE_DUTY_STATUSES),
            "total_items_moved": sum(entry["items_moved"] for entry in bucket),
            "total_assisting_count": sum(entry["assisting_count"] for entry in bucket),
            "avg_activity_score": _round_metric(
                safe_div(sum(entry["activity_score"] for entry in bucket), len(bucket)),
                2,
            ),
        }
        for role, bucket in sorted(role_buckets.items())
    ]
    duty_status_summaries = [
        {
            "duty_status": status,
            "staff_count": len(bucket),
            "total_items_moved": sum(entry["items_moved"] for entry in bucket),
            "total_assisting_count": sum(entry["assisting_count"] for entry in bucket),
            "avg_activity_score": _round_metric(
                safe_div(sum(entry["activity_score"] for entry in bucket), len(bucket)),
                2,
            ),
        }
        for status, bucket in sorted(duty_status_buckets.items())
    ]

    return {
        "branch": canonical_branch,
        "report_date": iso_date,
        "source_records": source_records,
        "summary_counts": summary_counts,
        "top_items_moved": [_ranked_entry(index, row, "items_moved") for index, row in enumerate(top_items, start=1)],
        "top_assisting": [_ranked_entry(index, row, "assisting_count") for index, row in enumerate(top_assisting, start=1)],
        "top_activity_score": [
            _ranked_entry(index, row, "activity_score") for index, row in enumerate(top_activity_score, start=1)
        ],
        "lowest_productivity": [
            _ranked_entry(index, row, "activity_score") for index, row in enumerate(lowest_productivity, start=1)
        ],
        "role_summaries": role_summaries,
        "duty_status_summaries": duty_status_summaries,
        "warnings": warnings,
    }


def build_section_productivity(
    branch: str,
    report_date: str,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Build one section productivity payload from structured staff performance data."""

    canonical_branch = _canonical_branch(branch)
    iso_date = _validate_iso_date(report_date)
    staff_signal = _load_signal("hr_performance", canonical_branch, iso_date, root=root)
    source_records = {
        "hr_performance": _structured_record_display_path("hr_performance", canonical_branch, iso_date, root=root),
    }

    warnings: list[dict[str, str]] = []
    rows = _staff_rows(staff_signal)
    if staff_signal is None:
        warnings.append(
            _warning(
                code="missing_staff_record",
                severity="warning",
                message="Structured staff performance record is missing for the requested branch/date.",
                source="hr_performance",
            )
        )

    section_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unresolved_rows: list[dict[str, Any]] = []
    for row in rows:
        if row["section"] is None:
            unresolved_rows.append(row)
            continue
        section_buckets[row["section"]].append(row)

    sections = []
    for section, bucket in sorted(section_buckets.items()):
        total_items_moved = sum(entry["items_moved"] for entry in bucket)
        total_assisting_count = sum(entry["assisting_count"] for entry in bucket)
        total_activity_score = sum(entry["activity_score"] for entry in bucket)
        productivity_index = (total_items_moved * 0.6) + (total_assisting_count * 0.4)
        sections.append(
            {
                "section": section,
                "staff_count": len(bucket),
                "items_moved": total_items_moved,
                "assisting_count": total_assisting_count,
                "avg_activity_score": _round_metric(safe_div(total_activity_score, len(bucket)), 2),
                "productivity_index": _round_metric(productivity_index, 2),
            }
        )
    sections.sort(key=lambda entry: (-entry["productivity_index"], entry["section"]))

    diagnostics = staff_signal.get("diagnostics") if isinstance(staff_signal, Mapping) else {}
    resolution_stats = diagnostics.get("section_resolution_stats") if isinstance(diagnostics, Mapping) else {}
    unresolved_examples = resolution_stats.get("unresolved_examples") if isinstance(resolution_stats, Mapping) else []
    unresolved_section_count = _staff_total(staff_signal, "unresolved_section_count")
    if unresolved_section_count is None:
        unresolved_section_count = len(unresolved_rows)

    return {
        "branch": canonical_branch,
        "report_date": iso_date,
        "source_records": source_records,
        "sections": sections,
        "unresolved_section_tracking": {
            "count": unresolved_section_count,
            "examples": list(unresolved_examples) if isinstance(unresolved_examples, list) else [],
            "staff_records": [
                {
                    "staff_name": row["staff_name"],
                    "raw_section": row["raw_section"],
                    "duty_status": row["duty_status"],
                }
                for row in unresolved_rows
            ],
        },
        "warnings": warnings,
    }


def build_branch_comparison(
    report_date: str,
    *,
    root: str | Path | None = None,
    branches: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build one branch comparison payload for all available branches on a date."""

    iso_date = _validate_iso_date(report_date)
    branch_list = sorted({_canonical_branch(branch) for branch in (branches or _available_branches(iso_date, root=root))})
    warnings: list[dict[str, str]] = []

    if not branch_list:
        warnings.append(
            _warning(
                code="no_branch_inputs",
                severity="warning",
                message="No structured sales or staff records were found for the requested date.",
                source="branch_comparison",
            )
        )

    branch_daily_payloads = [build_branch_daily_analytics(branch, iso_date, root=root) for branch in branch_list]
    scorecards = []
    for payload in branch_daily_payloads:
        staff_productivity_index = _round_metric(
            _first_number(
                safe_div(payload.get("total_items_moved"), payload.get("active_staff_count")),
                None,
            ),
            2,
        )
        assists_per_active_staff = payload.get("assists_per_active_staff")
        if staff_productivity_index is not None and assists_per_active_staff is not None:
            staff_productivity_index = _round_metric(staff_productivity_index + (float(assists_per_active_staff) * 0.5), 2)

        operational_score = _operational_score(payload.get("operational_flags"))
        scorecards.append(
            {
                "branch": payload["branch"],
                "gross_sales": payload.get("gross_sales"),
                "active_staff_count": payload.get("active_staff_count"),
                "conversion_rate": payload.get("conversion_rate"),
                "sales_per_active_staff": payload.get("sales_per_active_staff"),
                "staff_productivity_index": staff_productivity_index,
                "operational_score": operational_score,
                "flag_count": len(payload.get("operational_flags", [])),
                "warning_count": len(payload.get("warnings", [])),
                "source_records": payload.get("source_records", {}),
            }
        )
        missing_sources = sorted(
            source_name
            for source_name, present in (payload.get("sources") or {}).items()
            if present is False
        )
        if missing_sources:
            warnings.append(
                _warning(
                    code="branch_missing_inputs",
                    severity="warning",
                    message=(
                        f"Branch `{payload['branch']}` is missing structured inputs: {', '.join(missing_sources)}."
                    ),
                    source="branch_daily_analytics",
                )
            )
    scorecards.sort(key=lambda entry: entry["branch"])

    return {
        "report_date": iso_date,
        "branch_scorecards": scorecards,
        "ranked_branches_by_sales": _rank_scorecards(scorecards, "gross_sales"),
        "ranked_branches_by_staff_productivity": _rank_scorecards(scorecards, "staff_productivity_index"),
        "ranked_branches_by_conversion": _rank_scorecards(scorecards, "conversion_rate"),
        "ranked_branches_by_operational_score": _rank_scorecards(scorecards, "operational_score"),
        "warnings": warnings,
    }


def write_branch_daily_analytics_json(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Write one branch daily analytics payload to its canonical JSON path."""

    path = get_branch_daily_analytics_path(
        payload["branch"],
        payload["report_date"],
        output_root=output_root,
    )
    _ensure_writable(path, overwrite=overwrite)
    return write_json_file(path, dict(payload))


def write_staff_leaderboard_json(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Write one staff leaderboard payload to its canonical JSON path."""

    path = get_staff_leaderboard_path(
        payload["branch"],
        payload["report_date"],
        output_root=output_root,
    )
    _ensure_writable(path, overwrite=overwrite)
    return write_json_file(path, dict(payload))


def write_section_productivity_json(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Write one section productivity payload to its canonical JSON path."""

    path = get_section_productivity_path(
        payload["branch"],
        payload["report_date"],
        output_root=output_root,
    )
    _ensure_writable(path, overwrite=overwrite)
    return write_json_file(path, dict(payload))


def write_branch_comparison_json(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Write one branch comparison payload to its canonical JSON path."""

    path = get_branch_comparison_path(
        payload["report_date"],
        output_root=output_root,
    )
    _ensure_writable(path, overwrite=overwrite)
    return write_json_file(path, dict(payload))


def get_branch_daily_analytics_path(branch: str, report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the canonical output path for one branch daily analytics file."""

    return _analytics_root(output_root) / "branch_daily" / safe_segment(branch) / f"{report_date}.json"


def get_staff_leaderboard_path(branch: str, report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the canonical output path for one staff leaderboard file."""

    return _analytics_root(output_root) / "staff_daily" / safe_segment(branch) / f"{report_date}.json"


def get_section_productivity_path(branch: str, report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the canonical output path for one section productivity file."""

    return _analytics_root(output_root) / "section_daily" / safe_segment(branch) / f"{report_date}.json"


def get_branch_comparison_path(report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the canonical output path for one branch comparison file."""

    return _analytics_root(output_root) / "branch_comparison" / f"{report_date}.json"


def _analytics_root(output_root: str | Path | None) -> Path:
    base_root = _root_path(output_root)
    return base_root / "analytics"


def _structured_record_display_path(
    signal_type: str,
    branch: str,
    report_date: str,
    *,
    root: str | Path | None = None,
) -> str | None:
    path = _structured_record_path(signal_type, branch, report_date, root=root)
    if not path.exists():
        return None
    return _display_path(path, root=root)


def _structured_record_path(
    signal_type: str,
    branch: str,
    report_date: str,
    *,
    root: str | Path | None = None,
) -> Path:
    if root is None:
        return get_structured_path(signal_type, branch, report_date)
    return get_structured_path_for_root(
        _root_path(root) / "records" / "structured",
        signal_type,
        branch,
        report_date,
    )


def _load_signal(
    signal_type: str,
    branch: str,
    report_date: str,
    *,
    root: str | Path | None = None,
) -> dict[str, Any] | None:
    payload = read_structured(signal_type, branch, report_date, root=root)
    if payload is None:
        return None
    if not _payload_matches_request(payload, branch=branch, report_date=report_date):
        return None
    return payload


def _payload_matches_request(
    payload: Mapping[str, Any],
    *,
    branch: str,
    report_date: str,
) -> bool:
    branch_value = payload.get("branch_slug") or payload.get("branch")
    payload_branch = _string_or_none(branch_value)
    payload_date = _string_or_none(payload.get("report_date"))
    if payload_branch is None or payload_date is None:
        return False
    try:
        return _canonical_branch(payload_branch) == branch and _validate_iso_date(payload_date) == report_date
    except ValueError:
        return False


def _sales_metrics(signal: Mapping[str, Any] | None) -> dict[str, float | int | None | str]:
    gross_sales = _extract_number(signal, "metrics.gross_sales", "gross_sales")
    traffic = _extract_number(signal, "metrics.traffic", "traffic")
    served = _extract_number(signal, "metrics.served", "served", "metrics.customer_count")
    sales_per_labor_hour = _extract_number(signal, "metrics.sales_per_labor_hour", "sales_per_labor_hour")
    explicit_labor_hours = _extract_number(signal, "metrics.labor_hours", "labor_hours")
    derived_labor_hours = safe_div(gross_sales, sales_per_labor_hour)
    labor_hours = _first_number(explicit_labor_hours, derived_labor_hours)
    labor_hours_source = "explicit" if explicit_labor_hours is not None else "derived" if labor_hours is not None else "missing"
    return {
        "gross_sales": gross_sales,
        "traffic": traffic,
        "served": served,
        "labor_hours": labor_hours,
        "labor_hours_source": labor_hours_source,
        "sales_per_labor_hour": sales_per_labor_hour,
        "conversion_rate": _extract_number(signal, "metrics.conversion_rate", "conversion_rate"),
    }


def _staff_total(signal: Mapping[str, Any] | None, metric_name: str) -> int | None:
    value = _extract_number(signal, f"metrics.{metric_name}", metric_name)
    if value is None:
        return None
    return int(value)


def _staff_rows(signal: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(signal, Mapping):
        return []
    items = signal.get("items")
    if not isinstance(items, list):
        return []

    rows = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        items_moved = int(_extract_number(item, "items_moved") or 0)
        assisting_count = int(_extract_number(item, "assisting_count") or 0)
        activity_score = _extract_number(item, "activity_score")
        if activity_score is None:
            activity_score = compute_performance_score(
                items_moved=items_moved,
                assisting_count=assisting_count,
            )
        rows.append(
            {
                "staff_name": _string_or_none(item.get("staff_name")) or "Unknown",
                "role": _string_or_none(item.get("role")),
                "duty_status": _string_or_none(item.get("duty_status")) or "unknown",
                "section": _string_or_none(item.get("section")),
                "raw_section": _string_or_none(item.get("raw_section")),
                "items_moved": items_moved,
                "assisting_count": assisting_count,
                "activity_score": round(float(activity_score), 2),
            }
        )
    return rows


def _count_matching_statuses(rows: Sequence[Mapping[str, Any]], statuses: set[str]) -> int:
    return sum(1 for row in rows if _string_or_none(row.get("duty_status")) in statuses)


def _ranked_entry(rank: int, row: Mapping[str, Any], metric_name: str) -> dict[str, Any]:
    return {
        "rank": rank,
        "staff_name": row["staff_name"],
        "role": row["role"],
        "duty_status": row["duty_status"],
        "section": row["section"],
        metric_name: row[metric_name],
        "items_moved": row["items_moved"],
        "assisting_count": row["assisting_count"],
        "activity_score": row["activity_score"],
    }


def _rank_scorecards(scorecards: Sequence[Mapping[str, Any]], metric_name: str) -> list[dict[str, Any]]:
    ranked = sorted(
        scorecards,
        key=lambda entry: (
            entry.get(metric_name) is None,
            -(float(entry[metric_name]) if entry.get(metric_name) is not None else 0.0),
            entry["branch"],
        ),
    )
    return [
        {
            "rank": index,
            "branch": entry["branch"],
            metric_name: entry.get(metric_name),
        }
        for index, entry in enumerate(ranked, start=1)
    ]


def _available_branches(report_date: str, *, root: str | Path | None = None) -> set[str]:
    structured_root = _root_path(root) / "records" / "structured"
    branches: set[str] = set()
    for signal_type in ("sales_income", "hr_performance"):
        signal_dir = structured_root / signal_type
        if not signal_dir.exists():
            continue
        for branch_dir in signal_dir.iterdir():
            if not branch_dir.is_dir():
                continue
            if (branch_dir / f"{report_date}.json").exists():
                branches.add(_canonical_branch(branch_dir.name))
    return branches


def _operational_score(flags: Any) -> int:
    if not isinstance(flags, list):
        return _OPERATIONAL_SCORE_BASE
    score = _OPERATIONAL_SCORE_BASE
    for flag in flags:
        if not isinstance(flag, Mapping):
            continue
        severity = _string_or_none(flag.get("severity")) or "warning"
        score -= _SEVERITY_PENALTIES.get(severity, 10)
    return max(0, score)


def _warning_count(signal: Mapping[str, Any] | None) -> int:
    warnings = signal.get("warnings") if isinstance(signal, Mapping) else None
    return len(warnings) if isinstance(warnings, list) else 0


def _warning(*, code: str, severity: str, message: str, source: str) -> dict[str, str]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "source": source,
    }


def _extract_number(mapping: Mapping[str, Any] | None, *paths: str) -> float | None:
    for path in paths:
        value = _extract_path(mapping, path)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _extract_path(mapping: Mapping[str, Any] | None, path: str) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    current: Any = mapping
    for segment in path.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return None
        current = current[segment]
    return current


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _first_number(*values: float | int | None) -> float | None:
    for value in values:
        if value is not None:
            return float(value)
    return None


def _round_metric(value: float | None, digits: int) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _round_count(value: float | int | None) -> int | None:
    if value is None:
        return None
    return int(round(float(value)))


def _canonical_branch(branch: str) -> str:
    return canonical_branch_slug(branch).strip()


def _validate_iso_date(report_date: str) -> str:
    candidate = report_date.strip()
    parts = candidate.split("-")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        raise ValueError(f"report date must use {_ISO_DATE_PATTERN}")
    year, month, day = parts
    if len(year) != 4 or len(month) != 2 or len(day) != 2:
        raise ValueError(f"report date must use {_ISO_DATE_PATTERN}")
    return candidate


def _root_path(root: str | Path | None) -> Path:
    return REPO_ROOT if root is None else Path(root)


def _ensure_writable(path: Path, *, overwrite: bool) -> None:
    ensure_directory(path.parent)
    if path.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite existing analytics file: {path}")


def _display_path(path: Path, *, root: str | Path | None = None) -> str:
    base_root = _root_path(root)
    try:
        return str(path.relative_to(base_root))
    except ValueError:
        return str(path)


def main(argv: Sequence[str] | None = None) -> int:
    """Minimal CLI for one branch daily analytics build."""

    parser = argparse.ArgumentParser(description="Build Phase 3 branch daily analytics.")
    parser.add_argument("--branch", required=True, help="Canonical branch slug or recognized branch label.")
    parser.add_argument("--date", required=True, help="ISO report date in YYYY-MM-DD format.")
    parser.add_argument("--root", help="Repository root override.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting existing analytics output.")
    parser.add_argument("--print-json", action="store_true", help="Print JSON payload to stdout.")
    args = parser.parse_args(argv)

    payload = build_branch_daily_analytics(args.branch, args.date, root=args.root)
    path = write_branch_daily_analytics_json(payload, output_root=args.root, overwrite=args.overwrite)
    if args.print_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_display_path(path, root=args.root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
