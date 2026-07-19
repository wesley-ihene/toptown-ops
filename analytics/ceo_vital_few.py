"""Read-only Phase 1 KPI presentation contract for the CEO dashboard.

This module computes factual operational and people metrics from TAOP-owned
analytics and structured records.  It does not persist results, call external
systems, rank Colony intelligence, or trigger actions.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
import json
from pathlib import Path
from statistics import mean, median
from typing import Any

from packages.branch_registry import CANONICAL_BRANCHES, canonical_branch_slug_or_none
from packages.common.paths import REPO_ROOT

CONTRACT_VERSION = "ceo-kpi.v1"
BASELINE_DAYS = 56
TREND_DAYS = 7
HISTORY_DAYS = 63

_KPI_SPECS: tuple[dict[str, Any], ...] = (
    {
        "id": "sales_per_labor_hour",
        "domain": "operational_efficiency",
        "label": "Sales per labour hour",
        "formula": "total sales ÷ labour hours",
        "unit": "PGK_PER_HOUR",
        "cadence": "daily",
        "threshold_kind": "below_baseline_15",
        "threshold_rule": "Flag when more than 15% below the trailing 8-week median.",
        "source": "TAOP sales analytics",
    },
    {
        "id": "conversion_rate",
        "domain": "operational_efficiency",
        "label": "Conversion rate",
        "formula": "customers served ÷ door count",
        "unit": "PERCENT",
        "cadence": "daily",
        "threshold_kind": "below_baseline_two_days",
        "threshold_rule": "Flag when below the trailing 8-week median for two consecutive calendar days.",
        "source": "TAOP sales analytics",
    },
    {
        "id": "sales_per_active_staff",
        "domain": "operational_efficiency",
        "label": "Sales per active staff",
        "formula": "total sales ÷ active staff",
        "unit": "PGK_PER_STAFF",
        "cadence": "daily",
        "threshold_kind": "below_baseline_15",
        "threshold_rule": "Flag when more than 15% below the trailing 8-week median.",
        "source": "TAOP sales and staff-performance analytics",
    },
    {
        "id": "stock_release_to_sales",
        "domain": "operational_efficiency",
        "label": "Stock release to sales",
        "formula": "bale value released ÷ sales",
        "unit": "RATIO",
        "cadence": "weekly",
        "threshold_kind": "stock_piling",
        "threshold_rule": "Flag when release value exceeds sales and is more than 15% above the trailing 8-week median.",
        "source": "TAOP bale-release and sales records",
    },
    {
        "id": "attendance_rate",
        "domain": "people_insights",
        "label": "Attendance rate",
        "formula": "present ÷ rostered active",
        "unit": "PERCENT",
        "cadence": "daily",
        "threshold_kind": "attendance_floor",
        "threshold_rule": "Flag when attendance is below 90%.",
        "source": "TAOP attendance records",
    },
    {
        "id": "absence_leave_load",
        "domain": "people_insights",
        "label": "Absence / leave load",
        "formula": "(absent + sick + leave) ÷ rostered active",
        "unit": "PERCENT",
        "cadence": "daily",
        "threshold_kind": "above_baseline_15",
        "threshold_rule": "Flag when more than 15% above the trailing 8-week median.",
        "source": "TAOP attendance records",
    },
    {
        "id": "staff_productivity",
        "domain": "people_insights",
        "label": "Staff productivity",
        "formula": "(items moved + assists) ÷ active staff",
        "unit": "ACTIVITIES_PER_STAFF",
        "cadence": "daily",
        "threshold_kind": "below_baseline_15",
        "threshold_rule": "Flag when more than 15% below the trailing 8-week median.",
        "source": "TAOP staff-performance analytics",
    },
)


def build_phase1_dashboard(
    report_date: str,
    *,
    branch: str | None = None,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Build the versioned Phase 1 dashboard contract without writing state."""

    selected_date = _iso_date(report_date)
    selected_branch = None
    if branch:
        selected_branch = canonical_branch_slug_or_none(branch)
        if selected_branch is None:
            raise ValueError(f"unknown branch: {branch}")

    branches = sorted(CANONICAL_BRANCHES)
    base_root = Path(root) if root is not None else REPO_ROOT
    dates = [selected_date - timedelta(days=offset) for offset in range(HISTORY_DAYS - 1, -1, -1)]
    branch_series = {
        branch_slug: {
            day: _load_branch_day(base_root, branch_slug, day)
            for day in dates
        }
        for branch_slug in branches
    }
    enterprise_series = {
        day: _aggregate_enterprise_day(
            {branch_slug: branch_series[branch_slug][day] for branch_slug in branches},
            expected_branches=len(branches),
        )
        for day in dates
    }

    branch_scopes = [
        _build_scope(
            scope="branch",
            scope_id=branch_slug,
            display_name=CANONICAL_BRANCHES[branch_slug],
            report_date=selected_date,
            series=branch_series[branch_slug],
        )
        for branch_slug in branches
    ]
    enterprise_scope = _build_scope(
        scope="enterprise",
        scope_id="enterprise",
        display_name="All branches",
        report_date=selected_date,
        series=enterprise_series,
    )
    priority_pool = (
        next((scope["exceptions"] for scope in branch_scopes if scope["scope_id"] == selected_branch), [])
        if selected_branch
        else [exception for scope in branch_scopes for exception in scope["exceptions"]]
    )
    if not priority_pool:
        priority_pool = enterprise_scope["exceptions"]
    top_priority = _top_priority(
        priority_pool=priority_pool,
        scopes=branch_scopes if selected_branch is None else [scope for scope in branch_scopes if scope["scope_id"] == selected_branch],
        report_date=selected_date,
    )

    return {
        "contract_version": CONTRACT_VERSION,
        "report_date": selected_date.isoformat(),
        "selected_scope": selected_branch or "enterprise",
        "presentation_only": True,
        "decision_authority": "CEO",
        "writes_back": False,
        "top_priority": top_priority,
        "enterprise": enterprise_scope,
        "branches": branch_scopes,
        "summary": {
            "breached_kpi_count": sum(len(scope["exceptions"]) for scope in branch_scopes),
            "branches_with_breaches": sum(bool(scope["exceptions"]) for scope in branch_scopes),
            "branch_count": len(branches),
        },
        "gated_domains": [
            {
                "domain": "financial_clarity",
                "status": "blocked",
                "reason": "Accounting integration is not configured. TAOP revenue is not profit.",
                "required_source": "Accounting system (COGS, expenses, cash, and budget)",
            },
            {
                "domain": "probation_and_labor_cost",
                "status": "blocked",
                "reason": "Review dates and payroll cost are not available in the Phase 1 source contract.",
                "required_source": "HR Phase B and Able Payroll",
            },
        ],
    }


def _build_scope(
    *,
    scope: str,
    scope_id: str,
    display_name: str,
    report_date: date,
    series: Mapping[date, Mapping[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    kpis = [_build_kpi(spec, report_date=report_date, series=series) for spec in _KPI_SPECS]
    exceptions = [
        {
            "scope": scope,
            "scope_id": scope_id,
            "scope_display_name": display_name,
            "kpi_id": kpi["id"],
            "label": kpi["label"],
            "domain": kpi["domain"],
            "value": kpi["value"],
            "unit": kpi["unit"],
            "threshold": kpi["threshold"],
            "data_completeness": kpi["data_completeness"],
        }
        for kpi in kpis
        if kpi["threshold"]["state"] == "breach"
    ]
    exceptions.sort(key=lambda item: (-float(item["threshold"].get("deviation_pct") or 0), item["kpi_id"]))
    return {
        "scope": scope,
        "scope_id": scope_id,
        "display_name": display_name,
        "kpis": kpis,
        "exceptions": exceptions,
        "exception_count": len(exceptions),
        "data_completeness": _scope_completeness(kpis),
    }


def _build_kpi(
    spec: Mapping[str, Any],
    *,
    report_date: date,
    series: Mapping[date, Mapping[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    kpi_id = str(spec["id"])
    cadence = str(spec["cadence"])
    if cadence == "weekly":
        current = _window_observation(series, kpi_id, report_date, 7)
        comparison = _window_observation(series, kpi_id, report_date - timedelta(days=7), 7)
        baseline_values = [
            _window_observation(series, kpi_id, report_date - timedelta(days=7 * week), 7)["value"]
            for week in range(1, 9)
        ]
        baseline_values = [value for value in baseline_values if value is not None]
        trend_reference = comparison["value"]
        trend_samples = comparison["observed_days"]
        trend_expected = 7
        baseline_expected = 8
    else:
        current = dict(series.get(report_date, {}).get(kpi_id) or _empty_observation())
        trend_values = _history_values(series, kpi_id, report_date, TREND_DAYS)
        baseline_values = _history_values(series, kpi_id, report_date, BASELINE_DAYS)
        trend_reference = mean(trend_values) if trend_values else None
        trend_samples = len(trend_values)
        trend_expected = TREND_DAYS
        baseline_expected = BASELINE_DAYS

    target_value = median(baseline_values) if baseline_values else None
    trend = _trend(current.get("value"), trend_reference, trend_samples, trend_expected, cadence)
    threshold = _threshold(
        kind=str(spec["threshold_kind"]),
        current=current.get("value"),
        baseline=target_value,
        yesterday=(series.get(report_date - timedelta(days=1), {}).get(kpi_id) or {}).get("value"),
        rule=str(spec["threshold_rule"]),
    )
    completeness = _metric_completeness(
        current=current,
        trend_samples=trend_samples,
        trend_expected=trend_expected,
        baseline_samples=len(baseline_values),
        baseline_expected=baseline_expected,
        threshold_state=threshold["state"],
    )
    return {
        "id": kpi_id,
        "domain": spec["domain"],
        "label": spec["label"],
        "formula": spec["formula"],
        "value": _round(current.get("value")),
        "unit": spec["unit"],
        "trend": trend,
        "target": {
            "type": "trailing_8_week_median",
            "value": _round(target_value),
            "sample_count": len(baseline_values),
            "expected_sample_count": baseline_expected,
        },
        "threshold": threshold,
        "source": {
            "system": "TAOP operational records",
            "description": spec["source"],
            "records": sorted(set(current.get("source_records") or [])),
        },
        "cadence": cadence,
        "data_completeness": completeness,
    }


def _load_branch_day(root: Path, branch: str, report_date: date) -> dict[str, dict[str, Any]]:
    iso_date = report_date.isoformat()
    analytics_path = root / "analytics" / "branch_daily" / branch / f"{iso_date}.json"
    attendance_path = root / "records" / "structured" / "hr_attendance" / branch / f"{iso_date}.json"
    pricing_path = root / "records" / "structured" / "pricing_stock_release" / branch / f"{iso_date}.json"
    daily = _load_json(analytics_path, branch=branch, report_date=iso_date)
    attendance = _load_json(attendance_path, branch=branch, report_date=iso_date)
    pricing = _load_json(pricing_path, branch=branch, report_date=iso_date)

    sales = _number(daily, "gross_sales")
    labor_hours = _number(daily, "labor_hours")
    traffic = _number(daily, "traffic")
    served = _number(daily, "served")
    active_staff = _number(daily, "active_staff_count")
    items = _number(daily, "total_items_moved")
    assists = _number(daily, "total_assisting_count")
    released_value = _nested_number(pricing, "metrics", "total_amount")
    attendance_parts = _attendance_parts(attendance)

    analytics_sources = _source_paths(root, analytics_path, daily)
    attendance_sources = _source_paths(root, attendance_path, attendance)
    pricing_sources = _source_paths(root, pricing_path, pricing)
    analytics_quality = _quality_warnings(daily)
    attendance_quality = _quality_warnings(attendance) + attendance_parts["warnings"]
    pricing_quality = _quality_warnings(pricing)
    sales_present = daily is not None and sales is not None
    staff_present = daily is not None and active_staff is not None

    return {
        "sales_per_labor_hour": _observation(
            sales,
            labor_hours,
            source_records=analytics_sources,
            missing_inputs=_missing(("sales", sales), ("labour_hours", labor_hours)),
            quality_warnings=analytics_quality,
            sources_present={"sales": sales_present},
        ),
        "conversion_rate": _observation(
            served,
            traffic,
            source_records=analytics_sources,
            missing_inputs=_missing(("customers_served", served), ("door_count", traffic)),
            quality_warnings=analytics_quality,
            sources_present={"sales": sales_present},
        ),
        "sales_per_active_staff": _observation(
            sales,
            active_staff,
            source_records=analytics_sources,
            missing_inputs=_missing(("sales", sales), ("active_staff", active_staff)),
            quality_warnings=analytics_quality,
            sources_present={"sales": sales_present, "staff_performance": staff_present},
        ),
        "stock_release_to_sales": _observation(
            released_value,
            sales,
            source_records=analytics_sources + pricing_sources,
            missing_inputs=_missing(("bale_value_released", released_value), ("sales", sales)),
            quality_warnings=analytics_quality + pricing_quality,
            sources_present={"sales": sales_present, "bale_release": pricing is not None},
        ),
        "attendance_rate": _observation(
            attendance_parts["present"],
            attendance_parts["rostered"],
            source_records=attendance_sources,
            missing_inputs=_missing(("present", attendance_parts["present"]), ("rostered_active", attendance_parts["rostered"])),
            quality_warnings=attendance_quality,
            sources_present={"attendance": attendance is not None},
        ),
        "absence_leave_load": _observation(
            attendance_parts["absence_leave"],
            attendance_parts["rostered"],
            source_records=attendance_sources,
            missing_inputs=_missing(("absent_sick_leave", attendance_parts["absence_leave"]), ("rostered_active", attendance_parts["rostered"])),
            quality_warnings=attendance_quality,
            sources_present={"attendance": attendance is not None},
        ),
        "staff_productivity": _observation(
            None if items is None or assists is None else items + assists,
            active_staff,
            source_records=analytics_sources,
            missing_inputs=_missing(("items_moved", items), ("assists", assists), ("active_staff", active_staff)),
            quality_warnings=analytics_quality,
            sources_present={"staff_performance": staff_present},
        ),
    }


def _aggregate_enterprise_day(
    branch_days: Mapping[str, Mapping[str, Mapping[str, Any]]],
    *,
    expected_branches: int,
) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    for spec in _KPI_SPECS:
        kpi_id = str(spec["id"])
        observations = [(branch, day[kpi_id]) for branch, day in branch_days.items()]
        usable = [(branch, obs) for branch, obs in observations if obs.get("value") is not None]
        numerator = sum(float(obs["numerator"]) for _, obs in usable) if usable else None
        denominator = sum(float(obs["denominator"]) for _, obs in usable) if usable else None
        missing_branches = [branch for branch, obs in observations if obs.get("value") is None]
        aggregated[kpi_id] = _observation(
            numerator,
            denominator,
            source_records=[path for _, obs in usable for path in obs.get("source_records") or []],
            missing_inputs=[f"{branch}:missing" for branch in missing_branches],
            quality_warnings=[warning for _, obs in usable for warning in obs.get("quality_warnings") or []],
            sources_present={
                "branches_reporting": len(usable),
                "branches_expected": expected_branches,
            },
            observed_days=1 if usable else 0,
            coverage_complete=len(usable) == expected_branches,
        )
    return aggregated


def _window_observation(
    series: Mapping[date, Mapping[str, Mapping[str, Any]]],
    kpi_id: str,
    end_date: date,
    days: int,
) -> dict[str, Any]:
    observations = [
        series.get(end_date - timedelta(days=offset), {}).get(kpi_id) or _empty_observation()
        for offset in range(days)
    ]
    usable = [observation for observation in observations if observation.get("value") is not None]
    numerator = sum(float(observation["numerator"]) for observation in usable) if usable else None
    denominator = sum(float(observation["denominator"]) for observation in usable) if usable else None
    return _observation(
        numerator,
        denominator,
        source_records=[path for observation in usable for path in observation.get("source_records") or []],
        missing_inputs=[item for observation in observations for item in observation.get("missing_inputs") or []],
        quality_warnings=[item for observation in usable for item in observation.get("quality_warnings") or []],
        sources_present={"days_reporting": len(usable), "days_expected": days},
        observed_days=len(usable),
        coverage_complete=len(usable) == days,
    )


def _observation(
    numerator: float | None,
    denominator: float | None,
    *,
    source_records: list[str],
    missing_inputs: list[str],
    quality_warnings: list[str],
    sources_present: Mapping[str, Any],
    observed_days: int | None = None,
    coverage_complete: bool = True,
) -> dict[str, Any]:
    value = None
    if numerator is not None and denominator is not None and denominator > 0:
        value = numerator / denominator
    return {
        "value": value,
        "numerator": numerator,
        "denominator": denominator,
        "source_records": source_records,
        "missing_inputs": missing_inputs,
        "quality_warnings": sorted(set(quality_warnings)),
        "sources_present": dict(sources_present),
        "observed_days": observed_days if observed_days is not None else (1 if value is not None else 0),
        "coverage_complete": coverage_complete,
    }


def _threshold(
    *,
    kind: str,
    current: float | None,
    baseline: float | None,
    yesterday: float | None,
    rule: str,
) -> dict[str, Any]:
    boundary = None
    state = "unavailable"
    deviation_pct = None
    if current is None:
        pass
    elif kind == "attendance_floor":
        boundary = 0.90
        state = "breach" if current < boundary else "healthy"
        deviation_pct = _relative_gap(current, boundary, lower_is_bad=True)
    elif baseline is None:
        pass
    elif kind == "below_baseline_15":
        boundary = baseline * 0.85
        state = "breach" if current < boundary else "healthy"
        deviation_pct = _relative_gap(current, boundary, lower_is_bad=True)
    elif kind == "above_baseline_15":
        boundary = baseline * 1.15
        state = "breach" if current > boundary else "healthy"
        deviation_pct = _relative_gap(current, boundary, lower_is_bad=False)
    elif kind == "stock_piling":
        boundary = max(1.0, baseline * 1.15)
        state = "breach" if current > boundary else "healthy"
        deviation_pct = _relative_gap(current, boundary, lower_is_bad=False)
    elif kind == "below_baseline_two_days":
        boundary = baseline
        if current >= boundary:
            state = "healthy"
        elif yesterday is None:
            state = "unavailable"
        else:
            state = "breach" if yesterday < boundary else "healthy"
        deviation_pct = _relative_gap(current, boundary, lower_is_bad=True)
    return {
        "state": state,
        "rule": rule,
        "boundary": _round(boundary),
        "deviation_pct": _round(deviation_pct),
    }


def _trend(
    current: float | None,
    reference: float | None,
    sample_count: int,
    expected_count: int,
    cadence: str,
) -> dict[str, Any]:
    change_pct = None
    direction = "unavailable"
    if current is not None and reference is not None:
        if reference == 0:
            change_pct = 0.0 if current == 0 else None
        else:
            change_pct = ((current - reference) / abs(reference)) * 100
        if current > reference:
            direction = "up"
        elif current < reference:
            direction = "down"
        else:
            direction = "flat"
    return {
        "comparison": "previous_week" if cadence == "weekly" else "previous_7_day_average",
        "reference_value": _round(reference),
        "change_pct": _round(change_pct),
        "direction": direction,
        "sample_count": sample_count,
        "expected_sample_count": expected_count,
    }


def _metric_completeness(
    *,
    current: Mapping[str, Any],
    trend_samples: int,
    trend_expected: int,
    baseline_samples: int,
    baseline_expected: int,
    threshold_state: str,
) -> dict[str, Any]:
    current_available = current.get("value") is not None
    issues = list(current.get("missing_inputs") or [])
    issues.extend(current.get("quality_warnings") or [])
    if not current.get("coverage_complete", True):
        issues.append("current_period_coverage_partial")
    if trend_samples < trend_expected:
        issues.append("trend_history_partial")
    if baseline_samples < baseline_expected:
        issues.append("baseline_history_partial")
    if threshold_state == "unavailable":
        issues.append("threshold_not_evaluable")
    status = "unavailable" if not current_available else ("partial" if issues else "complete")
    return {
        "status": status,
        "current_value_available": current_available,
        "current_observed_days": current.get("observed_days", 0),
        "sources_present": current.get("sources_present") or {},
        "trend_samples": trend_samples,
        "trend_expected_samples": trend_expected,
        "baseline_samples": baseline_samples,
        "baseline_expected_samples": baseline_expected,
        "issues": sorted(set(str(issue) for issue in issues)),
    }


def _scope_completeness(kpis: list[Mapping[str, Any]]) -> dict[str, Any]:
    counts = {"complete": 0, "partial": 0, "unavailable": 0}
    for kpi in kpis:
        status = str(kpi["data_completeness"]["status"])
        counts[status] = counts.get(status, 0) + 1
    available = len(kpis) - counts["unavailable"]
    return {
        "status": "complete" if counts["complete"] == len(kpis) else ("partial" if available else "unavailable"),
        "available_kpis": available,
        "total_kpis": len(kpis),
        "counts": counts,
    }


def _top_priority(
    *,
    priority_pool: list[Mapping[str, Any]],
    scopes: list[Mapping[str, Any]],
    report_date: date,
) -> dict[str, Any]:
    if priority_pool:
        selected = sorted(
            priority_pool,
            key=lambda item: (-float(item["threshold"].get("deviation_pct") or 0), str(item["scope_id"]), str(item["kpi_id"])),
        )[0]
        return {
            "kind": "threshold_breach",
            "report_date": report_date.isoformat(),
            "scope_id": selected["scope_id"],
            "scope_display_name": selected["scope_display_name"],
            "kpi_id": selected["kpi_id"],
            "label": selected["label"],
            "message": f"Review {selected['scope_display_name']}: {selected['label']} has breached its stated threshold.",
            "advisory_only": True,
        }

    incomplete = [
        (scope, kpi)
        for scope in scopes
        for kpi in scope["kpis"]
        if kpi["data_completeness"]["status"] == "unavailable"
    ]
    if incomplete:
        scope, kpi = incomplete[0]
        return {
            "kind": "data_gap",
            "report_date": report_date.isoformat(),
            "scope_id": scope["scope_id"],
            "scope_display_name": scope["display_name"],
            "kpi_id": kpi["id"],
            "label": kpi["label"],
            "message": f"Review data completeness for {scope['display_name']}: {kpi['label']} is unavailable.",
            "advisory_only": True,
        }
    return {
        "kind": "no_exception",
        "report_date": report_date.isoformat(),
        "scope_id": "enterprise",
        "scope_display_name": "All branches",
        "kpi_id": None,
        "label": "No Phase 1 exception",
        "message": "No Phase 1 KPI threshold breach requires attention for this reporting date.",
        "advisory_only": True,
    }


def _attendance_parts(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if payload is None:
        return {"present": None, "rostered": None, "absence_leave": None, "warnings": []}
    items = payload.get("items")
    counts: dict[str, int] = {}
    if isinstance(items, list) and items:
        for item in items:
            if not isinstance(item, Mapping):
                continue
            status = str(item.get("status") or item.get("attendance_status") or "unknown").strip().lower()
            counts[status] = counts.get(status, 0) + 1
        present_count = float(counts.get("present", 0))
        half_day_count = float(counts.get("present_half", 0))
        present = present_count + (half_day_count * 0.5)
        absence_leave = float(counts.get("absent", 0) + counts.get("sick", 0) + counts.get("leave", 0))
        unknown_count = sum(count for status, count in counts.items() if status not in {"present", "present_half", "absent", "sick", "leave", "off"})
        rostered = present_count + half_day_count + absence_leave + unknown_count
        warnings = [f"attendance_unknown_status_count:{unknown_count}"] if unknown_count else []
        return {"present": present, "rostered": rostered, "absence_leave": absence_leave, "warnings": warnings}

    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}
    present = _first_number(metrics.get("effective_active_count"), metrics.get("present_count"), metrics.get("active_count"))
    absent = _first_number(metrics.get("absent_count"), metrics.get("absent"), 0.0)
    sick = _first_number(metrics.get("sick_count"), metrics.get("sick"), 0.0)
    leave = _first_number(metrics.get("leave_count"), metrics.get("annual_leave_count"), metrics.get("leave"), 0.0)
    absence_leave = None if any(value is None for value in (absent, sick, leave)) else absent + sick + leave
    rostered = None if present is None or absence_leave is None else present + absence_leave
    return {"present": present, "rostered": rostered, "absence_leave": absence_leave, "warnings": []}


def _history_values(
    series: Mapping[date, Mapping[str, Mapping[str, Any]]],
    kpi_id: str,
    report_date: date,
    days: int,
) -> list[float]:
    values = []
    for offset in range(1, days + 1):
        value = (series.get(report_date - timedelta(days=offset), {}).get(kpi_id) or {}).get("value")
        if value is not None:
            values.append(float(value))
    return values


def _load_json(path: Path, *, branch: str, report_date: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    payload_date = payload.get("report_date") or payload.get("date")
    if payload_date is not None and payload_date != report_date:
        return None
    payload_branch = payload.get("branch_slug") or payload.get("branch")
    if payload_branch is not None:
        canonical = canonical_branch_slug_or_none(str(payload_branch))
        if canonical is not None and canonical != branch:
            return None
    return dict(payload)


def _source_paths(root: Path, direct_path: Path, payload: Mapping[str, Any] | None) -> list[str]:
    if payload is None:
        return []
    paths = [_relative_path(root, direct_path)]
    source_records = payload.get("source_records")
    if isinstance(source_records, Mapping):
        paths.extend(str(value) for value in source_records.values() if value)
    return paths


def _quality_warnings(payload: Mapping[str, Any] | None) -> list[str]:
    if payload is None:
        return []
    warnings: list[str] = []
    status = payload.get("status")
    if status not in (None, "ready", "accepted", "accepted_split"):
        warnings.append(f"record_status:{status}")
    raw_warnings = payload.get("warnings")
    if isinstance(raw_warnings, list):
        for warning in raw_warnings:
            if isinstance(warning, Mapping):
                code = warning.get("code")
                if code:
                    warnings.append(f"source_warning:{code}")
    return warnings


def _empty_observation() -> dict[str, Any]:
    return {
        "value": None,
        "numerator": None,
        "denominator": None,
        "source_records": [],
        "missing_inputs": ["record_missing"],
        "quality_warnings": [],
        "sources_present": {},
        "observed_days": 0,
        "coverage_complete": False,
    }


def _missing(*items: tuple[str, float | None]) -> list[str]:
    missing = [name for name, value in items if value is None]
    for name, value in items:
        if value == 0 and name in {"labour_hours", "door_count", "active_staff", "sales", "rostered_active"}:
            missing.append(f"{name}:zero_denominator")
    return missing


def _number(payload: Mapping[str, Any] | None, key: str) -> float | None:
    return _to_number(payload.get(key)) if payload is not None else None


def _nested_number(payload: Mapping[str, Any] | None, parent: str, key: str) -> float | None:
    if payload is None or not isinstance(payload.get(parent), Mapping):
        return None
    return _to_number(payload[parent].get(key))


def _to_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_number(*values: Any) -> float | None:
    for value in values:
        number = _to_number(value)
        if number is not None:
            return number
    return None


def _relative_gap(current: float, boundary: float, *, lower_is_bad: bool) -> float:
    if boundary == 0:
        adverse_gap = (boundary - current) if lower_is_bad else (current - boundary)
        return 100.0 if adverse_gap > 0 else 0.0
    gap = (boundary - current) if lower_is_bad else (current - boundary)
    return max(0.0, gap / abs(boundary) * 100)


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 4)


def _relative_path(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _iso_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ValueError("report_date must be ISO YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise ValueError("report_date must be ISO YYYY-MM-DD")
    return parsed


__all__ = ["CONTRACT_VERSION", "build_phase1_dashboard"]
