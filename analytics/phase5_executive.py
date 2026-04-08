"""Deterministic Phase 5 CEO summaries over existing analytics JSON outputs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any

from packages.common.analytics_loader import (
    analytics_root,
    build_catalog,
    canonical_branch_or_none,
    display_branch_name,
    load_branch_analytics,
    load_branch_comparison,
)
from packages.common.paths import REPO_ROOT
from packages.record_store.writer import ensure_directory, write_json_file, write_text_file

LOW_CONVERSION_RATE_THRESHOLD = 0.5
LOW_SALES_PER_ACTIVE_STAFF_THRESHOLD = 250.0
HIGH_WARNING_DENSITY_THRESHOLD = 4
IDLE_ACTIVITY_SCORE_THRESHOLD = 0.0
NEEDS_REVIEW_STATUSES = {"needs_review"}
WARNING_REVIEW_STATUSES = {"accepted_with_warning"}
ACTIVE_DUTY_STATUSES = {"on_duty", "present"}
SEVERITIES = ("critical", "warning")
EXECUTIVE_ALERTS_DIRNAME = "executive_alerts"


def build_ceo_catalog(*, root: str | None = None) -> dict[str, Any]:
    """Return available CEO dates and branches from analytics outputs."""

    catalog = build_catalog(root=root)
    return {
        "available_dates": catalog.get("available_comparison_dates") or catalog.get("available_dates") or [],
        "available_branches": catalog.get("available_branches") or [],
    }


def build_ceo_branches(report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build per-branch CEO scorecards for one date from analytics outputs."""

    comparison_payload, not_found = load_branch_comparison(report_date=report_date, root=root)
    if comparison_payload is None:
        assert not_found is not None
        return None, not_found.as_dict()

    scorecards = comparison_payload.get("branch_scorecards")
    scorecards = [row for row in scorecards if isinstance(row, Mapping)] if isinstance(scorecards, list) else []
    branches = []
    for scorecard in scorecards:
        branch = _string_or_none(scorecard.get("branch"))
        if branch is None:
            continue

        branch_daily, _ = load_branch_analytics("branch_daily", branch=branch, report_date=report_date, root=root)
        staff_daily, _ = load_branch_analytics("staff", branch=branch, report_date=report_date, root=root)
        section_daily, _ = load_branch_analytics("section", branch=branch, report_date=report_date, root=root)

        sales_input_missing = _sales_input_missing(scorecard=scorecard, branch_daily=branch_daily)
        staff_input_missing = _staff_input_missing(scorecard=scorecard, branch_daily=branch_daily)
        record_statuses = {
            "sales": _nested(branch_daily, "traceability", "sales_status"),
            "staff": _nested(branch_daily, "traceability", "staff_status"),
        }

        warning_count = _to_int(scorecard.get("warning_count"))
        flag_count = _to_int(scorecard.get("flag_count"))
        data_completeness = {
            "branch_daily_present": branch_daily is not None,
            "staff_daily_present": staff_daily is not None,
            "section_daily_present": section_daily is not None,
            "sales_input_present": not sales_input_missing,
            "staff_input_present": not staff_input_missing,
        }
        branches.append(
            {
                "branch": branch,
                "branch_display_name": display_branch_name(branch),
                "gross_sales": _to_number(scorecard.get("gross_sales")),
                "traffic": _to_number(branch_daily.get("traffic")) if isinstance(branch_daily, Mapping) else None,
                "served": _to_number(branch_daily.get("served")) if isinstance(branch_daily, Mapping) else None,
                "operational_score": _to_number(scorecard.get("operational_score")),
                "conversion_rate": _to_number(scorecard.get("conversion_rate")),
                "active_staff_count": _to_number(scorecard.get("active_staff_count")),
                "sales_per_active_staff": (_to_number(branch_daily.get("sales_per_active_staff")) if isinstance(branch_daily, Mapping) else None)
                or _to_number(scorecard.get("sales_per_active_staff")),
                "staff_productivity_index": _to_number(scorecard.get("staff_productivity_index")),
                "warning_count": warning_count,
                "flag_count": flag_count,
                "missing_input_indicators": {
                    "sales_input_missing": sales_input_missing,
                    "staff_input_missing": staff_input_missing,
                },
                "data_completeness": data_completeness,
                "record_statuses": record_statuses,
                "readiness_status": _readiness_status(
                    sales_input_missing=sales_input_missing,
                    staff_input_missing=staff_input_missing,
                    branch_daily_present=branch_daily is not None,
                    staff_daily_present=staff_daily is not None,
                    section_daily_present=section_daily is not None,
                    record_statuses=record_statuses,
                    warning_count=warning_count,
                    flag_count=flag_count,
                ),
                "source_records": {
                    "branch_daily": _source_record(branch_daily, "branch_daily"),
                    "staff_daily": _source_record(staff_daily, "staff_daily"),
                    "section_daily": _source_record(section_daily, "section_daily"),
                    "branch_comparison": f"analytics/branch_comparison/{report_date}.json",
                },
            }
        )

    return {
        "report_date": report_date,
        "branches": branches,
        "branches_reporting_count": len(branches),
        "branches_missing_sales_count": sum(1 for branch in branches if branch["missing_input_indicators"]["sales_input_missing"]),
        "branches_missing_staff_count": sum(1 for branch in branches if branch["missing_input_indicators"]["staff_input_missing"]),
        "branch_comparison_warning_count": len(comparison_payload.get("warnings") or []),
    }, None


def build_ceo_staff(report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build cross-branch CEO staff summaries for one date."""

    branch_payload, error = build_ceo_branches(report_date, root=root)
    if branch_payload is None:
        return None, error

    staff_rows = []
    missing_branches = []
    for branch in branch_payload["branches"]:
        branch_name = branch["branch"]
        staff_daily, _ = load_branch_analytics("staff", branch=branch_name, report_date=report_date, root=root)
        if staff_daily is None:
            missing_branches.append(branch_name)
            continue
        rows = staff_daily.get("top_activity_score")
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            staff_rows.append(
                {
                    "branch": branch_name,
                    "branch_display_name": display_branch_name(branch_name),
                    "staff_name": row.get("staff_name"),
                    "section": row.get("section"),
                    "duty_status": row.get("duty_status"),
                    "items_moved": _to_number(row.get("items_moved")) or 0,
                    "assisting_count": _to_number(row.get("assisting_count")) or 0,
                    "activity_score": _to_number(row.get("activity_score")) or 0,
                }
            )

    top_activity_staff = _sorted_staff(staff_rows, "activity_score", reverse=True)[:10]
    top_items_staff = _sorted_staff(staff_rows, "items_moved", reverse=True)[:10]
    top_assisting_staff = _sorted_staff(staff_rows, "assisting_count", reverse=True)[:10]
    on_duty_rows = [row for row in staff_rows if _string_or_none(row.get("duty_status")) in ACTIVE_DUTY_STATUSES]
    weakest_on_duty_staff = _sorted_staff(on_duty_rows, "activity_score", reverse=False)[:10]
    idle_on_duty_staff = [
        row
        for row in weakest_on_duty_staff
        if float(row.get("activity_score") or 0) <= IDLE_ACTIVITY_SCORE_THRESHOLD
        or ((row.get("items_moved") or 0) == 0 and (row.get("assisting_count") or 0) == 0)
    ]
    unresolved_section_staff = [row for row in staff_rows if row.get("section") in (None, "")]

    return {
        "report_date": report_date,
        "top_activity_staff": top_activity_staff,
        "top_items_staff": top_items_staff,
        "top_assisting_staff": top_assisting_staff,
        "best_staff": top_activity_staff[0] if top_activity_staff else None,
        "weakest_staff": weakest_on_duty_staff[0] if weakest_on_duty_staff else None,
        "weakest_on_duty_staff": weakest_on_duty_staff,
        "idle_on_duty_staff": idle_on_duty_staff,
        "unresolved_section_staff": unresolved_section_staff,
        "branches_missing_staff_daily": missing_branches,
    }, None


def build_ceo_sections(report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build cross-branch CEO section summaries for one date."""

    branch_payload, error = build_ceo_branches(report_date, root=root)
    if branch_payload is None:
        return None, error

    sections = []
    unresolved_hotspots = []
    for branch in branch_payload["branches"]:
        branch_name = branch["branch"]
        section_daily, _ = load_branch_analytics("section", branch=branch_name, report_date=report_date, root=root)
        if section_daily is None:
            continue
        for section in section_daily.get("sections") or []:
            if not isinstance(section, Mapping):
                continue
            sections.append(
                {
                    "branch": branch_name,
                    "branch_display_name": display_branch_name(branch_name),
                    "section": section.get("section"),
                    "productivity_index": _to_number(section.get("productivity_index")),
                    "staff_count": _to_number(section.get("staff_count")),
                    "items_moved": _to_number(section.get("items_moved")),
                    "assisting_count": _to_number(section.get("assisting_count")),
                }
            )

        unresolved = section_daily.get("unresolved_section_tracking")
        unresolved_count = _to_int(_nested(section_daily, "unresolved_section_tracking", "count"))
        if unresolved_count > 0:
            unresolved_hotspots.append(
                {
                    "branch": branch_name,
                    "branch_display_name": display_branch_name(branch_name),
                    "count": unresolved_count,
                    "examples": unresolved.get("examples") if isinstance(unresolved, Mapping) else [],
                }
            )

    strongest_sections = _sorted_sections(sections, reverse=True)[:10]
    weakest_sections = _sorted_sections(sections, reverse=False)[:10]

    return {
        "report_date": report_date,
        "strongest_sections": strongest_sections,
        "weakest_sections": weakest_sections,
        "strongest_section": strongest_sections[0] if strongest_sections else None,
        "weakest_section": weakest_sections[0] if weakest_sections else None,
        "unresolved_section_hotspots": sorted(unresolved_hotspots, key=lambda row: (-row["count"], row["branch"])),
    }, None


def build_ceo_alerts(report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build deterministic CEO alerts for one date from analytics outputs."""

    branch_payload, error = build_ceo_branches(report_date, root=root)
    if branch_payload is None:
        return None, error
    staff_payload, _ = build_ceo_staff(report_date, root=root)
    section_payload, _ = build_ceo_sections(report_date, root=root)

    alerts: list[dict[str, Any]] = []
    idle_by_branch = Counter(row["branch"] for row in (staff_payload or {}).get("idle_on_duty_staff", []))
    unresolved_by_branch = {
        row["branch"]: row["count"]
        for row in (section_payload or {}).get("unresolved_section_hotspots", [])
    }

    for branch in branch_payload["branches"]:
        branch_name = branch["branch"]
        source_records = branch["source_records"]
        report_statuses = branch["record_statuses"]

        if branch["missing_input_indicators"]["sales_input_missing"]:
            alerts.append(
                _alert(
                    "critical",
                    "branch_missing_sales_input",
                    branch_name,
                    report_date,
                    "branch_daily",
                    "Sales input is missing for this branch/date.",
                    source_records,
                )
            )
        if branch["missing_input_indicators"]["staff_input_missing"]:
            alerts.append(
                _alert(
                    "critical",
                    "branch_missing_staff_input",
                    branch_name,
                    report_date,
                    "branch_daily",
                    "Staff input is missing for this branch/date.",
                    source_records,
                )
            )

        if any(status in NEEDS_REVIEW_STATUSES for status in report_statuses.values() if isinstance(status, str)):
            alerts.append(
                _alert(
                    "warning",
                    "record_needs_review",
                    branch_name,
                    report_date,
                    "branch_daily",
                    "Branch analytics reference records still marked needs_review.",
                    source_records,
                )
            )

        if any(status in WARNING_REVIEW_STATUSES for status in report_statuses.values() if isinstance(status, str)):
            alerts.append(
                _alert(
                    "warning",
                    "record_accepted_with_warning",
                    branch_name,
                    report_date,
                    "branch_daily",
                    "Branch analytics reference records accepted with warning.",
                    source_records,
                )
            )

        conversion_rate = _to_number(branch.get("conversion_rate"))
        if conversion_rate is not None and conversion_rate < LOW_CONVERSION_RATE_THRESHOLD:
            alerts.append(
                _alert(
                    "warning",
                    "low_conversion_rate",
                    branch_name,
                    report_date,
                    "branch_daily",
                    f"Conversion rate is {conversion_rate:.2f}, below the CEO threshold.",
                    source_records,
                )
            )

        sales_per_active_staff = _to_number(branch.get("sales_per_active_staff"))
        if sales_per_active_staff is not None and sales_per_active_staff < LOW_SALES_PER_ACTIVE_STAFF_THRESHOLD:
            alerts.append(
                _alert(
                    "warning",
                    "low_sales_per_active_staff",
                    branch_name,
                    report_date,
                    "branch_daily",
                    f"Sales per active staff is {sales_per_active_staff:.2f}, below the CEO threshold.",
                    source_records,
                )
            )

        if idle_by_branch.get(branch_name, 0) > 0:
            alerts.append(
                _alert(
                    "warning",
                    "idle_on_duty_staff",
                    branch_name,
                    report_date,
                    "staff_daily",
                    f"{idle_by_branch[branch_name]} on-duty staff record(s) appear idle.",
                    source_records,
                )
            )

        unresolved_count = unresolved_by_branch.get(branch_name, 0)
        if unresolved_count > 0:
            alerts.append(
                _alert(
                    "warning",
                    "unresolved_sections_present",
                    branch_name,
                    report_date,
                    "section_daily",
                    f"{unresolved_count} unresolved section hotspot(s) remain for this branch.",
                    source_records,
                )
            )

        if (branch.get("warning_count") or 0) >= HIGH_WARNING_DENSITY_THRESHOLD or (branch.get("flag_count") or 0) >= HIGH_WARNING_DENSITY_THRESHOLD:
            alerts.append(
                _alert(
                    "warning",
                    "high_warning_density",
                    branch_name,
                    report_date,
                    "branch_comparison",
                    "Warning or flag density is high for this branch.",
                    source_records,
                )
            )

        if branch["readiness_status"] == "data_gap":
            alerts.append(
                _alert(
                    "critical",
                    "branch_data_missing",
                    branch_name,
                    report_date,
                    "branch_comparison",
                    "Branch readiness is degraded because required analytics inputs are missing.",
                    source_records,
                )
            )

    deduped = _dedupe_alerts(alerts)
    severity_counts = {severity: sum(1 for item in deduped if item["severity"] == severity) for severity in SEVERITIES}
    return {
        "report_date": report_date,
        "count_by_severity": severity_counts,
        "critical_alerts": [item for item in deduped if item["severity"] == "critical"],
        "warning_alerts": [item for item in deduped if item["severity"] == "warning"],
        "alerts": deduped,
    }, None


def format_ceo_alerts_whatsapp(payload: Mapping[str, Any]) -> str:
    """Return a deterministic WhatsApp-ready executive alerts message."""

    report_date = _string_or_none(payload.get("report_date")) or "unknown-date"
    critical_count = _to_int(_nested(payload, "count_by_severity", "critical"))
    warning_count = _to_int(_nested(payload, "count_by_severity", "warning"))
    alerts = payload.get("alerts")
    alert_rows = [row for row in alerts if isinstance(row, Mapping)] if isinstance(alerts, list) else []

    lines = [
        f"TOPTOWN EXECUTIVE ALERTS {report_date}",
        f"Critical: {critical_count} | Warning: {warning_count}",
    ]
    if not alert_rows:
        lines.append("No executive alerts.")
        return "\n".join(lines)

    for index, alert in enumerate(alert_rows, start=1):
        severity = (_string_or_none(alert.get("severity")) or "warning").upper()
        branch = _string_or_none(alert.get("branch_display_name")) or _string_or_none(alert.get("branch")) or "Unknown"
        message = _string_or_none(alert.get("message")) or "No alert message."
        lines.append(f"{index}. [{severity}] {branch}: {message}")
    return "\n".join(lines)


def get_ceo_alerts_path(report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the canonical JSON alert artifact path."""

    return analytics_root(output_root) / EXECUTIVE_ALERTS_DIRNAME / f"{report_date}.json"


def get_ceo_alerts_whatsapp_path(report_date: str, *, output_root: str | Path | None = None) -> Path:
    """Return the canonical WhatsApp text alert artifact path."""

    return analytics_root(output_root) / EXECUTIVE_ALERTS_DIRNAME / f"{report_date}.whatsapp.txt"


def write_ceo_alerts_json(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Write the deterministic JSON executive alerts artifact."""

    path = get_ceo_alerts_path(str(payload["report_date"]), output_root=output_root)
    _ensure_writable(path, overwrite=overwrite)
    return write_json_file(path, dict(payload))


def write_ceo_alerts_whatsapp(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Write the deterministic WhatsApp-ready executive alerts artifact."""

    path = get_ceo_alerts_whatsapp_path(str(payload["report_date"]), output_root=output_root)
    _ensure_writable(path, overwrite=overwrite)
    return write_text_file(path, format_ceo_alerts_whatsapp(payload) + "\n")


def load_ceo_alerts_artifact(
    report_date: str,
    *,
    root: str | Path | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load the file-backed JSON executive alerts artifact when present."""

    path = get_ceo_alerts_path(report_date, output_root=root)
    if not path.exists():
        return None, _alert_artifact_not_found("alerts", report_date, path, root=root)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        return None, _alert_artifact_not_found("alerts", report_date, path, root=root)
    return dict(payload), None


def load_ceo_alerts_whatsapp_artifact(
    report_date: str,
    *,
    root: str | Path | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load the file-backed WhatsApp executive alerts artifact when present."""

    path = get_ceo_alerts_whatsapp_path(report_date, output_root=root)
    if not path.exists():
        return None, _alert_artifact_not_found("alerts_whatsapp", report_date, path, root=root)
    return {
        "report_date": report_date,
        "format": "whatsapp",
        "message": path.read_text(encoding="utf-8").rstrip("\n"),
        "artifact_path": _display_path(path, root=root),
    }, None


def build_ceo_overview(report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build the CEO overview payload for a selected date."""

    branch_payload, error = build_ceo_branches(report_date, root=root)
    if branch_payload is None:
        return None, error
    alerts_payload, _ = build_ceo_alerts(report_date, root=root)
    branches = branch_payload["branches"]

    payload = {
        "report_date": report_date,
        "total_gross_sales": _sum_numbers(branches, "gross_sales"),
        "total_active_staff": _sum_numbers(branches, "active_staff_count"),
        "total_traffic": _sum_numbers(branches, "traffic"),
        "total_served": _sum_numbers(branches, "served"),
        "branches_reporting_count": branch_payload["branches_reporting_count"],
        "branches_missing_sales_count": branch_payload["branches_missing_sales_count"],
        "branches_missing_staff_count": branch_payload["branches_missing_staff_count"],
        "top_branch_by_operational_score": _top_branch(branches, "operational_score"),
        "weakest_branch_by_operational_score": _bottom_branch(branches, "operational_score"),
        "top_branch_by_sales": _top_branch(branches, "gross_sales"),
        "top_branch_by_conversion": _top_branch(branches, "conversion_rate"),
        "summary_warning_counts": {
            "warning_count": _sum_numbers(branches, "warning_count"),
            "flag_count": _sum_numbers(branches, "flag_count"),
            "critical_alert_count": len((alerts_payload or {}).get("critical_alerts", [])),
            "warning_alert_count": len((alerts_payload or {}).get("warning_alerts", [])),
        },
        "branch_coverage": {
            "available_branch_count": len(build_ceo_catalog(root=root)["available_branches"]),
            "branches_reporting_count": branch_payload["branches_reporting_count"],
            "branches_with_data_gaps": [branch["branch"] for branch in branches if branch["readiness_status"] == "data_gap"],
        },
        "data_gaps": {
            "branches_missing_sales_input": [branch["branch"] for branch in branches if branch["missing_input_indicators"]["sales_input_missing"]],
            "branches_missing_staff_input": [branch["branch"] for branch in branches if branch["missing_input_indicators"]["staff_input_missing"]],
        },
        "branch_scorecards": branches,
        "alerts_snapshot": {
            "critical_alerts": (alerts_payload or {}).get("critical_alerts", []),
            "warning_alerts": (alerts_payload or {}).get("warning_alerts", []),
        },
    }
    return payload, None


def build_ceo_dashboard(branch: str | None, report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build the combined CEO dashboard payload for a selected date and optional branch."""

    overview, error = build_ceo_overview(report_date, root=root)
    if overview is None:
        return None, error
    branches_payload, _ = build_ceo_branches(report_date, root=root)
    staff_payload, _ = build_ceo_staff(report_date, root=root)
    sections_payload, _ = build_ceo_sections(report_date, root=root)
    alerts_payload, _ = build_ceo_alerts(report_date, root=root)

    selected_branch = canonical_branch_or_none(branch) or _string_or_none(_nested(overview, "top_branch_by_operational_score", "branch"))
    selected_branch_scorecard = None
    if branches_payload is not None and selected_branch is not None:
        for scorecard in branches_payload["branches"]:
            if scorecard["branch"] == selected_branch:
                selected_branch_scorecard = scorecard
                break

    return {
        "report_date": report_date,
        "selected_branch": selected_branch,
        "overview": overview,
        "branches": branches_payload,
        "staff": staff_payload,
        "sections": sections_payload,
        "alerts": alerts_payload,
        "selected_branch_scorecard": selected_branch_scorecard,
    }, None


# Backward-compatible wrappers for the previous executive scaffold.
def build_executive_overview(report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    return build_ceo_overview(report_date, root=root)


def build_branch_executive_summary(branch: str, report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    branches, error = build_ceo_branches(report_date, root=root)
    if branches is None:
        return None, error
    target = canonical_branch_or_none(branch)
    for item in branches["branches"]:
        if item["branch"] == target:
            return {
                "branch": item["branch"],
                "report_date": report_date,
                "branch_daily": {
                    "gross_sales": item["gross_sales"],
                    "conversion_rate": item["conversion_rate"],
                    "sales_per_active_staff": item["sales_per_active_staff"],
                    "active_staff_count": item["active_staff_count"],
                },
                "scorecard": item,
            }, None
    return None, {
        "error": "analytics_not_found",
        "message": "No analytics output matched the requested filters.",
        "product": "branch",
        "branch": target,
        "report_date": report_date,
        "expected_path": f"analytics/branch_daily/{target}/{report_date}.json" if target else None,
    }


def build_staff_executive_summary(branch: str, report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    payload, not_found = load_branch_analytics("staff", branch=branch, report_date=report_date, root=root)
    if payload is None:
        assert not_found is not None
        return None, not_found.as_dict()
    return {
        "branch": payload.get("branch"),
        "report_date": payload.get("report_date"),
        "summary_counts": payload.get("summary_counts"),
        "top_items_moved": (payload.get("top_items_moved") or [])[:5],
        "top_assisting": (payload.get("top_assisting") or [])[:5],
        "top_activity_score": (payload.get("top_activity_score") or [])[:5],
        "lowest_productivity": (payload.get("lowest_productivity") or [])[:5],
        "role_summaries": payload.get("role_summaries") or [],
        "duty_status_summaries": payload.get("duty_status_summaries") or [],
    }, None


def build_section_executive_summary(branch: str, report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    payload, not_found = load_branch_analytics("section", branch=branch, report_date=report_date, root=root)
    if payload is None:
        assert not_found is not None
        return None, not_found.as_dict()
    return {
        "branch": payload.get("branch"),
        "report_date": payload.get("report_date"),
        "section_count": len(payload.get("sections") or []),
        "top_sections": (payload.get("sections") or [])[:8],
        "unresolved_section_tracking": payload.get("unresolved_section_tracking") or {},
    }, None


def build_executive_alerts(report_date: str, *, root: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    return build_ceo_alerts(report_date, root=root)


def _sales_input_missing(*, scorecard: Mapping[str, Any], branch_daily: Mapping[str, Any] | None) -> bool:
    if branch_daily is not None:
        source_flag = _nested(branch_daily, "sources", "sales_income")
        if isinstance(source_flag, bool):
            return not source_flag
    if _nested(scorecard, "source_records", "sales_income") is None and _to_number(scorecard.get("gross_sales")) is None:
        return True
    return False


def _staff_input_missing(*, scorecard: Mapping[str, Any], branch_daily: Mapping[str, Any] | None) -> bool:
    if branch_daily is not None:
        source_flag = _nested(branch_daily, "sources", "hr_performance")
        if isinstance(source_flag, bool):
            return not source_flag
    if _nested(scorecard, "source_records", "hr_performance") is None and _to_number(scorecard.get("active_staff_count")) is None:
        return True
    return False


def _readiness_status(
    *,
    sales_input_missing: bool,
    staff_input_missing: bool,
    branch_daily_present: bool,
    staff_daily_present: bool,
    section_daily_present: bool,
    record_statuses: Mapping[str, Any],
    warning_count: int,
    flag_count: int,
) -> str:
    if sales_input_missing or staff_input_missing or not branch_daily_present or not staff_daily_present or not section_daily_present:
        return "data_gap"
    statuses = {value for value in record_statuses.values() if isinstance(value, str)}
    if statuses & NEEDS_REVIEW_STATUSES:
        return "needs_review"
    if statuses & WARNING_REVIEW_STATUSES or warning_count > 0 or flag_count > 0:
        return "attention_required"
    return "ready"


def _source_record(payload: Mapping[str, Any] | None, product: str) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    records = payload.get("source_records")
    if not isinstance(records, Mapping):
        return None
    if product == "branch_daily":
        return _string_or_none(records.get("sales_income")) or _string_or_none(records.get("hr_performance"))
    if product == "staff_daily":
        return _string_or_none(records.get("hr_performance"))
    if product == "section_daily":
        return _string_or_none(records.get("hr_performance"))
    return None


def _alert(
    severity: str,
    code: str,
    branch: str,
    report_date: str,
    source_product: str,
    message: str,
    source_records: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
        "branch": branch,
        "branch_display_name": display_branch_name(branch),
        "report_date": report_date,
        "source_product": source_product,
        "traceability": {
            "branch": branch,
            "report_date": report_date,
            "source_product": source_product,
            "source_records": dict(source_records),
        },
    }


def _dedupe_alerts(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    deduped = []
    for alert in alerts:
        key = (alert.get("severity"), alert.get("code"), alert.get("branch"), alert.get("report_date"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(alert)
    deduped.sort(key=lambda item: (_severity_rank(item.get("severity")), str(item.get("branch")), str(item.get("code"))))
    return deduped


def _sorted_staff(rows: list[dict[str, Any]], metric: str, *, reverse: bool) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            float(row.get(metric) or 0),
            float(row.get("activity_score") or 0),
            str(row.get("staff_name") or ""),
            str(row.get("branch") or ""),
        ),
        reverse=reverse,
    )


def _sorted_sections(rows: list[dict[str, Any]], *, reverse: bool) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            float(row.get("productivity_index") or 0),
            str(row.get("section") or ""),
            str(row.get("branch") or ""),
        ),
        reverse=reverse,
    )


def _top_branch(branches: list[Mapping[str, Any]], metric: str) -> dict[str, Any] | None:
    candidates = [branch for branch in branches if _to_number(branch.get(metric)) is not None]
    if not candidates:
        return None
    row = sorted(candidates, key=lambda item: (float(item.get(metric) or 0), str(item.get("branch") or "")), reverse=True)[0]
    return {"branch": row.get("branch"), metric: row.get(metric)}


def _bottom_branch(branches: list[Mapping[str, Any]], metric: str) -> dict[str, Any] | None:
    candidates = [branch for branch in branches if _to_number(branch.get(metric)) is not None]
    if not candidates:
        return None
    row = sorted(candidates, key=lambda item: (float(item.get(metric) or 0), str(item.get("branch") or "")))[0]
    return {"branch": row.get("branch"), metric: row.get(metric)}


def _sum_numbers(rows: list[Mapping[str, Any]], key: str) -> float | int | None:
    total = 0.0
    found = False
    for row in rows:
        value = _to_number(row.get(key))
        if value is None:
            continue
        total += value
        found = True
    if not found:
        return None
    return int(total) if total.is_integer() else round(total, 2)


def _nested(mapping: Mapping[str, Any] | None, key: str, child: str) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    block = mapping.get(key)
    if isinstance(block, Mapping):
        return block.get(child)
    return None


def _to_number(value: Any) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    return None


def _to_int(value: Any) -> int:
    number = _to_number(value)
    return int(number) if number is not None else 0


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _severity_rank(value: Any) -> int:
    if value == "critical":
        return 0
    if value == "warning":
        return 1
    return 9


def _ensure_writable(path: Path, *, overwrite: bool) -> None:
    ensure_directory(path.parent)
    if path.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite existing executive alerts file: {path}")


def _alert_artifact_not_found(product: str, report_date: str, path: Path, *, root: str | Path | None) -> dict[str, Any]:
    return {
        "error": "analytics_not_found",
        "message": "No analytics output matched the requested filters.",
        "product": product,
        "branch": None,
        "report_date": report_date,
        "expected_path": _display_path(path, root=root),
    }


def _display_path(path: Path, *, root: str | Path | None = None) -> str:
    base_root = Path(root) if root is not None else REPO_ROOT
    try:
        return str(path.relative_to(base_root))
    except ValueError:
        return str(path)
