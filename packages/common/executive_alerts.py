"""File-backed deterministic executive alerts over existing analytics outputs."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any

from analytics.phase5_executive import (
    LOW_CONVERSION_RATE_THRESHOLD,
    LOW_SALES_PER_ACTIVE_STAFF_THRESHOLD,
    build_ceo_branches,
    build_ceo_sections,
    build_ceo_staff,
)
from packages.common.paths import REPO_ROOT
from packages.record_store.writer import ensure_directory, write_json_file, write_text_file

from .whatsapp_alert_formatter import (
    format_executive_alert_branch_whatsapp,
    format_executive_alert_summary_whatsapp,
)

WEAK_BRANCH_OPERATIONAL_SCORE_THRESHOLD = 70
IDLE_ON_DUTY_CRITICAL_COUNT_THRESHOLD = 3
CRITICAL_BRANCH_GAP_THRESHOLD = 3
EXECUTIVE_ALERTS_ROOT = "alerts"
EXECUTIVE_ALERTS_DIRNAME = "executive"
SEVERITIES = ("critical", "warning", "info")


def build_executive_alert_summary(report_date: str, *, root: str | Path | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build the deterministic daily executive alert summary for one date."""

    branches_payload, error = build_ceo_branches(report_date, root=_root_str(root))
    if branches_payload is None:
        return None, error
    staff_payload, _ = build_ceo_staff(report_date, root=_root_str(root))
    sections_payload, _ = build_ceo_sections(report_date, root=_root_str(root))

    idle_staff_by_branch = _idle_staff_by_branch(staff_payload)
    unresolved_by_branch = _unresolved_by_branch(sections_payload)

    branch_artifacts: list[dict[str, Any]] = []
    all_alerts: list[dict[str, Any]] = []
    for branch_row in branches_payload["branches"]:
        branch_artifact = _build_branch_alert_artifact(
            branch_row,
            report_date=report_date,
            idle_staff_names=idle_staff_by_branch.get(branch_row["branch"], []),
            unresolved_section=unresolved_by_branch.get(branch_row["branch"], {"count": 0, "examples": []}),
            root=root,
        )
        branch_artifacts.append(branch_artifact)
        all_alerts.extend(branch_artifact["alerts"])

    all_alerts = _sorted_alerts(_dedupe_alerts(all_alerts))
    counts = _counts_by_severity(all_alerts)
    return {
        "report_date": report_date,
        "counts_by_severity": counts,
        "branch_count": len(branch_artifacts),
        "branches_with_alerts": [artifact["branch"] for artifact in branch_artifacts if artifact["alerts"]],
        "alerts": all_alerts,
        "branch_artifacts": [
            {
                "branch": artifact["branch"],
                "branch_display_name": artifact["branch_display_name"],
                "artifact_path": _display_path(get_executive_alert_branch_path(report_date, artifact["branch"], output_root=root), root=root),
                "whatsapp_path": _display_path(get_executive_alert_branch_whatsapp_path(report_date, artifact["branch"], output_root=root), root=root),
                "counts_by_severity": artifact["counts_by_severity"],
            }
            for artifact in branch_artifacts
        ],
    }, None


def write_executive_alert_artifacts(
    report_date: str,
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Build and write deterministic executive alert artifacts for one date."""

    summary_payload, error = build_executive_alert_summary(report_date, root=output_root)
    if summary_payload is None:
        raise FileNotFoundError(str((error or {}).get("message") or "Executive alerts source data missing."))

    summary_path = get_executive_alert_summary_path(report_date, output_root=output_root)
    _ensure_writable(summary_path, overwrite=overwrite)
    write_json_file(summary_path, summary_payload)

    summary_whatsapp_path = get_executive_alert_summary_whatsapp_path(report_date, output_root=output_root)
    _ensure_writable(summary_whatsapp_path, overwrite=overwrite)
    write_text_file(summary_whatsapp_path, format_executive_alert_summary_whatsapp(summary_payload) + "\n")

    branch_paths: dict[str, str] = {}
    branch_whatsapp_paths: dict[str, str] = {}
    for branch_entry in summary_payload["branch_artifacts"]:
        branch = branch_entry["branch"]
        branch_payload = build_executive_alert_branch(report_date, branch, root=output_root)[0]
        assert branch_payload is not None
        branch_path = get_executive_alert_branch_path(report_date, branch, output_root=output_root)
        _ensure_writable(branch_path, overwrite=overwrite)
        write_json_file(branch_path, branch_payload)
        branch_paths[branch] = str(branch_path)

        branch_whatsapp_path = get_executive_alert_branch_whatsapp_path(report_date, branch, output_root=output_root)
        _ensure_writable(branch_whatsapp_path, overwrite=overwrite)
        write_text_file(branch_whatsapp_path, format_executive_alert_branch_whatsapp(branch_payload) + "\n")
        branch_whatsapp_paths[branch] = str(branch_whatsapp_path)

    return {
        "summary_path": str(summary_path),
        "summary_whatsapp_path": str(summary_whatsapp_path),
        "branch_paths": branch_paths,
        "branch_whatsapp_paths": branch_whatsapp_paths,
    }


def build_executive_alert_branch(
    report_date: str,
    branch: str,
    *,
    root: str | Path | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build the deterministic branch alert artifact for one date."""

    summary_payload, error = build_executive_alert_summary(report_date, root=root)
    if summary_payload is None:
        return None, error
    for branch_entry in summary_payload["branch_artifacts"]:
        if branch_entry["branch"] == branch:
            branch_path = get_executive_alert_branch_path(report_date, branch, output_root=root)
            if branch_path.exists():
                with branch_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                if isinstance(payload, Mapping):
                    return dict(payload), None
            break

    branches_payload, error = build_ceo_branches(report_date, root=_root_str(root))
    if branches_payload is None:
        return None, error
    staff_payload, _ = build_ceo_staff(report_date, root=_root_str(root))
    sections_payload, _ = build_ceo_sections(report_date, root=_root_str(root))
    idle_staff_names = _idle_staff_by_branch(staff_payload).get(branch, [])
    unresolved_section = _unresolved_by_branch(sections_payload).get(branch, {"count": 0, "examples": []})
    for branch_row in branches_payload["branches"]:
        if branch_row["branch"] == branch:
            return _build_branch_alert_artifact(
                branch_row,
                report_date=report_date,
                idle_staff_names=idle_staff_names,
                unresolved_section=unresolved_section,
                root=root,
            ), None
    return None, _not_found("alerts_branch", branch, report_date, get_executive_alert_branch_path(report_date, branch, output_root=root), root=root)


def load_executive_alert_summary(report_date: str, *, root: str | Path | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load the file-backed daily alert summary when present."""

    path = get_executive_alert_summary_path(report_date, output_root=root)
    return _load_json_artifact(path, "alerts", branch=None, report_date=report_date, root=root)


def load_executive_alert_feed(report_date: str, *, root: str | Path | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load the file-backed WhatsApp summary preview when present."""

    path = get_executive_alert_summary_whatsapp_path(report_date, output_root=root)
    if not path.exists():
        return None, _not_found("alerts_feed", None, report_date, path, root=root)
    return {
        "report_date": report_date,
        "format": "whatsapp",
        "artifact_path": _display_path(path, root=root),
        "message": path.read_text(encoding="utf-8").rstrip("\n"),
    }, None


def load_executive_alert_branch(report_date: str, branch: str, *, root: str | Path | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load the file-backed branch alert artifact when present."""

    path = get_executive_alert_branch_path(report_date, branch, output_root=root)
    return _load_json_artifact(path, "alerts_branch", branch=branch, report_date=report_date, root=root)


def get_executive_alert_summary_path(report_date: str, *, output_root: str | Path | None = None) -> Path:
    return _alerts_root(output_root) / report_date / "summary.json"


def get_executive_alert_summary_whatsapp_path(report_date: str, *, output_root: str | Path | None = None) -> Path:
    return _alerts_root(output_root) / report_date / "summary.whatsapp.txt"


def get_executive_alert_branch_path(report_date: str, branch: str, *, output_root: str | Path | None = None) -> Path:
    return _alerts_root(output_root) / report_date / f"{branch}.json"


def get_executive_alert_branch_whatsapp_path(report_date: str, branch: str, *, output_root: str | Path | None = None) -> Path:
    return _alerts_root(output_root) / report_date / f"{branch}.whatsapp.txt"


def _build_branch_alert_artifact(
    branch_row: Mapping[str, Any],
    *,
    report_date: str,
    idle_staff_names: list[str],
    unresolved_section: Mapping[str, Any],
    root: str | Path | None,
) -> dict[str, Any]:
    branch = str(branch_row["branch"])
    branch_display_name = str(branch_row["branch_display_name"])
    source_paths = _source_paths(branch_row, root=root, report_date=report_date)
    alerts: list[dict[str, Any]] = []

    if branch_row["missing_input_indicators"]["sales_input_missing"]:
        alerts.append(
            _make_alert(
                alert_code="branch_missing_sales_input",
                severity="critical",
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="branch_daily",
                source_paths=source_paths,
                message="Sales input is missing for this branch/date.",
                metrics={"sales_input_missing": True},
            )
        )

    if branch_row["missing_input_indicators"]["staff_input_missing"]:
        alerts.append(
            _make_alert(
                alert_code="branch_missing_staff_input",
                severity="critical",
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="branch_daily",
                source_paths=source_paths,
                message="Staff input is missing for this branch/date.",
                metrics={"staff_input_missing": True},
            )
        )

    record_statuses = branch_row.get("record_statuses")
    if isinstance(record_statuses, Mapping):
        needs_review_sources = sorted(key for key, value in record_statuses.items() if value == "needs_review")
        if needs_review_sources:
            alerts.append(
                _make_alert(
                    alert_code="record_needs_review",
                    severity="warning",
                    branch=branch,
                    branch_display_name=branch_display_name,
                    report_date=report_date,
                    source="branch_daily",
                    source_paths=source_paths,
                    message="Branch analytics reference records still marked needs_review.",
                    metrics={"record_statuses": needs_review_sources},
                )
            )

        warning_sources = sorted(key for key, value in record_statuses.items() if value == "accepted_with_warning")
        if warning_sources:
            alerts.append(
                _make_alert(
                    alert_code="record_accepted_with_warning",
                    severity="info",
                    branch=branch,
                    branch_display_name=branch_display_name,
                    report_date=report_date,
                    source="branch_daily",
                    source_paths=source_paths,
                    message="Branch analytics reference records accepted with warning.",
                    metrics={"record_statuses": warning_sources},
                )
            )

    conversion_rate = _to_number(branch_row.get("conversion_rate"))
    if conversion_rate is not None and conversion_rate < LOW_CONVERSION_RATE_THRESHOLD:
        alerts.append(
            _make_alert(
                alert_code="low_conversion_rate",
                severity="warning",
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="branch_daily",
                source_paths=source_paths,
                message=f"Conversion rate is {conversion_rate:.2f}, below the executive threshold.",
                metrics={"conversion_rate": conversion_rate, "threshold": LOW_CONVERSION_RATE_THRESHOLD},
            )
        )

    sales_per_active_staff = _to_number(branch_row.get("sales_per_active_staff"))
    if sales_per_active_staff is not None and sales_per_active_staff < LOW_SALES_PER_ACTIVE_STAFF_THRESHOLD:
        alerts.append(
            _make_alert(
                alert_code="low_sales_per_active_staff",
                severity="warning",
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="branch_daily",
                source_paths=source_paths,
                message=f"Sales per active staff is {sales_per_active_staff:.2f}, below the executive threshold.",
                metrics={"sales_per_active_staff": sales_per_active_staff, "threshold": LOW_SALES_PER_ACTIVE_STAFF_THRESHOLD},
            )
        )

    operational_score = _to_number(branch_row.get("operational_score"))
    if operational_score is not None and operational_score < WEAK_BRANCH_OPERATIONAL_SCORE_THRESHOLD:
        alerts.append(
            _make_alert(
                alert_code="weak_branch_operational_score",
                severity="warning",
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="branch_comparison",
                source_paths=source_paths,
                message=f"Operational score is {int(operational_score)}, below the executive threshold.",
                metrics={"operational_score": operational_score, "threshold": WEAK_BRANCH_OPERATIONAL_SCORE_THRESHOLD},
            )
        )

    unresolved_count = _to_int(unresolved_section.get("count"))
    if unresolved_count > 0:
        alerts.append(
            _make_alert(
                alert_code="unresolved_sections_present",
                severity="warning",
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="section_daily",
                source_paths=source_paths,
                message=f"{unresolved_count} unresolved section hotspot(s) remain for this branch.",
                metrics={
                    "unresolved_section_count": unresolved_count,
                    "examples": _string_list(unresolved_section.get("examples")),
                },
            )
        )

    if idle_staff_names:
        severity = "critical" if len(idle_staff_names) >= IDLE_ON_DUTY_CRITICAL_COUNT_THRESHOLD else "warning"
        alerts.append(
            _make_alert(
                alert_code="idle_on_duty_staff",
                severity=severity,
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="staff_daily",
                source_paths=source_paths,
                message=f"{len(idle_staff_names)} on-duty staff record(s) appear idle.",
                metrics={"idle_staff_count": len(idle_staff_names), "staff_names": idle_staff_names},
            )
        )

    non_info_count = sum(1 for alert in alerts if alert["severity"] in {"critical", "warning"})
    if non_info_count >= CRITICAL_BRANCH_GAP_THRESHOLD or (
        any(alert["severity"] == "critical" for alert in alerts) and non_info_count >= 2
    ):
        alerts.append(
            _make_alert(
                alert_code="critical_branch_gap",
                severity="critical",
                branch=branch,
                branch_display_name=branch_display_name,
                report_date=report_date,
                source="executive_alerts",
                source_paths=source_paths,
                message="Multiple severe operational issues stacked for this branch/date.",
                metrics={
                    "critical_alert_count": sum(1 for alert in alerts if alert["severity"] == "critical"),
                    "warning_alert_count": sum(1 for alert in alerts if alert["severity"] == "warning"),
                },
            )
        )

    alerts = _sorted_alerts(_dedupe_alerts(alerts))
    return {
        "branch": branch,
        "branch_display_name": branch_display_name,
        "report_date": report_date,
        "counts_by_severity": _counts_by_severity(alerts),
        "alerts": alerts,
    }


def _make_alert(
    *,
    alert_code: str,
    severity: str,
    branch: str,
    branch_display_name: str,
    report_date: str,
    source: str,
    source_paths: list[str],
    message: str,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    metric_signature = json.dumps(dict(metrics), sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    dedupe_key = f"{report_date}|{branch}|{alert_code}|{metric_signature}"
    return {
        "alert_code": alert_code,
        "code": alert_code,
        "severity": severity,
        "branch": branch,
        "branch_display_name": branch_display_name,
        "report_date": report_date,
        "message": message,
        "source": source,
        "source_paths": list(source_paths),
        "dedupe_key": dedupe_key,
        "metrics": dict(metrics),
    }


def _source_paths(branch_row: Mapping[str, Any], *, root: str | Path | None, report_date: str) -> list[str]:
    source_records = branch_row.get("source_records")
    paths = {
        f"analytics/branch_daily/{branch_row['branch']}/{report_date}.json",
        f"analytics/staff_daily/{branch_row['branch']}/{report_date}.json",
        f"analytics/section_daily/{branch_row['branch']}/{report_date}.json",
        f"analytics/branch_comparison/{report_date}.json",
    }
    if isinstance(source_records, Mapping):
        for value in source_records.values():
            if isinstance(value, str) and value.strip():
                paths.add(value)
    return sorted(paths)


def _idle_staff_by_branch(staff_payload: Mapping[str, Any] | None) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    rows = staff_payload.get("idle_on_duty_staff") if isinstance(staff_payload, Mapping) else None
    if not isinstance(rows, list):
        return grouped
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        branch = _string_or_none(row.get("branch"))
        staff_name = _string_or_none(row.get("staff_name"))
        if branch and staff_name:
            grouped[branch].append(staff_name)
    for branch, names in grouped.items():
        grouped[branch] = sorted(set(names))
    return grouped


def _unresolved_by_branch(section_payload: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    rows = section_payload.get("unresolved_section_hotspots") if isinstance(section_payload, Mapping) else None
    if not isinstance(rows, list):
        return grouped
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        branch = _string_or_none(row.get("branch"))
        if branch is None:
            continue
        grouped[branch] = {
            "count": _to_int(row.get("count")),
            "examples": _string_list(row.get("examples")),
        }
    return grouped


def _dedupe_alerts(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for alert in alerts:
        seen[str(alert["dedupe_key"])] = alert
    return list(seen.values())


def _sorted_alerts(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        alerts,
        key=lambda item: (_severity_rank(item.get("severity")), str(item.get("branch")), str(item.get("alert_code"))),
    )


def _counts_by_severity(alerts: list[Mapping[str, Any]]) -> dict[str, int]:
    return {severity: sum(1 for alert in alerts if alert.get("severity") == severity) for severity in SEVERITIES}


def _severity_rank(value: Any) -> int:
    if value == "critical":
        return 0
    if value == "warning":
        return 1
    if value == "info":
        return 2
    return 9


def _ensure_writable(path: Path, *, overwrite: bool) -> None:
    ensure_directory(path.parent)
    if path.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite existing executive alerts file: {path}")


def _load_json_artifact(path: Path, product: str, *, branch: str | None, report_date: str, root: str | Path | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not path.exists():
        return None, _not_found(product, branch, report_date, path, root=root)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        return None, _not_found(product, branch, report_date, path, root=root)
    return dict(payload), None


def _not_found(product: str, branch: str | None, report_date: str, path: Path, *, root: str | Path | None) -> dict[str, Any]:
    return {
        "error": "analytics_not_found",
        "message": "No analytics output matched the requested filters.",
        "product": product,
        "branch": branch,
        "report_date": report_date,
        "expected_path": _display_path(path, root=root),
    }


def _alerts_root(root: str | Path | None) -> Path:
    base_root = Path(root) if root is not None else REPO_ROOT
    return base_root / EXECUTIVE_ALERTS_ROOT / EXECUTIVE_ALERTS_DIRNAME


def _display_path(path: Path, *, root: str | Path | None = None) -> str:
    base_root = Path(root) if root is not None else REPO_ROOT
    try:
        return str(path.relative_to(base_root))
    except ValueError:
        return str(path)


def _root_str(root: str | Path | None) -> str | None:
    return str(root) if root is not None else None


def _to_number(value: Any) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def _to_int(value: Any) -> int:
    number = _to_number(value)
    return int(number) if number is not None else 0


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]
