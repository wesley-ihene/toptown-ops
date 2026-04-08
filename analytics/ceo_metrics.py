"""Production-safe CEO analytics over existing structured signals."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
import argparse
import json
from typing import Any

from apps.hr_agent.field_mapper import canonical_branch_slug
from apps.hr_agent.staff_identity import normalize_staff_name
from packages.common.paths import REPO_ROOT
from packages.record_store.naming import safe_segment
from packages.record_store.reader import read_structured
from packages.record_store.writer import ensure_directory, write_json_file, write_text_file

LOW_CONVERSION_RATE_THRESHOLD = 0.5
LOW_SALES_PER_STAFF_THRESHOLD = 250.0
LOW_STOCK_VELOCITY_THRESHOLD = 0.75
STOCK_SHORTAGE_VELOCITY_THRESHOLD = 1.25
HIGH_STAFF_PRESENT_THRESHOLD = 10
LOW_STAFF_PRESENT_THRESHOLD = 3
LOW_REVENUE_PER_STAFF_THRESHOLD = 250.0
HIGH_REVENUE_PER_STAFF_THRESHOLD = 500.0
STRONG_SALES_THRESHOLD = 1000.0

_ISO_DATE_FORMAT = "%Y-%m-%d"
_SIGNAL_RECORD_TYPES: dict[str, str] = {
    "sales": "sales_income",
    "staff": "hr_performance",
    "attendance": "hr_attendance",
    "pricing": "pricing_stock_release",
}


def safe_div(numerator, denominator, default=None):
    """
    Return numerator / denominator when denominator is valid and non-zero.
    Otherwise return default.
    """

    if numerator is None or denominator in (None, 0):
        return default
    try:
        return numerator / denominator
    except ZeroDivisionError:
        return default
    except TypeError:
        return default


def compute_productivity(sales_signal, staff_signal=None, attendance_signal=None):
    """
    Inputs:
    - sales_signal: structured daily sales record
    - staff_signal: optional structured staff performance data for same branch/date
    - attendance_signal: optional structured attendance data for same branch/date

    Return dict with:
    {
      "sales_per_staff": ...,
      "sales_per_customer": ...,
      "conversion_rate": ...,
      "efficiency_index": ...,
      "staff_count_used": ...,
      "data_quality": {
        "sales_present": bool,
        "attendance_present": bool,
        "staff_present": bool
      }
    }
    """

    sales = _extract_sales_total(sales_signal)
    traffic = _extract_sales_traffic(sales_signal)
    served = _extract_sales_served(sales_signal)
    conversion_rate = _round_metric(
        _first_valid(
            safe_div(served, traffic),
            _extract_sales_conversion_rate_fallback(sales_signal),
        ),
        4,
    )

    attendance_count = _attendance_present_count(attendance_signal)
    explicit_staff_on_duty = _extract_number(
        sales_signal,
        "metrics.staff_on_duty",
        "staff_on_duty",
        "totals.staff_on_duty",
    )
    performance_count = _count_active_staff_from_performance(staff_signal)

    # Precedence is conservative and explicit:
    # 1) attendance-derived present count
    # 2) sales signal staff-on-duty field
    # 3) countable staff performance entries
    staff_count_used = _first_valid(attendance_count, explicit_staff_on_duty, performance_count)

    sales_per_customer = _round_metric(safe_div(sales, served), 2)
    sales_per_staff = _round_metric(safe_div(sales, staff_count_used), 2)
    efficiency_index = _round_metric(
        safe_div(
            (sales_per_staff * conversion_rate) if sales_per_staff is not None and conversion_rate is not None else None,
            1,
        ),
        4,
    )

    return {
        "sales_per_staff": sales_per_staff,
        "sales_per_customer": sales_per_customer,
        "conversion_rate": conversion_rate,
        "efficiency_index": efficiency_index,
        "staff_count_used": _round_count(staff_count_used),
        "traffic": _round_count(traffic),
        "served": _round_count(served),
        "data_quality": {
            "sales_present": isinstance(sales_signal, Mapping),
            "attendance_present": isinstance(attendance_signal, Mapping),
            "staff_present": isinstance(staff_signal, Mapping),
        },
    }


def compute_stock_velocity(sales_signal, pricing_signal):
    """
    Inputs:
    - sales_signal: structured daily sales record
    - pricing_signal: structured pricing stock release / bale release record

    Return dict with:
    {
      "released_qty": ...,
      "released_value": ...,
      "sales": ...,
      "velocity_ratio": ...,
      "avg_value_per_released_item": ...,
      "data_quality": {
        "sales_present": bool,
        "pricing_present": bool
      }
    }
    """

    sales = _extract_sales_total(sales_signal)
    released_qty = _extract_pricing_total_qty(pricing_signal)
    released_value = _extract_pricing_total_amount(pricing_signal)
    velocity_ratio = _round_metric(
        None if released_value in (None, 0) else safe_div(sales, released_value),
        4,
    )
    avg_value_per_released_item = _round_metric(safe_div(released_value, released_qty), 2)

    return {
        "released_qty": _round_count(released_qty),
        "released_value": _round_metric(released_value, 2),
        "sales": _round_metric(sales, 2),
        "velocity_ratio": velocity_ratio,
        "avg_value_per_released_item": avg_value_per_released_item,
        "data_quality": {
            "sales_present": isinstance(sales_signal, Mapping),
            "pricing_present": isinstance(pricing_signal, Mapping),
        },
    }


def compute_payroll_efficiency(sales_signal, attendance_signal, staff_signal=None):
    """
    Return dict with:
    {
      "staff_present": ...,
      "active_staff": ...,
      "utilization_ratio": ...,
      "revenue_per_staff": ...,
      "labor_efficiency_flag": ...,
      "data_quality": {
        "sales_present": bool,
        "attendance_present": bool,
        "staff_present": bool
      }
    }
    """

    sales = _extract_sales_total(sales_signal)
    staff_present = _attendance_present_count(attendance_signal)
    active_staff = _count_active_staff_from_performance(staff_signal)
    utilization_ratio = _round_metric(safe_div(active_staff, staff_present), 4)
    revenue_per_staff = _round_metric(safe_div(sales, staff_present), 2)
    labor_efficiency_flag = _derive_labor_efficiency_flag(
        staff_present=staff_present,
        revenue_per_staff=revenue_per_staff,
    )

    return {
        "staff_present": _round_count(staff_present),
        "active_staff": _round_count(active_staff),
        "utilization_ratio": utilization_ratio,
        "revenue_per_staff": revenue_per_staff,
        "labor_efficiency_flag": labor_efficiency_flag,
        "data_quality": {
            "sales_present": isinstance(sales_signal, Mapping),
            "attendance_present": isinstance(attendance_signal, Mapping),
            "staff_present": isinstance(staff_signal, Mapping),
        },
    }


def load_sales_signal(branch, report_date, root=None):
    return _load_signal("sales", branch, report_date, root=root)


def load_staff_signal(branch, report_date, root=None):
    return _load_signal("staff", branch, report_date, root=root)


def load_attendance_signal(branch, report_date, root=None):
    return _load_signal("attendance", branch, report_date, root=root)


def load_pricing_signal(branch, report_date, root=None):
    return _load_signal("pricing", branch, report_date, root=root)


def build_ceo_summary(branch, report_date, root=None):
    """
    Loads structured signals and returns:
    {
      "branch": "...",
      "date": "YYYY-MM-DD",
      "sources": {
        "sales": bool,
        "staff": bool,
        "attendance": bool,
        "pricing": bool
      },
      "productivity": {...},
      "stock_velocity": {...},
      "payroll_efficiency": {...},
      "alerts": [...]
    }
    """

    canonical_branch = _canonical_branch(branch)
    iso_date = _validate_iso_date(report_date)
    sales_signal = load_sales_signal(canonical_branch, iso_date, root=root)
    staff_signal = load_staff_signal(canonical_branch, iso_date, root=root)
    attendance_signal = load_attendance_signal(canonical_branch, iso_date, root=root)
    pricing_signal = load_pricing_signal(canonical_branch, iso_date, root=root)

    summary = {
        "branch": canonical_branch,
        "date": iso_date,
        "sources": {
            "sales": sales_signal is not None,
            "staff": staff_signal is not None,
            "attendance": attendance_signal is not None,
            "pricing": pricing_signal is not None,
        },
        "productivity": compute_productivity(
            sales_signal=sales_signal,
            staff_signal=staff_signal,
            attendance_signal=attendance_signal,
        ),
        "stock_velocity": compute_stock_velocity(
            sales_signal=sales_signal,
            pricing_signal=pricing_signal,
        ),
        "payroll_efficiency": compute_payroll_efficiency(
            sales_signal=sales_signal,
            attendance_signal=attendance_signal,
            staff_signal=staff_signal,
        ),
    }
    summary["alerts"] = generate_alerts(summary)
    return summary


def generate_alerts(summary_dict):
    alerts: list[dict[str, str]] = []
    productivity = summary_dict.get("productivity") or {}
    stock_velocity = summary_dict.get("stock_velocity") or {}
    payroll = summary_dict.get("payroll_efficiency") or {}

    traffic = _extract_number(productivity, "traffic")
    conversion_rate = _extract_number(productivity, "conversion_rate")
    sales_per_staff = _extract_number(productivity, "sales_per_staff")
    if traffic is not None and conversion_rate is not None and conversion_rate < LOW_CONVERSION_RATE_THRESHOLD:
        alerts.append(
            {
                "family": "productivity",
                "code": "conversion_issue",
                "message": "Traffic converted weakly relative to store footfall.",
            }
        )
    if sales_per_staff is not None and sales_per_staff < LOW_SALES_PER_STAFF_THRESHOLD:
        alerts.append(
            {
                "family": "productivity",
                "code": "labor_productivity_issue",
                "message": "Sales per staff member is below the review threshold.",
            }
        )

    velocity_ratio = _extract_number(stock_velocity, "velocity_ratio")
    stock_sales = _extract_number(stock_velocity, "sales")
    released_value = _extract_number(stock_velocity, "released_value")
    if velocity_ratio is not None and velocity_ratio < LOW_STOCK_VELOCITY_THRESHOLD:
        alerts.append(
            {
                "family": "stock_velocity",
                "code": "slow_stock_movement",
                "message": "Sales are converting released stock value slowly.",
            }
        )
    if (
        stock_sales is not None
        and stock_sales >= STRONG_SALES_THRESHOLD
        and velocity_ratio is not None
        and velocity_ratio >= STOCK_SHORTAGE_VELOCITY_THRESHOLD
        and released_value is not None
    ):
        alerts.append(
            {
                "family": "stock_velocity",
                "code": "stock_shortage_risk",
                "message": "Sales are strong relative to released stock value.",
            }
        )

    staff_present = _extract_number(payroll, "staff_present")
    revenue_per_staff = _extract_number(payroll, "revenue_per_staff")
    payroll_sales = _extract_sales_from_summary(summary_dict)
    if (
        staff_present is not None
        and staff_present >= HIGH_STAFF_PRESENT_THRESHOLD
        and revenue_per_staff is not None
        and revenue_per_staff < LOW_REVENUE_PER_STAFF_THRESHOLD
    ):
        alerts.append(
            {
                "family": "payroll_efficiency",
                "code": "labor_inefficiency",
                "message": "Headcount is high for the revenue generated.",
            }
        )
    if (
        staff_present is not None
        and staff_present <= LOW_STAFF_PRESENT_THRESHOLD
        and payroll_sales is not None
        and payroll_sales >= STRONG_SALES_THRESHOLD
    ):
        alerts.append(
            {
                "family": "payroll_efficiency",
                "code": "understaffed_risk",
                "message": "Sales are strong relative to the small present headcount.",
            }
        )
    return alerts


def write_ceo_summary_json(summary, output_root=None, overwrite: bool = False):
    path = _ceo_report_path(summary, output_root=output_root, suffix=".json")
    _ensure_writable(path, overwrite=overwrite)
    return write_json_file(path, dict(summary))


def write_ceo_summary_markdown(summary, output_root=None, overwrite: bool = False):
    path = _ceo_report_path(summary, output_root=output_root, suffix=".md")
    _ensure_writable(path, overwrite=overwrite)
    return write_text_file(path, _render_markdown_summary(summary))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build CEO metrics from structured signals.")
    parser.add_argument("--branch", required=True, help="Canonical branch slug or recognized branch label.")
    parser.add_argument("--date", required=True, help="ISO report date in YYYY-MM-DD format.")
    parser.add_argument("--root", help="Repository root or test fixture root override.")
    parser.add_argument("--write-json", action="store_true", help="Write JSON summary to REPORTS/ceo.")
    parser.add_argument("--write-markdown", action="store_true", help="Write Markdown summary to REPORTS/ceo.")
    parser.add_argument("--print-json", action="store_true", help="Print summary JSON to stdout.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting existing report files.")
    args = parser.parse_args(argv)

    try:
        summary = build_ceo_summary(branch=args.branch, report_date=args.date, root=args.root)
    except ValueError as error:
        parser.error(str(error))

    if args.write_json:
        json_path = write_ceo_summary_json(summary, output_root=args.root, overwrite=args.overwrite)
        print(f"Wrote JSON summary to {json_path}")
    if args.write_markdown:
        markdown_path = write_ceo_summary_markdown(summary, output_root=args.root, overwrite=args.overwrite)
        print(f"Wrote Markdown summary to {markdown_path}")

    if args.print_json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    elif not args.write_json and not args.write_markdown:
        print(_render_markdown_summary(summary), end="")

    return 0


def _load_signal(signal_key: str, branch: str, report_date: str, root: str | Path | None = None) -> dict[str, Any] | None:
    canonical_branch = _canonical_branch(branch)
    iso_date = _validate_iso_date(report_date)
    payload = read_structured(
        _SIGNAL_RECORD_TYPES[signal_key],
        canonical_branch,
        iso_date,
        root=root,
    )
    if payload is None:
        return None
    if not _payload_matches_request(payload, canonical_branch=canonical_branch, report_date=iso_date):
        return None
    return payload


def _payload_matches_request(
    payload: Mapping[str, Any],
    *,
    canonical_branch: str,
    report_date: str,
) -> bool:
    branch_candidates = [
        payload.get("branch_slug"),
        payload.get("branch"),
    ]
    branch_matches = any(
        isinstance(value, str) and _canonical_branch(value) == canonical_branch
        for value in branch_candidates
        if value
    )
    if not branch_matches:
        return False

    payload_date = payload.get("report_date")
    if not isinstance(payload_date, str):
        return None
    try:
        return _validate_iso_date(payload_date) == report_date
    except ValueError:
        return None


def _extract_sales_total(signal: Mapping[str, Any] | None) -> float | None:
    return _extract_number(
        signal,
        "totals.sales",
        "metrics.gross_sales",
        "metrics.sales",
        "sales",
    )


def _extract_sales_traffic(signal: Mapping[str, Any] | None) -> float | None:
    return _extract_number(
        signal,
        "customers.traffic",
        "metrics.traffic",
        "traffic",
    )


def _extract_sales_served(signal: Mapping[str, Any] | None) -> float | None:
    return _extract_number(
        signal,
        "customers.served",
        "metrics.served",
        "served",
        "customers.customer_count",
        "metrics.customer_count",
    )


def _extract_sales_conversion_rate_fallback(signal: Mapping[str, Any] | None) -> float | None:
    return _extract_number(
        signal,
        "performance.conversion_rate",
        "metrics.conversion_rate",
        "conversion_rate",
    )


def _extract_pricing_total_qty(signal: Mapping[str, Any] | None) -> float | None:
    explicit_total = _extract_number(
        signal,
        "totals.total_qty",
        "totals.total_quantity",
        "totals.quantity",
        "metrics.total_qty",
        "metrics.total_quantity",
        "metrics.quantity",
        "total_qty",
        "quantity",
    )
    if explicit_total is not None:
        return explicit_total
    return _sum_item_numbers(signal, ("qty", "quantity"))


def _extract_pricing_total_amount(signal: Mapping[str, Any] | None) -> float | None:
    explicit_total = _extract_number(
        signal,
        "totals.total_amount",
        "totals.total_value",
        "totals.amount",
        "metrics.total_amount",
        "metrics.total_value",
        "metrics.amount",
        "total_amount",
        "amount",
    )
    if explicit_total is not None:
        return explicit_total
    return _sum_item_numbers(signal, ("amount", "value"))


def _attendance_present_count(signal: Mapping[str, Any] | None) -> float | None:
    explicit_count = _extract_number(
        signal,
        "metrics.present_count",
        "metrics.staff_present",
        "present_count",
        "staff_present",
    )
    if explicit_count is not None:
        return explicit_count
    items = _extract_items(signal)
    if items is None:
        return None
    present_count = 0
    found_countable_row = False
    for item in items:
        if not isinstance(item, Mapping):
            continue
        status = item.get("status")
        if isinstance(status, str):
            found_countable_row = True
            if status == "present":
                present_count += 1
                continue
        presence_score = item.get("presence_score")
        if isinstance(presence_score, (int, float)):
            found_countable_row = True
            if presence_score > 0:
                present_count += 1
    if not found_countable_row:
        return None
    return float(present_count)


def _count_active_staff_from_performance(signal: Mapping[str, Any] | None) -> float | None:
    items = _extract_items(signal)
    if items is None:
        return None
    names: set[str] = set()
    saw_staff_name = False
    for item in items:
        if not isinstance(item, Mapping):
            continue
        staff_name = item.get("staff_name")
        if not isinstance(staff_name, str):
            continue
        normalized_name = normalize_staff_name(staff_name)
        if not normalized_name:
            continue
        saw_staff_name = True
        duty_status = item.get("duty_status")
        # Off-duty performance rows are excluded to keep the count aligned with active daily staff.
        if isinstance(duty_status, str) and duty_status == "off_duty":
            continue
        names.add(normalized_name)
    if not saw_staff_name:
        return None
    return float(len(names))


def _derive_labor_efficiency_flag(*, staff_present: float | None, revenue_per_staff: float | None) -> str:
    if staff_present is None or revenue_per_staff is None:
        return "UNKNOWN"
    if revenue_per_staff >= HIGH_REVENUE_PER_STAFF_THRESHOLD:
        return "HIGH"
    if staff_present >= HIGH_STAFF_PRESENT_THRESHOLD and revenue_per_staff < LOW_REVENUE_PER_STAFF_THRESHOLD:
        return "LOW"
    return "NORMAL"


def _extract_sales_from_summary(summary: Mapping[str, Any]) -> float | None:
    stock_velocity = summary.get("stock_velocity")
    if isinstance(stock_velocity, Mapping):
        sales = _extract_number(stock_velocity, "sales")
        if sales is not None:
            return sales
    return None


def _extract_number(source: Mapping[str, Any] | None, *paths: str) -> float | None:
    if not isinstance(source, Mapping):
        return None
    for path in paths:
        value = _get_path(source, path)
        number = _coerce_number(value)
        if number is not None:
            return number
    return None


def _sum_item_numbers(signal: Mapping[str, Any] | None, keys: tuple[str, ...]) -> float | None:
    items = _extract_items(signal)
    if items is None:
        return None
    total = 0.0
    found_value = False
    for item in items:
        if not isinstance(item, Mapping):
            continue
        item_number = None
        for key in keys:
            item_number = _coerce_number(item.get(key))
            if item_number is not None:
                break
        if item_number is None:
            continue
        total += item_number
        found_value = True
    if not found_value:
        return None
    return total


def _extract_items(signal: Mapping[str, Any] | None) -> list[Mapping[str, Any]] | None:
    if not isinstance(signal, Mapping):
        return None
    items = signal.get("items")
    if isinstance(items, Sequence) and not isinstance(items, (str, bytes, bytearray)):
        return [item for item in items if isinstance(item, Mapping)]
    return None


def _get_path(source: Mapping[str, Any], path: str) -> Any:
    current: Any = source
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _first_valid(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _round_metric(value: float | None, digits: int) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _round_count(value: float | None) -> int | float | None:
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    return round(value, 2)


def _canonical_branch(branch: str) -> str:
    return canonical_branch_slug(branch).strip()


def _validate_iso_date(value: str) -> str:
    try:
        return datetime.strptime(value, _ISO_DATE_FORMAT).date().isoformat()
    except ValueError as error:
        raise ValueError(f"Invalid ISO date {value!r}; expected YYYY-MM-DD.") from error


def _ceo_report_path(summary: Mapping[str, Any], *, output_root: str | Path | None, suffix: str) -> Path:
    branch = summary.get("branch")
    date = summary.get("date")
    if not isinstance(branch, str) or not branch:
        raise ValueError("Summary branch is required for report writing.")
    if not isinstance(date, str) or not date:
        raise ValueError("Summary date is required for report writing.")
    _validate_iso_date(date)
    base_root = Path(output_root) if output_root is not None else REPO_ROOT
    reports_dir = base_root / "REPORTS" / "ceo" / safe_segment(branch)
    ensure_directory(reports_dir)
    return reports_dir / f"{date}_ceo_summary{suffix}"


def _ensure_writable(path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"{path} already exists; pass overwrite=True to replace it.")


def _render_markdown_summary(summary: Mapping[str, Any]) -> str:
    lines = [
        f"# CEO Summary: {summary['branch']} ({summary['date']})",
        "",
        "## Sources available",
    ]
    for key in ("sales", "staff", "attendance", "pricing"):
        lines.append(f"- {key}: {_format_bool(summary['sources'].get(key))}")

    lines.extend(
        [
            "",
            "## Productivity",
            _metric_line("Sales per staff", summary["productivity"].get("sales_per_staff")),
            _metric_line("Sales per customer", summary["productivity"].get("sales_per_customer")),
            _metric_line("Conversion rate", summary["productivity"].get("conversion_rate")),
            _metric_line("Efficiency index", summary["productivity"].get("efficiency_index")),
            _metric_line("Staff count used", summary["productivity"].get("staff_count_used")),
            "",
            "## Stock Velocity",
            _metric_line("Released quantity", summary["stock_velocity"].get("released_qty")),
            _metric_line("Released value", summary["stock_velocity"].get("released_value")),
            _metric_line("Sales", summary["stock_velocity"].get("sales")),
            _metric_line("Velocity ratio", summary["stock_velocity"].get("velocity_ratio")),
            _metric_line(
                "Average value per released item",
                summary["stock_velocity"].get("avg_value_per_released_item"),
            ),
            "",
            "## Payroll Efficiency",
            _metric_line("Staff present", summary["payroll_efficiency"].get("staff_present")),
            _metric_line("Active staff", summary["payroll_efficiency"].get("active_staff")),
            _metric_line("Utilization ratio", summary["payroll_efficiency"].get("utilization_ratio")),
            _metric_line("Revenue per staff", summary["payroll_efficiency"].get("revenue_per_staff")),
            _metric_line(
                "Labor efficiency flag",
                summary["payroll_efficiency"].get("labor_efficiency_flag"),
            ),
            "",
            "## Alerts",
        ]
    )
    alerts = summary.get("alerts") or []
    if alerts:
        for alert in alerts:
            if isinstance(alert, Mapping):
                lines.append(f"- [{alert.get('family')}] {alert.get('message')}")
    else:
        lines.append("- None")
    return "\n".join(lines) + "\n"


def _metric_line(label: str, value: Any) -> str:
    return f"- {label}: {_format_metric(value)}"


def _format_metric(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return _format_bool(value)
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


def _format_bool(value: Any) -> str:
    return "yes" if bool(value) else "no"


if __name__ == "__main__":
    raise SystemExit(main())
