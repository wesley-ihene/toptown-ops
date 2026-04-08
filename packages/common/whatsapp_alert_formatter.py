"""Deterministic WhatsApp-ready formatting for executive alerts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def format_executive_alert_summary_whatsapp(payload: Mapping[str, Any]) -> str:
    """Return a compact WhatsApp-ready daily alert summary."""

    report_date = _string_or_none(payload.get("report_date")) or "unknown-date"
    counts = payload.get("counts_by_severity")
    critical = _to_int(counts.get("critical")) if isinstance(counts, Mapping) else 0
    warning = _to_int(counts.get("warning")) if isinstance(counts, Mapping) else 0
    info = _to_int(counts.get("info")) if isinstance(counts, Mapping) else 0
    alerts = payload.get("alerts")
    alert_rows = [row for row in alerts if isinstance(row, Mapping)] if isinstance(alerts, list) else []

    lines = [
        f"TOPTOWN EXECUTIVE ALERTS {report_date}",
        f"Critical: {critical} | Warning: {warning} | Info: {info}",
    ]
    if not alert_rows:
        lines.append("No executive alerts.")
        return "\n".join(lines)

    for index, alert in enumerate(alert_rows[:8], start=1):
        severity = (_string_or_none(alert.get("severity")) or "warning").upper()
        branch = _string_or_none(alert.get("branch_display_name")) or _string_or_none(alert.get("branch")) or "Unknown"
        message = _string_or_none(alert.get("message")) or "No alert message."
        lines.append(f"{index}. [{severity}] {branch}: {message}")
    if len(alert_rows) > 8:
        lines.append(f"... plus {len(alert_rows) - 8} more alert(s).")
    return "\n".join(lines)


def format_executive_alert_branch_whatsapp(payload: Mapping[str, Any]) -> str:
    """Return a compact WhatsApp-ready branch alert summary."""

    branch = _string_or_none(payload.get("branch_display_name")) or _string_or_none(payload.get("branch")) or "Unknown"
    report_date = _string_or_none(payload.get("report_date")) or "unknown-date"
    counts = payload.get("counts_by_severity")
    critical = _to_int(counts.get("critical")) if isinstance(counts, Mapping) else 0
    warning = _to_int(counts.get("warning")) if isinstance(counts, Mapping) else 0
    info = _to_int(counts.get("info")) if isinstance(counts, Mapping) else 0
    alerts = payload.get("alerts")
    alert_rows = [row for row in alerts if isinstance(row, Mapping)] if isinstance(alerts, list) else []

    lines = [
        f"TOPTOWN BRANCH ALERTS {branch} {report_date}",
        f"Critical: {critical} | Warning: {warning} | Info: {info}",
    ]
    if not alert_rows:
        lines.append("No branch alerts.")
        return "\n".join(lines)

    for index, alert in enumerate(alert_rows[:6], start=1):
        severity = (_string_or_none(alert.get("severity")) or "warning").upper()
        message = _string_or_none(alert.get("message")) or "No alert message."
        lines.append(f"{index}. [{severity}] {message}")
    if len(alert_rows) > 6:
        lines.append(f"... plus {len(alert_rows) - 6} more alert(s).")
    return "\n".join(lines)


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0
