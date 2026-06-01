"""Helpers for intelligence-only record payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import re
from typing import Any

INTELLIGENCE_REPORT_FAMILY = "intelligence"
SUPERVISOR_CONTROL_REPORT_TYPE = "supervisor_control"
_CHECKLIST_SIGNAL_KEYS = {
    "cash_variance": "Cash_Variance",
    "staffing_issues": "Staffing_Issues",
    "stock_issues": "Stock_Issues",
    "pricing_or_system_issues": "Pricing_System_Issues",
    "exceptions_escalated": "Exceptions",
}
_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")


def build_supervisor_control_intelligence_record(
    *,
    branch: str | None,
    report_date: str | None,
    supervisor: str | None,
    supervisor_confirmation: str | None,
    raw_text: str | None,
    confidence: float,
    source_message_id: str | None,
    sender_phone: str | None,
    created_at: str | None,
    source_agent: str,
    source: str,
    signal_weight: float,
    sop_compliance: str,
    status: str,
    metrics: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
    notes: Sequence[str],
    warnings: Sequence[Mapping[str, Any]],
    branch_text: str | None,
) -> dict[str, Any]:
    """Build the canonical supervisor-control intelligence payload."""

    item_payloads = [dict(item) for item in items if isinstance(item, Mapping)]
    warning_payloads = [dict(warning) for warning in warnings if isinstance(warning, Mapping)]
    note_list = [note for note in notes if isinstance(note, str)]

    cash_variance, cash_variance_detail = _checklist_signal_state(item_payloads, "cash_variance")
    staffing_issues, staffing_issues_detail = _checklist_signal_state(item_payloads, "staffing_issues")
    stock_issues, stock_issues_detail = _checklist_signal_state(item_payloads, "stock_issues")
    pricing_or_system_issues, pricing_or_system_issues_detail = _checklist_signal_state(
        item_payloads,
        "pricing_or_system_issues",
    )
    exceptions_escalated, exceptions_escalated_detail = _checklist_signal_state(item_payloads, "exceptions_escalated")
    if exceptions_escalated is None:
        exceptions_escalated = _escalation_flag(metrics)

    key_values = {
        "Supervisor": supervisor,
        "Supervisor confirmation": supervisor_confirmation,
        "Cash variance": _display_checklist_value(cash_variance, cash_variance_detail),
        "Staffing issues": _display_checklist_value(staffing_issues, staffing_issues_detail),
        "Stock issues affecting sales": _display_checklist_value(stock_issues, stock_issues_detail),
        "Pricing or system issues": _display_checklist_value(
            pricing_or_system_issues,
            pricing_or_system_issues_detail,
        ),
        "Exceptions escalated": _display_checklist_value(exceptions_escalated, exceptions_escalated_detail),
    }
    checklist = [
        f"{label}: {value}"
        for label, value in (
            ("Cash variance", _display_checklist_value(cash_variance, cash_variance_detail)),
            ("Staffing issues", _display_checklist_value(staffing_issues, staffing_issues_detail)),
            ("Stock issues affecting sales", _display_checklist_value(stock_issues, stock_issues_detail)),
            (
                "Pricing or system issues",
                _display_checklist_value(pricing_or_system_issues, pricing_or_system_issues_detail),
            ),
            ("Exceptions escalated", _display_checklist_value(exceptions_escalated, exceptions_escalated_detail)),
        )
        if value is not None
    ]

    return {
        "signal_type": SUPERVISOR_CONTROL_REPORT_TYPE,
        "report_family": INTELLIGENCE_REPORT_FAMILY,
        "report_type": SUPERVISOR_CONTROL_REPORT_TYPE,
        "source_agent": source_agent,
        "source": source,
        "branch": branch,
        "report_date": report_date,
        "supervisor": supervisor,
        "cash_variance": cash_variance,
        "cash_variance_detail": cash_variance_detail,
        "staffing_issues": staffing_issues,
        "staffing_issues_detail": staffing_issues_detail,
        "stock_issues": stock_issues,
        "stock_issues_detail": stock_issues_detail,
        "stock_issues_affecting_sales": stock_issues,
        "stock_issues_affecting_sales_detail": stock_issues_detail,
        "pricing_or_system_issues": pricing_or_system_issues,
        "pricing_or_system_issues_detail": pricing_or_system_issues_detail,
        "exceptions_escalated": exceptions_escalated,
        "exceptions_escalated_detail": exceptions_escalated_detail,
        "supervisor_confirmation": supervisor_confirmation,
        "raw_text": raw_text,
        "confidence": confidence,
        "source_message_id": source_message_id,
        "sender_phone": sender_phone,
        "created_at": created_at or _utc_timestamp(),
        "signal_weight": signal_weight,
        "sop_compliance": sop_compliance,
        "status": status,
        "metrics": dict(metrics),
        "items": item_payloads,
        "notes": note_list,
        "checklist": checklist,
        "key_values": {label: value for label, value in key_values.items() if value is not None},
        "provenance": {
            "branch_text": branch_text,
            "detected_subtype": SUPERVISOR_CONTROL_REPORT_TYPE,
            "supervisor": supervisor,
            "supervisor_confirmation": supervisor_confirmation,
            "notes": note_list,
        },
        "warnings": warning_payloads,
    }


def _checklist_signal_state(
    items: Sequence[Mapping[str, Any]],
    signal_name: str,
) -> tuple[bool | None, str | None]:
    """Return the normalized checklist boolean and any preserved detail text."""

    expected_key = _CHECKLIST_SIGNAL_KEYS[signal_name]
    for item in items:
        action_taken = _string_or_none(item.get("action_taken"))
        details = _string_or_none(item.get("details"))
        detail_key, detail_value = _detail_key_value(details)
        if action_taken != expected_key and detail_key != expected_key:
            continue
        if detail_value is not None:
            normalized_detail = _boolean_checklist_value(detail_value)
            if normalized_detail is not None:
                return normalized_detail, None
            return True, detail_value
        supervisor_confirmed = _string_or_none(item.get("supervisor_confirmed"))
        normalized_confirmation = _boolean_checklist_value(supervisor_confirmed)
        if normalized_confirmation is not None:
            return normalized_confirmation, None
    return None, None


def _detail_key_value(details: str | None) -> tuple[str | None, str | None]:
    if details is None or ":" not in details:
        return None, None
    key, value = details.split(":", 1)
    return _string_or_none(key), _string_or_none(value)


def _boolean_checklist_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    text = _string_or_none(value)
    if text is None:
        return None
    normalized = _normalize_value_token(text)
    if normalized in {"yes", "y", "true", "present", "issue", "issues"}:
        return True
    if normalized in {"no", "n", "false", "none", "nil", "na", "n a", "no issue", "no issues", "nothing"}:
        return False
    return None


def _display_checklist_value(value: bool | None, detail: str | None = None) -> str | None:
    if detail is not None:
        return detail
    if value is True:
        return "YES"
    if value is False:
        return "NO"
    return None


def _escalation_flag(metrics: Mapping[str, Any]) -> bool | None:
    value = metrics.get("escalated_count")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return value > 0


def _string_or_none(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_value_token(value: str) -> str:
    """Return one compact comparison token for tolerant supervisor checklist values."""

    return " ".join(_NON_ALPHANUMERIC_PATTERN.sub(" ", value.casefold()).split())


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
