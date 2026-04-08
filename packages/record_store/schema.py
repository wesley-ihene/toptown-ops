"""Minimal placeholder schemas for structured operational records."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

SALES_INCOME_SCHEMA: dict[str, Any] = {
    "signal_type": "sales_income",
    "branch": "",
    "date": "",
    "source": "whatsapp",
    "payload": {},
}

PRICING_STOCK_RELEASE_SCHEMA: dict[str, Any] = {
    "signal_type": "pricing_stock_release",
    "branch": "",
    "date": "",
    "source": "whatsapp",
    "payload": {},
}

HR_ATTENDANCE_SCHEMA: dict[str, Any] = {
    "signal_type": "hr_attendance",
    "branch": "",
    "date": "",
    "source": "whatsapp",
    "payload": {},
}

HR_PERFORMANCE_SCHEMA: dict[str, Any] = {
    "signal_type": "hr_performance",
    "branch": "",
    "date": "",
    "source": "whatsapp",
    "payload": {},
}

SUPERVISOR_CONTROL_SCHEMA: dict[str, Any] = {
    "signal_type": "supervisor_control",
    "branch": "",
    "date": "",
    "source": "whatsapp",
    "payload": {},
}

RECORD_TEMPLATES: dict[str, dict[str, Any]] = {
    "sales_income": SALES_INCOME_SCHEMA,
    "pricing_stock_release": PRICING_STOCK_RELEASE_SCHEMA,
    "hr_attendance": HR_ATTENDANCE_SCHEMA,
    "hr_performance": HR_PERFORMANCE_SCHEMA,
    "supervisor_control": SUPERVISOR_CONTROL_SCHEMA,
}


def get_record_template(record_type: str) -> dict[str, Any]:
    """Return a deep-copied record template by type."""

    try:
        return deepcopy(RECORD_TEMPLATES[record_type])
    except KeyError as exc:
        raise ValueError(f"Unsupported record type: {record_type}") from exc
