"""Structured-record persistence helpers for the supervisor control agent."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from packages.record_store.writer import write_structured

SIGNAL_TYPE = "supervisor_control"


def write_structured_record(payload: Mapping[str, Any]) -> Path | None:
    """Persist a valid supervisor control payload as a canonical structured record."""

    branch = payload.get("branch")
    report_date = payload.get("report_date")
    status = payload.get("status")

    if status == "invalid_input":
        return None
    if not isinstance(branch, str) or not branch.strip():
        return None
    if not isinstance(report_date, str) or not report_date.strip():
        return None

    return write_structured(
        signal_type=SIGNAL_TYPE,
        branch=branch,
        date=report_date,
        payload=dict(payload),
    )
