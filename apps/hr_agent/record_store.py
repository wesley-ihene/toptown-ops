"""Structured-record persistence helpers for the HR agent."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import re
from typing import Any

from apps.hr_agent.date_branch_resolver import normalize_report_date
from apps.hr_agent.field_mapper import canonical_branch_slug
from packages.record_store.writer import write_governed_structured

_SUBTYPE_TO_RECORD_TYPE = {
    "staff_performance": "hr_performance",
    "staff_attendance": "hr_attendance",
}
_CANONICAL_BRANCH_PATTERN = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def write_structured_record(
    payload: Mapping[str, Any],
    *,
    metadata: Mapping[str, Any] | None = None,
) -> object | None:
    """Persist a valid HR payload as a canonical structured record."""

    signal_type = payload.get("signal_type")
    signal_subtype = payload.get("signal_subtype")
    branch = payload.get("branch")
    report_date = payload.get("report_date")
    status = payload.get("status")

    if signal_subtype == "staff_attendance":
        if signal_type not in {"hr", "hr_staffing"}:
            return None
    elif signal_type != "hr":
        return None
    if status == "invalid_input":
        return None
    if not isinstance(signal_subtype, str) or signal_subtype not in _SUBTYPE_TO_RECORD_TYPE:
        return None
    if not isinstance(branch, str) or not branch.strip():
        return None
    if not isinstance(report_date, str) or not report_date.strip():
        return None

    canonical_branch = _canonical_branch_or_none(branch)
    iso_report_date = _iso_date_or_none(report_date)
    if canonical_branch is None or iso_report_date is None:
        return None

    return write_governed_structured(
        signal_type=_SUBTYPE_TO_RECORD_TYPE[signal_subtype],
        branch=canonical_branch,
        date=iso_report_date,
        payload=dict(payload),
        metadata=dict(metadata) if isinstance(metadata, Mapping) else None,
    )


def _canonical_branch_or_none(branch: str) -> str | None:
    """Return a canonical branch slug suitable for structured record paths."""

    candidate = canonical_branch_slug(branch.strip())
    if not candidate or not _CANONICAL_BRANCH_PATTERN.fullmatch(candidate):
        return None
    return candidate


def _iso_date_or_none(report_date: str) -> str | None:
    """Return an ISO report date suitable for structured record paths."""

    candidate = normalize_report_date(report_date.strip())
    if candidate is None:
        return None
    if not _ISO_DATE_PATTERN.fullmatch(candidate):
        return None
    return candidate
