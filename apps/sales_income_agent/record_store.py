"""Structured-record persistence helpers for the sales income agent."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import re
from typing import Any

from apps.sales_income_agent.date_branch_resolver import normalize_report_date
from packages.branch_registry import canonical_branch_slug
from packages.record_store.writer import write_structured

SIGNAL_TYPE = "sales_income"
_CANONICAL_BRANCH_PATTERN = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def write_structured_record(payload: Mapping[str, Any]) -> Path | None:
    """Persist a valid sales payload as a canonical structured record."""

    branch = payload.get("branch")
    report_date = payload.get("report_date")
    status = payload.get("status")

    if status == "invalid_input":
        return None
    if not isinstance(branch, str) or not branch.strip():
        return None
    if not isinstance(report_date, str) or not report_date.strip():
        return None

    canonical_branch = _canonical_branch_or_none(branch)
    iso_report_date = _iso_date_or_none(report_date)
    if canonical_branch is None or iso_report_date is None:
        return None

    persisted_payload = dict(payload)
    persisted_payload["branch"] = canonical_branch
    persisted_payload["report_date"] = iso_report_date
    return write_structured(
        signal_type=SIGNAL_TYPE,
        branch=canonical_branch,
        date=iso_report_date,
        payload=persisted_payload,
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
