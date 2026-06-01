"""Structured-record persistence helpers for the pricing stock release agent."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from packages.common.branch import canonical_branch_slug_or_none
from packages.common.date import normalize_report_date
from packages.record_store.writer import write_governed_structured

SIGNAL_TYPE = "pricing_stock_release"


def write_structured_record(
    payload: Mapping[str, Any],
    *,
    metadata: Mapping[str, Any] | None = None,
) -> object | None:
    """Persist a valid pricing payload as a canonical structured record."""

    branch = payload.get("branch")
    report_date = payload.get("report_date")
    status = payload.get("status")

    if status == "invalid_input":
        return None
    if not isinstance(branch, str) or not branch.strip():
        return None
    if not isinstance(report_date, str) or not report_date.strip():
        return None

    branch_slug = _canonical_branch_slug(branch)
    normalized_date = _normalize_report_date(report_date)
    persisted_payload = dict(payload)
    if branch_slug is not None:
        persisted_payload["branch_slug"] = branch_slug
    persisted_payload["report_date"] = normalized_date

    return write_governed_structured(
        signal_type=SIGNAL_TYPE,
        branch=branch_slug or branch.strip(),
        date=normalized_date,
        payload=persisted_payload,
        metadata=metadata if isinstance(metadata, dict) else dict(metadata) if isinstance(metadata, Mapping) else None,
    )


def _canonical_branch_slug(branch: str) -> str | None:
    """Return the canonical branch slug used for structured record paths."""

    return canonical_branch_slug_or_none(branch)


def _normalize_report_date(report_date: str) -> str:
    """Return an ISO report date for structured record persistence."""

    return normalize_report_date(report_date)
