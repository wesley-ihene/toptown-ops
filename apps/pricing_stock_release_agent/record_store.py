"""Structured-record persistence helpers for the pricing stock release agent."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
import re
from typing import Any

from packages.record_store.writer import write_structured

SIGNAL_TYPE = "pricing_stock_release"
_WHITESPACE_PATTERN = re.compile(r"\s+")
_BRANCH_SLUG_ALIASES: dict[str, str] = {
    "waigani": "waigani",
    "waigani branch": "waigani",
    "ttc pom waigani branch": "waigani",
    "ttc waigani branch": "waigani",
    "lae 5th street": "lae_5th_street",
    "lae 5th street branch": "lae_5th_street",
    "ttc 5th street branch": "lae_5th_street",
    "5th street": "lae_5th_street",
    "bena road": "bena_road",
    "bena road branch": "bena_road",
    "ttc bena road branch": "bena_road",
    "lae malaita": "lae_malaita",
    "lae malaita branch": "lae_malaita",
    "ttc lae malaita branch": "lae_malaita",
}


def write_structured_record(payload: Mapping[str, Any]) -> Path | None:
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
    persisted_payload["branch_slug"] = branch_slug
    persisted_payload["report_date"] = normalized_date

    return write_structured(
        signal_type=SIGNAL_TYPE,
        branch=branch_slug,
        date=normalized_date,
        payload=persisted_payload,
    )


def _canonical_branch_slug(branch: str) -> str:
    """Return the canonical branch slug used for structured record paths."""

    normalized = _normalize_text(branch)
    if normalized in _BRANCH_SLUG_ALIASES:
        return _BRANCH_SLUG_ALIASES[normalized]
    return normalized.replace(" ", "_")


def _normalize_report_date(report_date: str) -> str:
    """Return an ISO report date for structured record persistence."""

    cleaned = report_date.strip()
    for pattern in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y", "%d-%m-%y", "%d-%m-%Y"):
        try:
            return datetime.strptime(cleaned, pattern).date().isoformat()
        except ValueError:
            continue
    return cleaned


def _normalize_text(value: str) -> str:
    """Return a comparison-safe normalized string."""

    return _WHITESPACE_PATTERN.sub(" ", value.casefold().replace("_", " ")).strip()
