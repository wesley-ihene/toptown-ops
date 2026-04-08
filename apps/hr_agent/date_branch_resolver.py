"""Date and branch normalization helpers for HR reports."""

from __future__ import annotations

from apps.date_resolver_agent.worker import normalize_report_date as normalize_upstream_report_date
from packages.branch_registry import canonical_branch_slug


def normalize_report_date(raw_value: str) -> str:
    """Return an ISO date when the input matches a supported format."""

    return normalize_upstream_report_date(raw_value) or raw_value.strip()


def resolve_branch(raw_value: str) -> tuple[str, str]:
    """Return the display branch and canonical branch slug."""

    branch = raw_value.strip()
    return branch, canonical_branch_slug(branch)
