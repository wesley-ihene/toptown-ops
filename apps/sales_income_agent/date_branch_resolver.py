"""Date and branch normalization helpers for sales reports."""

from __future__ import annotations

from apps.sales_income_agent.field_mapper import canonical_branch_slug
from packages.normalization.branches import normalize_branch
from packages.normalization.dates import normalize_report_date as normalize_strict_report_date


def normalize_report_date(raw_value: str) -> str | None:
    """Return an ISO date when the input matches a supported format."""

    return normalize_strict_report_date(raw_value).normalized_value


def resolve_branch(raw_value: str) -> tuple[str, str | None]:
    """Return the display branch and canonical branch slug."""

    branch = raw_value.strip()
    normalized = normalize_branch(branch)
    return branch, normalized.normalized_value
