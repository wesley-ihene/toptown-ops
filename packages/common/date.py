"""Shared date and branch normalization helpers."""

from __future__ import annotations

from packages.common.branch import canonical_branch_slug
from packages.normalization.branches import normalize_branch
from packages.normalization.dates import normalize_report_date as normalize_strict_report_date


def normalize_report_date(raw_value: str) -> str:
    """Return an ISO date when the input matches a supported format."""

    result = normalize_strict_report_date(raw_value)
    return result.normalized_value or raw_value.strip()


def resolve_branch(raw_value: str) -> tuple[str, str]:
    """Return the display branch and canonical branch slug."""

    branch = raw_value.strip()
    normalized = normalize_branch(branch)
    return branch, normalized.normalized_value or canonical_branch_slug(branch)
