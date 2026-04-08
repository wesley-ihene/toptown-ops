"""Date and branch normalization helpers for sales reports."""

from __future__ import annotations

from datetime import datetime
import re

from apps.sales_income_agent.field_mapper import canonical_branch_slug

_WEEKDAY_PREFIX_PATTERN = re.compile(
    r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+",
    flags=re.IGNORECASE,
)


def normalize_report_date(raw_value: str) -> str:
    """Return an ISO date when the input matches a supported format."""

    cleaned = _WEEKDAY_PREFIX_PATTERN.sub("", raw_value.strip())
    for pattern in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y", "%d-%m-%y", "%d-%m-%Y"):
        try:
            return datetime.strptime(cleaned, pattern).date().isoformat()
        except ValueError:
            continue
    return cleaned


def resolve_branch(raw_value: str) -> tuple[str, str]:
    """Return the display branch and canonical branch slug."""

    branch = raw_value.strip()
    return branch, canonical_branch_slug(branch)
