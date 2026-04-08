"""Canonical mapping helpers for HR branch and section wording."""

from __future__ import annotations

from typing import Final

from apps.hr_agent.normalizer import normalize_text
from packages.branch_registry import canonical_branch_slug as canonical_upstream_branch_slug
from packages.section_registry import resolve_section_alias

SECTION_ALIASES: Final[dict[str, tuple[str, ...]]] = {}


def canonical_branch_slug(value: str) -> str:
    """Return a canonical branch slug from a free-form branch value."""

    return canonical_upstream_branch_slug(value)


def canonical_section_name(value: str) -> str | None:
    """Return a canonical section name for a wording variant when recognized."""

    match = resolve_section_alias(value)
    if match is not None:
        return match.section_slug

    normalized = normalize_text(value)
    if normalized in SECTION_ALIASES:
        return normalized
    return None
