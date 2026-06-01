"""Shared branch normalization helpers."""

from __future__ import annotations

from packages.normalization.branches import (
    canonical_branch_slug as canonical_upstream_branch_slug,
    canonical_branch_slug_or_none as canonical_upstream_branch_slug_or_none,
    is_canonical_branch_slug as is_upstream_canonical_branch_slug,
)


def canonical_branch_slug(value: str) -> str:
    """Return a canonical branch slug from a free-form branch value."""

    return canonical_upstream_branch_slug(value)


def canonical_branch_slug_or_none(value: str | None) -> str | None:
    """Return a canonical branch slug or ``None`` when the value is not allowed."""

    return canonical_upstream_branch_slug_or_none(value)


def is_canonical_branch_slug(value: str | None) -> bool:
    """Return whether ``value`` is one configured canonical branch slug."""

    return is_upstream_canonical_branch_slug(value)
