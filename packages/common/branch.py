"""Shared branch normalization helpers."""

from __future__ import annotations

from packages.branch_registry import canonical_branch_slug as canonical_upstream_branch_slug


def canonical_branch_slug(value: str) -> str:
    """Return a canonical branch slug from a free-form branch value."""

    return canonical_upstream_branch_slug(value)
