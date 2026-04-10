"""Deterministic branch aliases for upstream intake routing."""

from __future__ import annotations

from packages.normalization.branches import (
    BRANCH_ALIASES,
    CANONICAL_BRANCHES,
    BranchMatch,
    canonical_branch_slug,
    normalize_branch_text,
    resolve_branch_alias,
)

__all__ = [
    "BRANCH_ALIASES",
    "CANONICAL_BRANCHES",
    "BranchMatch",
    "canonical_branch_slug",
    "normalize_branch_text",
    "resolve_branch_alias",
]
