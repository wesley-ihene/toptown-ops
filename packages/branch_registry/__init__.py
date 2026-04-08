"""Deterministic branch aliases for upstream intake routing."""

from __future__ import annotations

from dataclasses import dataclass
import re

CANONICAL_BRANCHES = {
    "waigani": "Waigani",
    "bena_road": "Bena Road",
    "lae_malaita": "Lae Malaita",
    "lae_5th_street": "Lae 5th Street",
}

BRANCH_ALIASES: dict[str, str] = {
    "waigani": "waigani",
    "waigani branch": "waigani",
    "ttc pom waigani branch": "waigani",
    "ttc waigani branch": "waigani",
    "bena road": "bena_road",
    "bena road branch": "bena_road",
    "bena road goroka branch": "bena_road",
    "bena road-goroka branch": "bena_road",
    "ttc bena road branch": "bena_road",
    "ttc bena road goroka branch": "bena_road",
    "ttc bena road-goroka branch": "bena_road",
    "lae malaita": "lae_malaita",
    "lae malaita branch": "lae_malaita",
    "ttc lae malaita branch": "lae_malaita",
    "lae malaita street shop": "lae_malaita",
    "malaita street": "lae_malaita",
    "malaita street shop": "lae_malaita",
    "lae market branch malaita street": "lae_malaita",
    "lae 5th street": "lae_5th_street",
    "lae 5th street branch": "lae_5th_street",
    "5th street lae branch": "lae_5th_street",
    "ttc 5th street branch": "lae_5th_street",
    "ttc 5th street lae branch": "lae_5th_street",
    "ttc lae 5th street branch": "lae_5th_street",
}

_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class BranchMatch:
    """Resolved branch alias plus routing evidence."""

    slug: str
    display_name: str
    matched_alias: str
    confidence: float


def normalize_branch_text(value: str) -> str:
    """Normalize a free-form branch line for alias matching."""

    lowered = value.casefold().strip()
    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", lowered)
    return " ".join(normalized.split())


def resolve_branch_alias(value: str) -> BranchMatch | None:
    """Return the best alias match for a free-form branch string."""

    normalized = normalize_branch_text(value)
    if not normalized:
        return None

    exact_slug = BRANCH_ALIASES.get(normalized)
    if exact_slug is not None:
        return BranchMatch(
            slug=exact_slug,
            display_name=CANONICAL_BRANCHES.get(exact_slug, exact_slug.replace("_", " ").title()),
            matched_alias=normalized,
            confidence=1.0,
        )

    partial_matches = [
        alias
        for alias in BRANCH_ALIASES
        if alias in normalized or normalized in alias
    ]
    if not partial_matches:
        return None

    matched_alias = max(partial_matches, key=len)
    slug = BRANCH_ALIASES[matched_alias]
    return BranchMatch(
        slug=slug,
        display_name=CANONICAL_BRANCHES.get(slug, slug.replace("_", " ").title()),
        matched_alias=matched_alias,
        confidence=0.85,
    )


def canonical_branch_slug(value: str) -> str:
    """Return one canonical branch slug or a normalized fallback slug."""

    match = resolve_branch_alias(value)
    if match is not None:
        return match.slug
    normalized = normalize_branch_text(value)
    return normalized.replace(" ", "_")
