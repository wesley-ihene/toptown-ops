"""Central branch alias normalization."""

from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata

from .types import AppliedRule, NormalizedValue

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
    "ttc waigani": "waigani",
    "bena road": "bena_road",
    "bena road branch": "bena_road",
    "bena road goroka branch": "bena_road",
    "bena road goroka": "bena_road",
    "bena road-goroka branch": "bena_road",
    "ttc bena road branch": "bena_road",
    "ttc bena road goroka branch": "bena_road",
    "ttc bena road-goroka branch": "bena_road",
    "lae malaita": "lae_malaita",
    "lae malaita branch": "lae_malaita",
    "ttc lae malaita branch": "lae_malaita",
    "ttc lae malaita": "lae_malaita",
    "lae malaita street shop": "lae_malaita",
    "malaita street": "lae_malaita",
    "malaita street shop": "lae_malaita",
    "lae market branch malaita street": "lae_malaita",
    "lae 5th street": "lae_5th_street",
    "lae 5th street branch": "lae_5th_street",
    "5th street lae branch": "lae_5th_street",
    "ttc 5th street": "lae_5th_street",
    "ttc 5th street branch": "lae_5th_street",
    "ttc 5th street lae branch": "lae_5th_street",
    "ttc lae 5th street": "lae_5th_street",
    "ttc lae 5th street branch": "lae_5th_street",
}

_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True, frozen=True)
class BranchMatch:
    """Resolved branch alias plus matching evidence."""

    slug: str
    display_name: str
    matched_alias: str
    confidence: float


def normalize_branch_text(value: str) -> str:
    """Return a comparison-safe branch string."""

    lowered = _compatibility_fold(value).casefold().strip()
    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", lowered)
    return " ".join(normalized.split())


def resolve_branch_alias(value: str) -> BranchMatch | None:
    """Return the best alias match for a free-form branch string."""

    normalized = normalize_branch_text(value)
    if not normalized:
        return None

    if normalized in CANONICAL_BRANCHES:
        return BranchMatch(
            slug=normalized,
            display_name=CANONICAL_BRANCHES[normalized],
            matched_alias=normalized,
            confidence=1.0,
        )

    exact_slug = BRANCH_ALIASES.get(normalized)
    if exact_slug is not None:
        return BranchMatch(
            slug=exact_slug,
            display_name=CANONICAL_BRANCHES.get(exact_slug, exact_slug.replace("_", " ").title()),
            matched_alias=normalized,
            confidence=1.0,
        )

    token_match = _token_bag_match(normalized)
    if token_match is not None:
        matched_alias, slug = token_match
        return BranchMatch(
            slug=slug,
            display_name=CANONICAL_BRANCHES.get(slug, slug.replace("_", " ").title()),
            matched_alias=matched_alias,
            confidence=0.9,
        )

    partial_matches = [
        alias
        for alias in BRANCH_ALIASES
        if alias in normalized or normalized in alias
    ]
    if partial_matches:
        matched_alias = max(partial_matches, key=lambda alias: (len(alias.split()), len(alias)))
        slug = BRANCH_ALIASES[matched_alias]
        return BranchMatch(
            slug=slug,
            display_name=CANONICAL_BRANCHES.get(slug, slug.replace("_", " ").title()),
            matched_alias=matched_alias,
            confidence=0.85,
        )
    return None


def normalize_branch(raw_value: str) -> NormalizedValue:
    """Return one canonical branch slug only when confidently matched."""

    result = NormalizedValue(raw_value=raw_value, metadata={"value_type": "branch"})
    match = resolve_branch_alias(raw_value)
    if match is None:
        result.hard_errors.append("unknown_branch_alias")
        return result

    result.normalized_value = match.slug
    result.confidence = match.confidence
    result.metadata.update(
        {
            "display_name": match.display_name,
            "matched_alias": match.matched_alias,
        }
    )
    result.applied_rules.append(
        AppliedRule(
            name="branch_alias_matched",
            raw_value=raw_value,
            normalized_value=match.slug,
            details={
                "matched_alias": match.matched_alias,
                "display_name": match.display_name,
            },
        )
    )
    return result


def canonical_branch_slug(value: str) -> str:
    """Return a canonical branch slug with legacy normalized fallback."""

    result = normalize_branch(value)
    if result.normalized_value is not None:
        return result.normalized_value
    normalized = normalize_branch_text(value)
    return normalized.replace(" ", "_")


def _compatibility_fold(value: str) -> str:
    """Return one ASCII-friendly representation for noisy Unicode headers."""

    normalized = unicodedata.normalize("NFKD", value)
    return "".join(character for character in normalized if not unicodedata.combining(character))


def _token_bag_match(normalized: str) -> tuple[str, str] | None:
    """Return one alias match when tokens match exactly despite ordering noise."""

    normalized_tokens = tuple(sorted(token for token in normalized.split() if token))
    if not normalized_tokens:
        return None

    matches: list[str] = []
    for alias, slug in BRANCH_ALIASES.items():
        alias_tokens = tuple(sorted(token for token in alias.split() if token))
        if alias_tokens == normalized_tokens:
            matches.append(alias)

    if len(matches) != 1:
        return None
    matched_alias = matches[0]
    return matched_alias, BRANCH_ALIASES[matched_alias]
