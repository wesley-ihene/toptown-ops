"""Central branch alias normalization."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import re
import unicodedata

from packages.common.paths import REPO_ROOT

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
    "pom waigani": "waigani",
    "pom waigani branch": "waigani",
    "ttc pom waigani": "waigani",
    "ttc pom waigani branch": "waigani",
    "ttc waigani branch": "waigani",
    "ttc waigani": "waigani",
    "bena road": "bena_road",
    "benaroad": "bena_road",
    "bena road branch": "bena_road",
    "goroka benaroad": "bena_road",
    "goroka benaroad branch": "bena_road",
    "goroka bena road": "bena_road",
    "bena road goroka branch": "bena_road",
    "bena road goroka": "bena_road",
    "bena road-goroka branch": "bena_road",
    "ttc bena road goroka": "bena_road",
    "ttc bena road branch": "bena_road",
    "ttc bena road goroka branch": "bena_road",
    "ttc bena road-goroka branch": "bena_road",
    "lae malaita": "lae_malaita",
    "lae malaita branch": "lae_malaita",
    "ttc lae malaita branch": "lae_malaita",
    "ttc lae malaita": "lae_malaita",
    "lae malaita shop": "lae_malaita",
    "lae malaita street shop": "lae_malaita",
    "malaita shop": "lae_malaita",
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
_CANONICAL_BRANCH_SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_CONFIG_PATH = REPO_ROOT / "config" / "branches.yaml"
UNKNOWN_BRANCH_BUCKET = "unknown"


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

    allowed_slugs = configured_branch_slugs()

    if normalized in allowed_slugs:
        return BranchMatch(
            slug=normalized,
            display_name=_display_name(normalized),
            matched_alias=normalized,
            confidence=1.0,
        )

    exact_slug = BRANCH_ALIASES.get(normalized)
    if exact_slug is not None and exact_slug in allowed_slugs:
        return BranchMatch(
            slug=exact_slug,
            display_name=_display_name(exact_slug),
            matched_alias=normalized,
            confidence=1.0,
        )

    token_match = _token_bag_match(normalized, allowed_slugs=allowed_slugs)
    if token_match is not None:
        matched_alias, slug = token_match
        return BranchMatch(
            slug=slug,
            display_name=_display_name(slug),
            matched_alias=matched_alias,
            confidence=0.9,
        )

    partial_matches = [
        alias
        for alias in BRANCH_ALIASES
        if (alias in normalized or normalized in alias) and BRANCH_ALIASES.get(alias) in allowed_slugs
    ]
    if partial_matches:
        matched_alias = max(partial_matches, key=lambda alias: (len(alias.split()), len(alias)))
        slug = BRANCH_ALIASES[matched_alias]
        return BranchMatch(
            slug=slug,
            display_name=_display_name(slug),
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
    """Return one configured canonical branch slug or raise ``ValueError``."""

    slug = canonical_branch_slug_or_none(value)
    if slug is None:
        raise ValueError(f"unknown_branch_slug: {value!r}")
    return slug


def canonical_branch_slug_or_none(value: str | None) -> str | None:
    """Return one configured canonical branch slug or ``None``."""

    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None

    result = normalize_branch(cleaned)
    if result.normalized_value is not None and result.normalized_value in configured_branch_slugs():
        return result.normalized_value

    normalized_slug = normalize_branch_text(cleaned).replace(" ", "_")
    if normalized_slug in configured_branch_slugs():
        return normalized_slug
    return None


def is_canonical_branch_slug(value: str | None) -> bool:
    """Return whether ``value`` is one configured canonical branch slug."""

    if value is None:
        return False
    cleaned = value.strip()
    if not cleaned or not _CANONICAL_BRANCH_SLUG_PATTERN.fullmatch(cleaned):
        return False
    return cleaned in configured_branch_slugs()


@lru_cache(maxsize=1)
def configured_branch_slugs() -> frozenset[str]:
    """Return the configured canonical branch slugs from ``config/branches.yaml``."""

    configured = _configured_branch_slugs_from_file()
    if configured:
        return configured
    return frozenset(CANONICAL_BRANCHES)


def _compatibility_fold(value: str) -> str:
    """Return one ASCII-friendly representation for noisy Unicode headers."""

    normalized = unicodedata.normalize("NFKD", value)
    return "".join(character for character in normalized if not unicodedata.combining(character))


def _token_bag_match(normalized: str, *, allowed_slugs: frozenset[str]) -> tuple[str, str] | None:
    """Return one alias match when tokens match exactly despite ordering noise."""

    normalized_tokens = tuple(sorted(token for token in normalized.split() if token))
    if not normalized_tokens:
        return None

    matches: list[str] = []
    for alias, slug in BRANCH_ALIASES.items():
        if slug not in allowed_slugs:
            continue
        alias_tokens = tuple(sorted(token for token in alias.split() if token))
        if alias_tokens == normalized_tokens:
            matches.append(alias)

    if len(matches) != 1:
        return None
    matched_alias = matches[0]
    return matched_alias, BRANCH_ALIASES[matched_alias]


def _configured_branch_slugs_from_file() -> frozenset[str]:
    """Return configured branch slugs from the simple branch config when readable."""

    try:
        content = _CONFIG_PATH.read_text(encoding="utf-8")
    except OSError:
        return frozenset()

    slugs: list[str] = []
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped.startswith("- slug:"):
            continue
        slug = stripped.split(":", 1)[1].strip().strip("\"'")
        if slug and _CANONICAL_BRANCH_SLUG_PATTERN.fullmatch(slug):
            slugs.append(slug)
    return frozenset(slugs)


def _display_name(slug: str) -> str:
    """Return one display name for a canonical branch slug."""

    return CANONICAL_BRANCHES.get(slug, slug.replace("_", " ").title())
