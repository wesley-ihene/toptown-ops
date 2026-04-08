"""Resolve branch aliases from early WhatsApp header lines."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.header_normalizer_agent.worker import HeaderNormalizationResult
from packages.branch_registry import resolve_branch_alias


@dataclass(slots=True)
class BranchResolution:
    """Resolved branch hint with confidence and evidence."""

    branch_hint: str | None
    branch_display_name: str | None
    confidence: float
    evidence: list[str] = field(default_factory=list)
    raw_branch_line: str | None = None


def resolve_branch(
    header_result: HeaderNormalizationResult,
    *,
    metadata_branch_hint: str | None = None,
) -> BranchResolution:
    """Resolve one branch hint from the first header lines or metadata."""

    for candidate in header_result.candidates:
        match = resolve_branch_alias(candidate.raw_line)
        if match is None:
            match = resolve_branch_alias(candidate.normalized_line)
        if match is not None:
            return BranchResolution(
                branch_hint=match.slug,
                branch_display_name=match.display_name,
                confidence=match.confidence,
                evidence=[f"header_line:{candidate.line_number}:{candidate.raw_line}"],
                raw_branch_line=candidate.raw_line,
            )

    if metadata_branch_hint:
        match = resolve_branch_alias(metadata_branch_hint)
        if match is not None:
            return BranchResolution(
                branch_hint=match.slug,
                branch_display_name=match.display_name,
                confidence=0.7,
                evidence=[f"metadata_branch_hint:{metadata_branch_hint}"],
                raw_branch_line=metadata_branch_hint,
            )

    return BranchResolution(
        branch_hint=None,
        branch_display_name=None,
        confidence=0.0,
        evidence=[],
        raw_branch_line=None,
    )
