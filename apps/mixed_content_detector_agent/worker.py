"""Detect explicit mixed-report content and trailing notes in one raw message."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.header_normalizer_agent.worker import normalize_headers
from packages.report_registry import FAMILY_BOUNDARY_HEADERS, NONCRITICAL_TRAILING_NOTE_HEADERS


@dataclass(slots=True)
class BoundaryHint:
    """One detected family boundary in the raw message."""

    report_family: str
    line_number: int
    raw_line: str
    normalized_line: str


@dataclass(slots=True)
class MixedContentDetection:
    """Outcome of mixed-content detection."""

    classification: str
    is_mixed: bool
    detected_families: list[str] = field(default_factory=list)
    confidence: float = 0.0
    evidence: list[str] = field(default_factory=list)
    boundary_hints: list[BoundaryHint] = field(default_factory=list)


def detect_mixed_content(text: str) -> MixedContentDetection:
    """Return whether a message is mixed, single, or single with trailing notes."""

    normalized_headers = normalize_headers(text, max_lines=64)
    boundary_hints = _find_boundary_hints(text)
    detected_families = []
    for hint in boundary_hints:
        if hint.report_family not in detected_families:
            detected_families.append(hint.report_family)

    trailing_notes = _has_noncritical_trailing_notes(text, boundary_hints)
    if len(detected_families) >= 2:
        return MixedContentDetection(
            classification="mixed_report",
            is_mixed=True,
            detected_families=detected_families,
            confidence=min(1.0, 0.75 + (0.1 * len(detected_families))),
            evidence=[f"boundary:{hint.line_number}:{hint.report_family}:{hint.raw_line}" for hint in boundary_hints],
            boundary_hints=boundary_hints,
        )
    if len(detected_families) == 1 and trailing_notes:
        return MixedContentDetection(
            classification="single_report_with_noncritical_trailing_notes",
            is_mixed=False,
            detected_families=detected_families,
            confidence=0.9,
            evidence=[
                f"boundary:{hint.line_number}:{hint.report_family}:{hint.raw_line}" for hint in boundary_hints
            ] + [f"trailing_notes:{note}" for note in trailing_notes],
            boundary_hints=boundary_hints,
        )
    return MixedContentDetection(
        classification="single_report",
        is_mixed=False,
        detected_families=detected_families,
        confidence=0.95 if detected_families else 0.5,
        evidence=[f"header_candidate:{line}" for line in normalized_headers.normalized_lines()[:3]],
        boundary_hints=boundary_hints,
    )


def _find_boundary_hints(text: str) -> list[BoundaryHint]:
    """Return explicit family boundary hints from the message body."""

    hints: list[BoundaryHint] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        normalized_line = " ".join(raw_line.casefold().replace("/", " ").replace("-", " ").split())
        normalized_line = "".join(char if char.isalnum() or char.isspace() else " " for char in normalized_line)
        normalized_line = " ".join(normalized_line.split())
        if not normalized_line:
            continue
        for family, headers in FAMILY_BOUNDARY_HEADERS.items():
            for header in headers:
                normalized_header = " ".join(header.casefold().split())
                if normalized_line == normalized_header or normalized_line.startswith(f"{normalized_header} "):
                    hints.append(
                        BoundaryHint(
                            report_family=family,
                            line_number=line_number,
                            raw_line=raw_line.strip(),
                            normalized_line=normalized_line,
                        )
                    )
                    break
            else:
                continue
            break
    return _dedupe_consecutive_hints(hints)


def _dedupe_consecutive_hints(hints: list[BoundaryHint]) -> list[BoundaryHint]:
    """Collapse repeated immediately-adjacent headers for the same family."""

    deduped: list[BoundaryHint] = []
    for hint in hints:
        if deduped and deduped[-1].report_family == hint.report_family:
            continue
        deduped.append(hint)
    return deduped


def _has_noncritical_trailing_notes(text: str, boundary_hints: list[BoundaryHint]) -> list[str]:
    """Return trailing note markers when present after a single report."""

    if len({hint.report_family for hint in boundary_hints}) != 1:
        return []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    found_notes: list[str] = []
    for line in lines:
        normalized = " ".join("".join(char if char.isalnum() or char.isspace() else " " for char in line.casefold()).split())
        for marker in NONCRITICAL_TRAILING_NOTE_HEADERS:
            if normalized == marker or normalized.startswith(f"{marker} "):
                found_notes.append(marker)
    return found_notes
