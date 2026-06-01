"""Deterministically split mixed-content messages into report segments."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import re
from typing import Final

from apps.branch_resolver_agent.worker import resolve_branch
from apps.date_resolver_agent.worker import resolve_report_date
from apps.header_normalizer_agent.worker import normalize_headers
from apps.mixed_content_detector_agent.worker import BoundaryHint, MixedContentDetection
from packages.report_registry import APPROVED_MIXED_SPLIT_TITLES

_MIXED_SPLIT_CONFIDENCE_MIN: Final[float] = 0.85
_SALES_SUPERVISOR_SEPARATOR_CONFIDENCE: Final[float] = 0.95
_SALES_SUPERVISOR_FAMILIES: Final[frozenset[str]] = frozenset({"sales_income", "supervisor_control"})
_INTELLIGENCE_REPORT_FAMILIES: Final[frozenset[str]] = frozenset({"supervisor_control"})
_NON_ALPHANUMERIC_PATTERN: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")
_SECTION_SEPARATOR_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\s*[-=_*]{3,}\s*$")
_SUPERVISOR_CONTROL_HEADER_ALIASES: Final[tuple[str, ...]] = (
    "supervisor control report",
    "supervisor control summary",
    "supervisor control",
    "supervisor summary",
)
_SEGMENT_KEYWORDS: Final[dict[str, tuple[str, ...]]] = {
    "sales_income": (
        "gross sales",
        "total sales",
        "cash sales",
        "card sales",
        "eftpos sales",
        "t cash",
        "t card",
        "total cash",
        "total card",
        "main door",
        "traffic",
        "served",
        "customers served",
        "guest customer serve",
        "z reading",
        "till total",
        "deposit total",
        "balanced by",
    ),
    "supervisor_control": (
        "floor check",
        "cashier reconciled",
        "store locked",
        "checklist signed",
        "cash variance",
        "staffing issues",
        "stock issues",
        "pricing or system issues",
        "supervisor confirmation",
        "supervisor confirmed",
        "exception type",
        "action taken",
    ),
}
RUNTIME_STATUS = "LIVE_RUNTIME"
RUNTIME_OWNER = "report_splitter_agent"
RUNTIME_NOTE = "Live mixed-report splitter used by apps.orchestrator_agent.worker."


@dataclass(slots=True)
class ReportSegment:
    """One logical report segment derived from a mixed parent message."""

    segment_id: str
    segment_index: int
    detected_report_family: str
    report_family_label: str
    blocks_transactional_processing: bool
    header_line: str
    raw_text: str
    start_line: int
    end_line: int
    split_confidence: float
    evidence: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ReportSplitResult:
    """Split outcome for one raw message."""

    segments: list[ReportSegment] = field(default_factory=list)
    common_prefix_lines: list[str] = field(default_factory=list)
    split_confidence: float = 0.0


def split_report(text: str, detection: MixedContentDetection) -> ReportSplitResult:
    """Split a mixed raw message into non-overlapping report segments."""

    boundary_hints = _resolved_boundary_hints(detection)
    if not detection.is_mixed or len(boundary_hints) < 2:
        return ReportSplitResult(segments=[], common_prefix_lines=[], split_confidence=detection.confidence)

    raw_lines = text.splitlines()
    common_prefix_lines = [
        line.rstrip()
        for line in raw_lines[: boundary_hints[0].line_number - 1]
        if line.strip() and not _is_separator_line(line)
    ]
    parent_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    segments: list[ReportSegment] = []

    for segment_index, hint in enumerate(boundary_hints):
        start_line = hint.line_number
        end_line = len(raw_lines)
        for next_hint in boundary_hints[segment_index + 1 :]:
            if next_hint.line_number > hint.line_number:
                end_line = _segment_end_line(raw_lines, start_line=start_line, next_hint=next_hint)
                break
        body_lines = [
            line.rstrip()
            for line in raw_lines[start_line - 1 : end_line]
            if line.strip() and not _is_separator_line(line)
        ]
        segment_text = "\n".join(common_prefix_lines + body_lines).strip()
        segments.append(
            ReportSegment(
                segment_id=f"{parent_sha}:{segment_index}",
                segment_index=segment_index,
                detected_report_family=hint.report_family,
                report_family_label=_report_family_label(hint.report_family),
                blocks_transactional_processing=_blocks_transactional_processing(hint.report_family),
                header_line=hint.raw_line.strip(),
                raw_text=segment_text,
                start_line=start_line,
                end_line=end_line,
                split_confidence=detection.confidence,
                evidence=[f"boundary:{hint.line_number}:{hint.raw_line}"],
            )
        )

    split_confidence = _resolved_split_confidence(
        detection=detection,
        segments=segments,
        raw_lines=raw_lines,
    )
    for segment in segments:
        segment.split_confidence = split_confidence
        keyword_matches = _matched_segment_keywords(segment.detected_report_family, segment.raw_text)
        segment.evidence.extend(f"keyword:{keyword}" for keyword in keyword_matches)

    return ReportSplitResult(
        segments=segments,
        common_prefix_lines=common_prefix_lines,
        split_confidence=split_confidence,
    )


def _resolved_boundary_hints(detection: MixedContentDetection) -> list[BoundaryHint]:
    """Return stable boundary hints in original line order."""

    deduped: list[BoundaryHint] = []
    seen_lines: set[int] = set()
    for hint in sorted(detection.boundary_hints, key=lambda value: value.line_number):
        if hint.line_number in seen_lines:
            continue
        if deduped and deduped[-1].report_family == hint.report_family:
            continue
        deduped.append(hint)
        seen_lines.add(hint.line_number)
    return deduped


def _segment_end_line(
    raw_lines: list[str],
    *,
    start_line: int,
    next_hint: BoundaryHint,
) -> int:
    """Return the current segment end line without trailing separator rows."""

    for line_number in range(next_hint.line_number - 1, start_line, -1):
        candidate = raw_lines[line_number - 1]
        if _is_separator_line(candidate):
            return line_number - 1
        if candidate.strip():
            break
    return next_hint.line_number - 1


def _resolved_split_confidence(
    *,
    detection: MixedContentDetection,
    segments: list[ReportSegment],
    raw_lines: list[str],
) -> float:
    """Return the final split confidence with a narrow sales/supervisor boost."""

    if not _segments_have_required_scope(segments):
        return min(detection.confidence, 0.4)
    if _should_force_sales_supervisor_separator_confidence(
        detection=detection,
        segments=segments,
        raw_lines=raw_lines,
    ):
        return _SALES_SUPERVISOR_SEPARATOR_CONFIDENCE
    if _eligible_for_sales_supervisor_boost(detection):
        if len(segments) != 2:
            return detection.confidence
        if not all(_segment_has_valid_keywords(segment) for segment in segments):
            return detection.confidence
    return max(detection.confidence, _MIXED_SPLIT_CONFIDENCE_MIN)


def _should_force_sales_supervisor_separator_confidence(
    *,
    detection: MixedContentDetection,
    segments: list[ReportSegment],
    raw_lines: list[str],
) -> bool:
    """Return whether a separator-bounded sales/supervisor split should be forced."""

    if not _eligible_for_sales_supervisor_boost(detection):
        return False
    sales_segment, supervisor_segment = _sales_and_supervisor_segments(segments)
    if sales_segment is None or supervisor_segment is None:
        return False
    if not all(_segment_has_valid_keywords(segment) for segment in (sales_segment, supervisor_segment)):
        return False
    if not _segment_matches_boundary_hint(detection, sales_segment):
        return False
    if not _segment_matches_boundary_hint(detection, supervisor_segment):
        return False
    if not _has_separator_between_segments(raw_lines, sales_segment=sales_segment, supervisor_segment=supervisor_segment):
        return False
    return _segment_starts_with_supervisor_header(raw_lines, supervisor_segment)


def _eligible_for_sales_supervisor_boost(detection: MixedContentDetection) -> bool:
    """Return whether the narrow split-confidence boost is allowed."""

    detected_families = {family for family in detection.detected_families if family}
    return len(detected_families) == 2 and detected_families <= _SALES_SUPERVISOR_FAMILIES


def _report_family_label(detected_report_family: str) -> str:
    """Return the governance-facing report family label for one segment."""

    if detected_report_family in _INTELLIGENCE_REPORT_FAMILIES:
        return "intelligence"
    return detected_report_family


def _blocks_transactional_processing(detected_report_family: str) -> bool:
    """Return whether one segment should block transactional mixed acceptance."""

    return detected_report_family not in _INTELLIGENCE_REPORT_FAMILIES


def _sales_and_supervisor_segments(
    segments: list[ReportSegment],
) -> tuple[ReportSegment | None, ReportSegment | None]:
    """Return ordered sales and supervisor segments when exactly one of each exists."""

    if len(segments) != 2:
        return None, None

    sales_segment = next(
        (segment for segment in segments if segment.detected_report_family == "sales_income"),
        None,
    )
    supervisor_segment = next(
        (segment for segment in segments if segment.detected_report_family == "supervisor_control"),
        None,
    )
    if sales_segment is None or supervisor_segment is None:
        return None, None
    if sales_segment.segment_index >= supervisor_segment.segment_index:
        return None, None
    return sales_segment, supervisor_segment


def _segment_matches_boundary_hint(
    detection: MixedContentDetection,
    segment: ReportSegment,
) -> bool:
    """Return whether the segment start line matches an explicit detected header."""

    return any(
        hint.report_family == segment.detected_report_family and hint.line_number == segment.start_line
        for hint in detection.boundary_hints
    )


def _has_separator_between_segments(
    raw_lines: list[str],
    *,
    sales_segment: ReportSegment,
    supervisor_segment: ReportSegment,
) -> bool:
    """Return whether an explicit separator line appears between the two segments."""

    for line_number in range(sales_segment.end_line + 1, supervisor_segment.start_line):
        if _is_separator_line(raw_lines[line_number - 1]):
            return True
    return False


def _segment_starts_with_supervisor_header(
    raw_lines: list[str],
    segment: ReportSegment,
) -> bool:
    """Return whether the segment begins with a known supervisor-summary title."""

    if segment.detected_report_family != "supervisor_control":
        return False
    if segment.start_line < 1 or segment.start_line > len(raw_lines):
        return False
    normalized_line = _normalize_text(raw_lines[segment.start_line - 1])
    if normalized_line in _SUPERVISOR_CONTROL_HEADER_ALIASES:
        return True
    return any(normalized_line.startswith(f"{alias} ") for alias in _SUPERVISOR_CONTROL_HEADER_ALIASES)


def _segment_has_valid_keywords(segment: ReportSegment) -> bool:
    """Return whether a segment contains enough family-specific keywords."""

    return len(_matched_segment_keywords(segment.detected_report_family, segment.raw_text)) >= 2


def _segments_have_required_scope(segments: list[ReportSegment]) -> bool:
    """Return whether each segment has an approved title plus inherited branch/date scope."""

    return bool(segments) and all(_segment_has_required_scope(segment) for segment in segments)


def _segment_has_required_scope(segment: ReportSegment) -> bool:
    """Return whether one split segment has the minimum safe routing scope."""

    if not _segment_has_approved_title(segment):
        return False
    header_result = normalize_headers(segment.raw_text, max_lines=12)
    branch_resolution = resolve_branch(header_result)
    date_resolution = resolve_report_date(header_result)
    return branch_resolution.branch_hint is not None and date_resolution.iso_date is not None


def _segment_has_approved_title(segment: ReportSegment) -> bool:
    """Return whether one segment begins with an approved split title."""

    approved_titles = APPROVED_MIXED_SPLIT_TITLES.get(segment.detected_report_family)
    if not approved_titles:
        return False
    normalized_header = _normalize_text(segment.header_line)
    return normalized_header in { _normalize_text(title) for title in approved_titles }


def _matched_segment_keywords(report_family: str, text: str) -> list[str]:
    """Return matched family-specific keywords from one split segment."""

    keywords = _SEGMENT_KEYWORDS.get(report_family)
    if not keywords:
        return []
    normalized_text = _normalize_text(text)
    return [keyword for keyword in keywords if keyword in normalized_text]


def _normalize_text(value: str) -> str:
    """Return a stable keyword-matching representation for one segment."""

    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", value.casefold())
    return " ".join(normalized.split())


def _is_separator_line(value: str) -> bool:
    """Return whether a line is an explicit mixed-section separator."""

    return bool(_SECTION_SEPARATOR_PATTERN.match(value.strip()))
