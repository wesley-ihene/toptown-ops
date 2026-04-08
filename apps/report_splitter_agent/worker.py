"""Deterministically split mixed-content messages into report segments."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib

from apps.mixed_content_detector_agent.worker import MixedContentDetection


@dataclass(slots=True)
class ReportSegment:
    """One logical report segment derived from a mixed parent message."""

    segment_id: str
    segment_index: int
    detected_report_family: str
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

    if not detection.is_mixed or len(detection.boundary_hints) < 2:
        return ReportSplitResult(segments=[], common_prefix_lines=[], split_confidence=detection.confidence)

    raw_lines = text.splitlines()
    common_prefix_lines = [line.rstrip() for line in raw_lines[: detection.boundary_hints[0].line_number - 1] if line.strip()]
    parent_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    segments: list[ReportSegment] = []

    for segment_index, hint in enumerate(detection.boundary_hints):
        start_line = hint.line_number
        end_line = len(raw_lines)
        for next_hint in detection.boundary_hints[segment_index + 1 :]:
            if next_hint.line_number > hint.line_number:
                end_line = next_hint.line_number - 1
                break
        body_lines = [line.rstrip() for line in raw_lines[start_line - 1 : end_line] if line.strip()]
        segment_text = "\n".join(common_prefix_lines + body_lines).strip()
        segments.append(
            ReportSegment(
                segment_id=f"{parent_sha}:{segment_index}",
                segment_index=segment_index,
                detected_report_family=hint.report_family,
                raw_text=segment_text,
                start_line=start_line,
                end_line=end_line,
                split_confidence=detection.confidence,
                evidence=[f"boundary:{hint.line_number}:{hint.raw_line}"],
            )
        )

    return ReportSplitResult(
        segments=segments,
        common_prefix_lines=common_prefix_lines,
        split_confidence=detection.confidence,
    )
