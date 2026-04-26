"""Report-title tolerance helpers for noisy attendance messages."""

from __future__ import annotations

from dataclasses import dataclass
import re

from .statuses import looks_like_attendance_row

_ATTENDANCE_TITLE_PATTERN = re.compile(
    r"^\s*(?:staff(?:s|['’]s)?\s+)?attendance(?:\s+report)?\.?\s*$",
    flags=re.IGNORECASE,
)
_NUMBERED_LINE_PATTERN = re.compile(r"^\s*\d+")
_ATTENDANCE_SUMMARY_MARKERS = (
    "total staff",
    "staff present",
    "day off",
    "leave",
    "absent",
    "sick",
)


@dataclass(slots=True, frozen=True)
class TitleDetection:
    """One detected report title from raw human input."""

    canonical_title: str
    raw_value: str
    line_index: int | None
    explicit_line: bool


def detect_attendance_title(lines: list[str]) -> TitleDetection | None:
    """Return one attendance-title normalization candidate when safely supported."""

    for index, line in enumerate(lines):
        if _ATTENDANCE_TITLE_PATTERN.match(line):
            return TitleDetection(
                canonical_title="ATTENDANCE REPORT",
                raw_value=line.strip(),
                line_index=index,
                explicit_line=True,
            )

    if _has_attendance_structure(lines):
        return TitleDetection(
            canonical_title="ATTENDANCE REPORT",
            raw_value="attendance_structure_detected",
            line_index=None,
            explicit_line=False,
        )
    return None


def _has_attendance_structure(lines: list[str]) -> bool:
    numbered_lines = [line for line in lines if _NUMBERED_LINE_PATTERN.match(line)]
    if not numbered_lines:
        return False
    row_count = sum(1 for line in numbered_lines if looks_like_attendance_row(line))
    summary_hits = sum(1 for line in lines if _normalize_text(line).startswith(_ATTENDANCE_SUMMARY_MARKERS))
    ratio = row_count / len(numbered_lines)
    return (row_count >= 2 and summary_hits >= 1) or (row_count >= 3 and ratio >= 0.75)


def _normalize_text(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").split())
