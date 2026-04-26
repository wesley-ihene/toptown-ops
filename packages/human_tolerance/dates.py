"""Date tolerance helpers for human-written WhatsApp reports."""

from __future__ import annotations

from dataclasses import dataclass
import re

from packages.normalization.dates import normalize_report_date

_DATE_LINE_PATTERN = re.compile(r"^\s*date\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)
_INLINE_DATE_PATTERN = re.compile(
    r"(?i)(?:^|\b)(?:date\s*[:=-]\s*)?(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*[:\s,]*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})\s*$"
)
_DATE_ONLY_PATTERN = re.compile(r"^\s*[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4}\s*$")


@dataclass(slots=True, frozen=True)
class DateDetection:
    """One detected report-date candidate from raw human input."""

    iso_date: str
    canonical_date: str
    raw_value: str
    line_index: int
    explicit_line: bool
    already_canonical: bool


def detect_report_date(lines: list[str]) -> DateDetection | None:
    """Return one canonical report date from explicit or header text."""

    for index, line in enumerate(lines):
        match = _DATE_LINE_PATTERN.match(line)
        if match is None:
            continue
        raw_value = match.group(1).strip()
        normalized = normalize_report_date(raw_value).normalized_value
        if normalized is None:
            continue
        canonical = canonical_attendance_date(normalized)
        return DateDetection(
            iso_date=normalized,
            canonical_date=canonical,
            raw_value=raw_value,
            line_index=index,
            explicit_line=True,
            already_canonical=line.strip() == f"Date: {canonical}",
        )

    for index, line in enumerate(lines[:8]):
        candidate = line.strip()
        if not candidate:
            continue
        if _INLINE_DATE_PATTERN.match(candidate) is None and _DATE_ONLY_PATTERN.match(candidate) is None:
            continue
        normalized = normalize_report_date(candidate).normalized_value
        if normalized is None:
            continue
        return DateDetection(
            iso_date=normalized,
            canonical_date=canonical_attendance_date(normalized),
            raw_value=candidate,
            line_index=index,
            explicit_line=False,
            already_canonical=False,
        )
    return None


def canonical_attendance_date(iso_date: str) -> str:
    """Return `DD/MM/YY` from one ISO report date."""

    if len(iso_date) < 10 or iso_date[4] != "-" or iso_date[7] != "-":
        return iso_date
    return f"{iso_date[8:10]}/{iso_date[5:7]}/{iso_date[2:4]}"
