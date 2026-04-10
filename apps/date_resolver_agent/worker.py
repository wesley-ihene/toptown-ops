"""Resolve report dates from early WhatsApp header lines."""

from __future__ import annotations

from dataclasses import dataclass, field
import re

from apps.header_normalizer_agent.worker import HeaderNormalizationResult
from packages.normalization.dates import normalize_report_date as normalize_strict_report_date

_DATE_PATTERN = re.compile(
    r"\b(?:(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+)?"
    r"(\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4})\b",
    flags=re.IGNORECASE,
)
_ISO_DATE_PATTERN = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_WEEKDAY_PREFIX_PATTERN = re.compile(
    r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+",
    flags=re.IGNORECASE,
)


@dataclass(slots=True)
class DateResolution:
    """Resolved report date with provenance."""

    iso_date: str | None
    raw_date: str | None
    confidence: float
    evidence: list[str] = field(default_factory=list)


def normalize_report_date(raw_value: str) -> str | None:
    """Return ISO date for supported branch report date formats."""

    return normalize_strict_report_date(raw_value).normalized_value


def resolve_report_date(header_result: HeaderNormalizationResult) -> DateResolution:
    """Resolve report date from the first non-empty header lines."""

    for candidate in header_result.candidates:
        matched = _DATE_PATTERN.search(candidate.raw_line)
        if matched is None:
            matched = _ISO_DATE_PATTERN.search(candidate.raw_line)
        if matched is None:
            matched = _DATE_PATTERN.search(candidate.normalized_line)
        if matched is None:
            matched = _ISO_DATE_PATTERN.search(candidate.normalized_line)
        if matched is None:
            continue
        raw_date = matched.group(0).strip()
        iso_date = normalize_report_date(raw_date)
        if iso_date is None:
            continue
        return DateResolution(
            iso_date=iso_date,
            raw_date=raw_date,
            confidence=1.0,
            evidence=[f"header_line:{candidate.line_number}:{candidate.raw_line}"],
        )

    return DateResolution(iso_date=None, raw_date=None, confidence=0.0, evidence=[])
