"""Deterministic report-family classification for upstream routing."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Final

from apps.header_normalizer_agent.worker import HeaderNormalizationResult

_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")
_NUMBERED_STAFF_PATTERN = re.compile(r"^\s*\d+\s*[.)-]")

FAMILY_PATTERNS: Final[dict[str, tuple[str, ...]]] = {
    "staff_performance": (
        "staff performance report",
        "staff assisting customers",
        "staff assisting report",
    ),
    "staff_sales": (
        "staff sales report",
        "sales by staff",
        "staff sales",
    ),
    "sales_income": (
        "day end sales report",
        "sales income report",
        "sales report",
    ),
    "attendance": (
        "staff attendance report",
        "staff attendance",
        "attendance report",
    ),
    "pricing_stock_release": (
        "daily bale summary",
        "pricing stock release",
        "released to rail",
        "stock release",
    ),
    "supervisor_control": (
        "supervisor control",
        "supervisor checklist",
        "supervisor report",
        "control report",
    ),
}


@dataclass(slots=True)
class FamilyClassification:
    """Deterministic report-family decision."""

    report_family: str
    confidence: float
    evidence: list[str] = field(default_factory=list)


def classify_report_family(text: str, header_result: HeaderNormalizationResult) -> FamilyClassification:
    """Classify one report family from normalized header and body evidence."""

    header_lines = header_result.normalized_lines()
    normalized_body = _normalize_text(text)
    evidence_by_family: dict[str, list[str]] = {}
    score_by_family: dict[str, float] = {}

    for family, patterns in FAMILY_PATTERNS.items():
        score = 0.0
        evidence: list[str] = []
        for candidate in header_result.candidates:
            if any(pattern in candidate.normalized_line for pattern in patterns):
                score += 0.8
                evidence.append(f"header_line:{candidate.line_number}:{candidate.raw_line}")
                break
        for pattern in patterns:
            if pattern in normalized_body and pattern not in " ".join(header_lines):
                score += 0.2
                evidence.append(f"body_marker:{pattern}")
                break
        if family == "staff_performance":
            if "items moved" in normalized_body:
                score += 0.15
                evidence.append("body_marker:items moved")
            if "assist" in normalized_body:
                score += 0.15
                evidence.append("body_marker:assist")
            numbered_count = sum(1 for line in text.splitlines() if _NUMBERED_STAFF_PATTERN.match(line))
            if numbered_count >= 3:
                score += 0.1
                evidence.append(f"body_marker:numbered_staff_entries:{numbered_count}")
        if family == "attendance" and any(token in normalized_body for token in ("present", "absent", "annual leave", "sick")):
            score += 0.2
            evidence.append("body_marker:attendance_statuses")
        if score > 0:
            score_by_family[family] = min(score, 1.0)
            evidence_by_family[family] = evidence

    if not score_by_family:
        return FamilyClassification(report_family="unknown", confidence=0.0, evidence=[])

    report_family = max(score_by_family, key=lambda family: (score_by_family[family], family))
    confidence = score_by_family[report_family]
    if confidence < 0.45:
        return FamilyClassification(report_family="unknown", confidence=confidence, evidence=evidence_by_family[report_family])
    return FamilyClassification(
        report_family=report_family,
        confidence=confidence,
        evidence=evidence_by_family[report_family],
    )


def _normalize_text(value: str) -> str:
    """Normalize free-form text for conservative family marker matching."""

    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", value.casefold())
    return " ".join(normalized.split())
