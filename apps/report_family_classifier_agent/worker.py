"""Deterministic report-family classification for upstream routing."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Final

from apps.header_normalizer_agent.worker import HeaderNormalizationResult

_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")
_NUMBERED_STAFF_PATTERN = re.compile(r"^\s*\d+\s*[.)-]")
_NUMBERED_ENTRY_PATTERN = re.compile(r"^\s*(?:#\s*)?\d+\s*(?:[.)-]|\.\.)")
_TILL_PATTERN = re.compile(r"\btill\s*#?\d+", flags=re.IGNORECASE)
_ATTENDANCE_STATUS_LINE_PATTERN = re.compile(
    r"^\s*\d+\s*[.)-]?\s*.+?(?:=|[-:])\s*(?:pres(?:ent)?|off|on leave|leave|awn|absent|sick|suspend(?:ed)?|late)\b",
    flags=re.IGNORECASE,
)
_PERFORMANCE_STATUS_LINE_PATTERN = re.compile(
    r"^\s*\d+\s*[.)-]?\s*.+?\s*-\s*(?:off|leave|sick|absent|sent home|\d)\b",
    flags=re.IGNORECASE,
)
_BALE_HEADER_LINE_PATTERN = re.compile(r"^\s*(?:#\s*)?\d+\s*(?:\.\s*|\s+)[A-Za-z]", flags=re.IGNORECASE)
_CHECKMARK_PATTERN = re.compile(r"[✔✅]")

FAMILY_PATTERNS: Final[dict[str, tuple[str, ...]]] = {
    "staff_performance": (
        "staff performance report",
        "staff peformance report",
        "staff performance rating",
        "staff peformance rating",
        "update for staff performance",
        "performance assisting",
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
        "staffs attendance",
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
        "supervisor control summary",
        "supervisor checklist",
        "supervisor report",
        "control report",
    ),
}

_SALES_MARKERS: Final[tuple[str, ...]] = (
    "t cash",
    "t card",
    "cash sales",
    "card sales",
    "eftpos sales",
    "gross sales",
    "total sales",
    "z reading",
    "main door",
    "guest customer serve",
    "customers served",
    "customer count",
    "balanced by",
)
_ATTENDANCE_SUMMARY_MARKERS: Final[tuple[str, ...]] = (
    "total staff",
    "day off",
    "on leave",
    "present",
    "absent",
    "annual leave",
    "absent with notice",
    "absent without notice",
)
_PERFORMANCE_MARKERS: Final[tuple[str, ...]] = (
    "items moved",
    "total items moved",
    "items sold",
    "item sold",
    "assist",
    "assisting",
    "customers assist",
    "customer assists",
    "performance rating",
)
_BALE_MARKERS: Final[tuple[str, ...]] = (
    "daily bale summary",
    "released to rail",
    "bale summary",
    "bale #",
    "total bales",
    "total quantity",
    "total amount",
    "prepared by",
)
_SUPERVISOR_MARKERS: Final[tuple[str, ...]] = (
    "cash variance",
    "staffing issues",
    "stock issues affecting sales",
    "pricing or system issues",
    "pricing or system issues affecting sales",
    "exceptions escalated to ops manager",
    "supervisor confirmation",
    "all material issues have been escalated",
)


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
        heuristic_score, heuristic_evidence = _family_heuristic_score(
            family=family,
            text=text,
            normalized_body=normalized_body,
        )
        score += heuristic_score
        evidence.extend(heuristic_evidence)
        if family == "staff_performance":
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


def _family_heuristic_score(*, family: str, text: str, normalized_body: str) -> tuple[float, list[str]]:
    """Return deterministic structure-based family evidence for noisy WhatsApp input."""

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    score = 0.0
    evidence: list[str] = []

    if family == "sales_income":
        sales_hits = _matched_tokens(normalized_body, _SALES_MARKERS)
        if len(sales_hits) >= 4:
            score += 0.35
            evidence.append(f"body_marker:sales_labels:{','.join(sales_hits[:4])}")
        elif len(sales_hits) >= 2:
            score += 0.2
            evidence.append(f"body_marker:sales_labels:{','.join(sales_hits[:2])}")
        till_count = sum(1 for line in lines if _TILL_PATTERN.search(line))
        if till_count >= 1:
            score += 0.2
            evidence.append(f"body_marker:till_sections:{till_count}")
        if any(marker in normalized_body for marker in ("customer count", "main door", "guest customer serve")):
            score += 0.15
            evidence.append("body_marker:sales_customer_structure")

    elif family == "attendance":
        attendance_line_count = sum(
            1 for line in lines if _ATTENDANCE_STATUS_LINE_PATTERN.search(line) or (_CHECKMARK_PATTERN.search(line) and _NUMBERED_ENTRY_PATTERN.match(line))
        )
        if attendance_line_count >= 5:
            score += 0.3
            evidence.append(f"body_marker:attendance_entries:{attendance_line_count}")
        elif attendance_line_count >= 3:
            score += 0.2
            evidence.append(f"body_marker:attendance_entries:{attendance_line_count}")
        summary_hits = _matched_tokens(normalized_body, _ATTENDANCE_SUMMARY_MARKERS)
        if len(summary_hits) >= 3:
            score += 0.2
            evidence.append(f"body_marker:attendance_totals:{','.join(summary_hits[:3])}")
        if _CHECKMARK_PATTERN.search(text):
            score += 0.1
            evidence.append("body_marker:attendance_checkmarks")

    elif family == "staff_performance":
        performance_hits = _matched_tokens(normalized_body, _PERFORMANCE_MARKERS)
        if len(performance_hits) >= 3:
            score += 0.25
            evidence.append(f"body_marker:performance_labels:{','.join(performance_hits[:3])}")
        elif len(performance_hits) >= 2:
            score += 0.15
            evidence.append(f"body_marker:performance_labels:{','.join(performance_hits[:2])}")
        section_count = sum(1 for line in lines if "section" in line.casefold())
        if section_count >= 3:
            score += 0.15
            evidence.append(f"body_marker:performance_sections:{section_count}")
        rating_count = sum(1 for line in lines if _PERFORMANCE_STATUS_LINE_PATTERN.search(line))
        if rating_count >= 5:
            score += 0.2
            evidence.append(f"body_marker:performance_ratings:{rating_count}")

    elif family == "pricing_stock_release":
        bale_hits = _matched_tokens(normalized_body, _BALE_MARKERS)
        if len(bale_hits) >= 3:
            score += 0.25
            evidence.append(f"body_marker:bale_labels:{','.join(bale_hits[:3])}")
        item_header_count = sum(1 for line in lines if _BALE_HEADER_LINE_PATTERN.match(line))
        qty_count = sum(1 for line in lines if any(token in line.casefold() for token in ("qty", "quantity")))
        amount_count = sum(1 for line in lines if any(token in line.casefold() for token in ("amt", "amount", "value")))
        if item_header_count >= 1 and qty_count >= 1 and amount_count >= 1:
            score += 0.35
            evidence.append(
                f"body_marker:bale_item_structure:headers={item_header_count},qty={qty_count},amount={amount_count}"
            )
        if "waiting for approval" in normalized_body or "bales released" in normalized_body:
            score += 0.1
            evidence.append("body_marker:bale_release_summary")

    elif family == "supervisor_control":
        supervisor_hits = _matched_tokens(normalized_body, _SUPERVISOR_MARKERS)
        if len(supervisor_hits) >= 4:
            score += 0.45
            evidence.append(f"body_marker:supervisor_checklist:{','.join(supervisor_hits[:4])}")
        elif len(supervisor_hits) >= 2:
            score += 0.25
            evidence.append(f"body_marker:supervisor_checklist:{','.join(supervisor_hits[:2])}")
        if "supervisor" in normalized_body:
            score += 0.1
            evidence.append("body_marker:supervisor_named")

    return score, evidence


def _matched_tokens(normalized_body: str, markers: tuple[str, ...]) -> list[str]:
    """Return unique body markers found in deterministic order."""

    hits: list[str] = []
    for marker in markers:
        if marker in normalized_body and marker not in hits:
            hits.append(marker)
    return hits
