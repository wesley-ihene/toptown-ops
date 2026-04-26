"""Deterministic TAOP message-intent detection for operational WhatsApp queries."""

from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import Any

from packages.normalization.branches import normalize_branch_text
from packages.normalization.dates import normalize_report_date

_REPORT_TITLES = (
    "day-end sales report",
    "attendance report",
    "staff performance report",
    "daily bale summary",
    "supervisor control report",
    "store monitoring report",
)
_QUERY_MARKERS = (
    "confirm",
    "receive",
    "received",
    "status",
    "missing",
    "not sent",
    "did you receive",
    "have all",
    "which branch",
    "which reports",
    "what reports",
)
_ATTENDANCE_MARKERS = (
    "attendance",
    "staff attendance",
    "staffs attendance",
    "attendance report",
)
_BALE_MARKERS = (
    "bale summary",
    "daily bale summary",
    "released to rail",
    "pricing stock release",
    "pricing_stock_release",
)
_REPORT_MARKERS = ("report", "reports")
_DATE_PATTERN = re.compile(r"\b\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}\b", flags=re.IGNORECASE)
_QUESTION_WORD_PATTERN = re.compile(r"\b(confirm|did|have|which|what|status)\b", flags=re.IGNORECASE)
_BRANCH_HINT_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bwaigani\b", flags=re.IGNORECASE), "waigani"),
    (re.compile(r"\b(?:lae\s+)?malaita(?:\s+street)?\b", flags=re.IGNORECASE), "lae_malaita"),
    (re.compile(r"\b(?:lae\s+)?5th\s+street\b", flags=re.IGNORECASE), "lae_5th_street"),
    (re.compile(r"\bbena(?:\s+road)?\b", flags=re.IGNORECASE), "bena_road"),
)


def detect_message_intent(
    text: str,
    *,
    reference_now: datetime | None = None,
) -> dict[str, Any]:
    """Classify deterministic operational-status queries before report rejection."""

    cleaned = text.strip()
    if not cleaned:
        return {"message_intent": "unknown"}

    normalized = normalize_branch_text(cleaned)
    if _looks_like_report_submission(cleaned, normalized):
        return {"message_intent": "report_submission"}
    if not _looks_like_operational_query(cleaned, normalized):
        return {"message_intent": "unknown"}

    branch = _extract_branch(cleaned)
    report_date = _resolve_date(cleaned, reference_now=reference_now)

    if _contains_any(normalized, _ATTENDANCE_MARKERS):
        query_type = "attendance_receipt_status"
        report_type = "attendance_status"
    elif _contains_any(normalized, _BALE_MARKERS):
        query_type = "bale_summary_receipt_status"
        report_type = "bale_summary_status"
    elif _contains_any(normalized, _REPORT_MARKERS):
        query_type = "report_receipt_status"
        report_type = "report_status"
    else:
        return {"message_intent": "unknown"}

    all_branches = branch is None or "all four" in normalized or "all branches" in normalized
    return {
        "message_intent": "operational_query",
        "query_type": query_type,
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "all_branches": all_branches,
        "raw_text": cleaned,
    }


def _looks_like_report_submission(raw_text: str, normalized_text: str) -> bool:
    lines = [normalize_branch_text(line) for line in raw_text.splitlines() if line.strip()]
    if any(any(line.startswith(title) for title in _REPORT_TITLES) for line in lines):
        return True
    return "branch " in normalized_text and "date " in normalized_text and "\n" in raw_text


def _looks_like_operational_query(raw_text: str, normalized_text: str) -> bool:
    if "?" in raw_text:
        return True if _contains_any(normalized_text, _QUERY_MARKERS) else False
    if not _QUESTION_WORD_PATTERN.search(normalized_text):
        return False
    return _contains_any(normalized_text, _QUERY_MARKERS)


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _extract_branch(text: str) -> str | None:
    for pattern, branch in _BRANCH_HINT_PATTERNS:
        if pattern.search(text):
            return branch
    normalized = normalize_branch_text(text)
    for pattern, branch in _BRANCH_HINT_PATTERNS:
        if pattern.search(normalized):
            return branch
    return None


def _resolve_date(text: str, *, reference_now: datetime | None = None) -> str:
    local_now = (reference_now or datetime.now().astimezone()).astimezone()
    normalized_text = text.casefold()
    if "yesterday" in normalized_text:
        return (local_now.date() - timedelta(days=1)).isoformat()
    if "today" in normalized_text:
        return local_now.date().isoformat()

    match = _DATE_PATTERN.search(text)
    if match is not None:
        normalized = normalize_report_date(match.group(0)).normalized_value
        if normalized is not None:
            return normalized
    return local_now.date().isoformat()
