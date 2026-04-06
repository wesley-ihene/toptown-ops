"""Conservative text-based classification helpers for Orchestra intake."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import re
from typing import Any, Final, Literal

from packages.signal_contracts.work_item import WorkItem

CLASSIFIER_STAGE: Final[str] = "classifier"

ReportType = Literal[
    "sales",
    "staff_attendance",
    "bale_summary",
    "supervisor_control",
    "mixed",
    "unknown",
]

DetectableReportType = Literal[
    "sales",
    "staff_attendance",
    "bale_summary",
    "supervisor_control",
]

REPORT_MARKERS: Final[dict[DetectableReportType, tuple[str, ...]]] = {
    "sales": (
        "sales",
        "sale",
        "revenue",
        "turnover",
        "invoice",
        "receipt",
        "cash up",
    ),
    "staff_attendance": (
        "attendance",
        "present",
        "absent",
        "late",
        "shift",
        "clock in",
        "clock out",
    ),
    "bale_summary": (
        "bale",
        "bales",
        "bale summary",
        "bale count",
        "kg",
        "weight",
        "tonnage",
    ),
    "supervisor_control": (
        "supervisor",
        "control",
        "inspection",
        "checklist",
        "approved",
        "issue log",
        "compliance",
    ),
}

_NON_ALPHANUMERIC_PATTERN: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class ClassificationResult:
    """Typed outcome for raw work item classification."""

    report_type: ReportType
    matched_markers: dict[DetectableReportType, list[str]] = field(default_factory=dict)
    normalized_text: str = ""

    def to_payload(self) -> dict[str, Any]:
        """Convert the classification result into a JSON-safe payload block."""

        return {
            "report_type": self.report_type,
            "matched_markers": self.matched_markers,
            "normalized_text": self.normalized_text,
        }


def normalize_text(text: str) -> str:
    """Normalize free-form text for conservative marker matching."""

    lowered_text = text.casefold()
    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", lowered_text)
    return " ".join(normalized.split())


def has_marker(text: str, marker: str) -> bool:
    """Return whether a normalized marker exists in normalized text."""

    normalized_marker = normalize_text(marker)
    if not normalized_marker:
        return False
    return f" {normalized_marker} " in f" {text} "


def matched_markers(text: str, markers: Sequence[str]) -> list[str]:
    """Return all markers that appear in normalized text."""

    return [marker for marker in markers if has_marker(text, marker)]


def classify_text(text: str) -> ClassificationResult:
    """Classify normalized text into a conservative report type."""

    normalized_text = normalize_text(text)
    matches_by_type: dict[DetectableReportType, list[str]] = {}

    for report_type, markers in REPORT_MARKERS.items():
        matches = matched_markers(normalized_text, markers)
        if matches:
            matches_by_type[report_type] = matches

    matched_types = list(matches_by_type)
    if len(matched_types) > 1:
        report_type: ReportType = "mixed"
    elif len(matched_types) == 1:
        report_type = matched_types[0]
    else:
        report_type = "unknown"

    return ClassificationResult(
        report_type=report_type,
        matched_markers=matches_by_type,
        normalized_text=normalized_text,
    )


def classify_work_item(work_item: WorkItem) -> ClassificationResult:
    """Classify a raw work item and attach the result to its payload."""

    raw_text = _extract_text_from_work_item(work_item)
    result = classify_text(raw_text)
    work_item.payload["classification"] = result.to_payload()
    return result


def _extract_text_from_work_item(work_item: WorkItem) -> str:
    """Extract classifier input text from a raw work item payload."""

    raw_message = work_item.payload.get("raw_message", "")
    if isinstance(raw_message, str):
        return raw_message
    if isinstance(raw_message, Mapping):
        text_fields = []
        for field_name in ("text", "body", "message", "caption", "content"):
            value = raw_message.get(field_name)
            if isinstance(value, str) and value.strip():
                text_fields.append(value)
        if text_fields:
            return " ".join(text_fields)
        fallback_values = [
            value.strip()
            for value in raw_message.values()
            if isinstance(value, str) and value.strip()
        ]
        return " ".join(fallback_values)
    return str(raw_message)
