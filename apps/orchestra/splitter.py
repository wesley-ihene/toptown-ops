"""Conservative helpers for splitting mixed Orchestra work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal
import re

from apps.orchestra.intake import stable_message_hash
from packages.signal_contracts.work_item import WorkItem

SPLITTER_STAGE: Final[str] = "splitter"
SPLIT_REASON_EXPLICIT_SECTIONS: Final[str] = "explicit_section_markers"
TEXT_FIELDS: Final[tuple[str, ...]] = ("text", "body", "message", "caption", "content")

ConcreteReportType = Literal[
    "sales",
    "staff_attendance",
    "bale_summary",
    "supervisor_control",
]

SECTION_MARKERS: Final[dict[ConcreteReportType, tuple[str, ...]]] = {
    "sales": ("sales", "sales report", "day end sales report"),
    "staff_attendance": ("staff attendance", "attendance"),
    "bale_summary": ("bale summary", "bales"),
    "supervisor_control": (
        "supervisor control report",
        "supervisor control summary",
        "supervisor control",
        "supervisor summary",
        "supervisor report",
    ),
}

_NON_ALPHANUMERIC_PATTERN: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")
_SECTION_SEPARATOR_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\s*[-=_*]{3,}\s*$")
_SUPERVISOR_CONTROL_HEADER_ALIASES: Final[tuple[str, ...]] = (
    "supervisor control report",
    "supervisor control summary",
    "supervisor control",
    "supervisor summary",
)
_SUPERVISOR_SECTION_KEYWORDS: Final[tuple[str, ...]] = (
    "floor check",
    "cashier reconciled",
    "store locked",
    "checklist signed",
    "cash variance",
    "staffing issues",
    "stock issues",
    "pricing or system issues",
    "supervisor confirmation",
    "exception type",
    "action taken",
)


@dataclass(slots=True)
class SplitResult:
    """Typed split outcome for a classified work item."""

    parent_work_item: WorkItem
    child_work_items: list[WorkItem] = field(default_factory=list)
    was_split: bool = False


def split_work_item(work_item: WorkItem) -> SplitResult:
    """Split a mixed work item into conservative child work items."""

    classification = _classification_payload(work_item)
    if classification.get("report_type") != "mixed":
        return SplitResult(
            parent_work_item=work_item,
            child_work_items=[work_item],
            was_split=False,
        )

    raw_text = _extract_raw_text(work_item)
    sections = _extract_sections(raw_text)
    expected_types = _expected_child_types(classification)
    candidate_types = [report_type for report_type in expected_types if report_type in sections]

    if len(candidate_types) < 2:
        return SplitResult(
            parent_work_item=work_item,
            child_work_items=[work_item],
            was_split=False,
        )

    parent_work_item = _create_split_parent(work_item, child_count=len(candidate_types))
    child_work_items = [
        _create_split_child(
            work_item=work_item,
            report_type=report_type,
            section_text=sections[report_type],
            child_index=child_index,
            child_count=len(candidate_types),
            matched_markers=_matched_markers_for_type(classification, report_type),
        )
        for child_index, report_type in enumerate(candidate_types)
    ]

    return SplitResult(
        parent_work_item=parent_work_item,
        child_work_items=child_work_items,
        was_split=True,
    )


def _classification_payload(work_item: WorkItem) -> Mapping[str, Any]:
    """Return the classification payload as a mapping when available."""

    classification = work_item.payload.get("classification", {})
    if isinstance(classification, Mapping):
        return classification
    return {}


def _expected_child_types(classification: Mapping[str, Any]) -> list[ConcreteReportType]:
    """Return concrete child types supported by the current split scaffold."""

    matched_markers = classification.get("matched_markers", {})
    if not isinstance(matched_markers, Mapping):
        return []

    supported_types: list[ConcreteReportType] = []
    for report_type in SECTION_MARKERS:
        if report_type in matched_markers:
            supported_types.append(report_type)
    return supported_types


def _extract_sections(raw_text: str) -> dict[ConcreteReportType, str]:
    """Extract explicit section blocks from raw text."""

    raw_lines = raw_text.splitlines()
    collected_sections: dict[ConcreteReportType, list[str]] = {}
    current_type: ConcreteReportType | None = None
    previous_nonempty_line = ""

    for line_index, line in enumerate(raw_lines):
        header_match = _match_section_header(
            line,
            previous_nonempty_line=previous_nonempty_line,
            following_lines=raw_lines[line_index + 1 :],
        )
        if header_match is not None:
            current_type, inline_text = header_match
            collected_sections.setdefault(current_type, [])
            if inline_text:
                collected_sections[current_type].append(inline_text)
            previous_nonempty_line = line.strip()
            continue

        if current_type is None:
            if line.strip():
                previous_nonempty_line = line.strip()
            continue

        stripped_line = line.strip()
        if stripped_line and not _is_separator_line(stripped_line):
            collected_sections[current_type].append(stripped_line)
        if stripped_line:
            previous_nonempty_line = stripped_line

    return {
        report_type: "\n".join(lines).strip()
        for report_type, lines in collected_sections.items()
        if any(line.strip() for line in lines)
    }


def _match_section_header(
    line: str,
    *,
    previous_nonempty_line: str = "",
    following_lines: list[str] | None = None,
) -> tuple[ConcreteReportType, str] | None:
    """Return a report type when a line starts with an explicit section marker."""

    stripped_line = line.strip()
    if not stripped_line or _is_separator_line(stripped_line):
        return None

    normalized_line = _normalize_line(stripped_line)
    for report_type, markers in SECTION_MARKERS.items():
        for marker in markers:
            match = re.match(
                rf"^{_marker_pattern(marker)}(?:\s*[:\-]\s*(.*))?$",
                stripped_line,
                flags=re.IGNORECASE,
            )
            if match:
                return report_type, (match.group(1) or "").strip()
            if report_type == "supervisor_control" and (
                _looks_like_supervisor_control_title(normalized_line)
                or _is_supervisor_control_separator_boundary(
                    normalized_line=normalized_line,
                    previous_nonempty_line=previous_nonempty_line,
                    following_lines=following_lines or [],
                )
            ):
                return report_type, ""
    return None


def _marker_pattern(marker: str) -> str:
    """Return a flexible regex for one section marker."""

    return r"[\s\-]+".join(re.escape(part) for part in marker.split())


def _looks_like_supervisor_control_title(normalized_line: str) -> bool:
    """Return whether a line looks like a supervisor-control section title."""

    if normalized_line in {*_SUPERVISOR_CONTROL_HEADER_ALIASES, "supervisor report"}:
        return True
    tokens = normalized_line.split()
    if len(tokens) < 3 or tokens[:2] != ["supervisor", "control"]:
        return False
    return tokens[2] in {"report", "summary", "checklist"} and len(tokens) <= 4


def _is_supervisor_control_separator_boundary(
    *,
    normalized_line: str,
    previous_nonempty_line: str,
    following_lines: list[str],
) -> bool:
    """Return whether a separator-bounded title begins a supervisor section."""

    if not _is_separator_line(previous_nonempty_line):
        return False
    if not _looks_like_supervisor_control_alias(normalized_line):
        return False
    return _has_supervisor_section_keywords(following_lines)


def _looks_like_supervisor_control_alias(normalized_line: str) -> bool:
    """Return whether a normalized line starts with a known supervisor alias."""

    if normalized_line in _SUPERVISOR_CONTROL_HEADER_ALIASES:
        return True
    return any(normalized_line.startswith(f"{alias} ") for alias in _SUPERVISOR_CONTROL_HEADER_ALIASES)


def _has_supervisor_section_keywords(lines: list[str]) -> bool:
    """Return whether the next section contains enough supervisor evidence."""

    normalized_candidates: list[str] = []
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line:
            continue
        if _is_separator_line(stripped_line):
            break
        normalized_candidates.append(_normalize_line(stripped_line))
        if len(normalized_candidates) >= 6:
            break

    if not normalized_candidates:
        return False

    normalized_text = " ".join(normalized_candidates)
    matches = 0
    for keyword in _SUPERVISOR_SECTION_KEYWORDS:
        if keyword in normalized_text:
            matches += 1
        if matches >= 2:
            return True
    return False


def _normalize_line(value: str) -> str:
    """Return a stable comparison key for one candidate section line."""

    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", value.casefold())
    return " ".join(normalized.split())


def _is_separator_line(value: str) -> bool:
    """Return whether a line is an explicit section separator."""

    return bool(_SECTION_SEPARATOR_PATTERN.match(value.strip()))


def _create_split_parent(work_item: WorkItem, *, child_count: int) -> WorkItem:
    """Create a parent view annotated as the source of a split."""

    payload = dict(work_item.payload)
    payload["message_role"] = "split_parent"
    payload["split_reason"] = SPLIT_REASON_EXPLICIT_SECTIONS
    payload["child_count"] = child_count
    return WorkItem(kind=work_item.kind, payload=payload)


def _create_split_child(
    *,
    work_item: WorkItem,
    report_type: ConcreteReportType,
    section_text: str,
    child_index: int,
    child_count: int,
    matched_markers: list[str],
) -> WorkItem:
    """Create a child work item for a single explicit report section."""

    raw_message = _child_raw_message(work_item.payload.get("raw_message"), section_text)
    payload = dict(work_item.payload)
    payload["raw_message"] = raw_message
    payload["message_hash"] = stable_message_hash(raw_message)
    payload["classification"] = {
        "report_family": _classification_report_family(report_type),
        "report_type": report_type,
        "blocks_transactional_processing": _blocks_transactional_processing(report_type),
        "matched_markers": {report_type: matched_markers},
    }
    payload["message_role"] = "split_child"
    payload["parent_message_hash"] = str(work_item.payload.get("message_hash", ""))
    payload["child_index"] = child_index
    payload["child_count"] = child_count
    payload["split_reason"] = SPLIT_REASON_EXPLICIT_SECTIONS
    return WorkItem(kind=work_item.kind, payload=payload)


def _matched_markers_for_type(
    classification: Mapping[str, Any],
    report_type: ConcreteReportType,
) -> list[str]:
    """Return marker evidence for a concrete child report type."""

    matched_markers = classification.get("matched_markers", {})
    if not isinstance(matched_markers, Mapping):
        return []

    markers = matched_markers.get(report_type, [])
    if isinstance(markers, list):
        return [marker for marker in markers if isinstance(marker, str)]
    return []


def _classification_report_family(report_type: ConcreteReportType) -> str:
    """Return the routed family label for one split child."""

    if report_type == "supervisor_control":
        return "intelligence"
    return report_type


def _blocks_transactional_processing(report_type: ConcreteReportType) -> bool:
    """Return whether one split child should block transactional mixed handling."""

    return report_type != "supervisor_control"


def _extract_raw_text(work_item: WorkItem) -> str:
    """Extract the raw text available for explicit section splitting."""

    raw_message = work_item.payload.get("raw_message", "")
    if isinstance(raw_message, str):
        return raw_message
    if isinstance(raw_message, Mapping):
        parts = []
        for field_name in TEXT_FIELDS:
            value = raw_message.get(field_name)
            if isinstance(value, str) and value.strip():
                parts.append(value)
        return "\n".join(parts)
    return str(raw_message)


def _child_raw_message(raw_message: Any, section_text: str) -> str | dict[str, Any]:
    """Return a child raw message with section text substituted in place."""

    if isinstance(raw_message, str):
        return section_text
    if isinstance(raw_message, Mapping):
        child_raw_message = dict(raw_message)
        for field_name in TEXT_FIELDS:
            value = child_raw_message.get(field_name)
            if isinstance(value, str):
                child_raw_message[field_name] = section_text
                return child_raw_message
        child_raw_message["text"] = section_text
        return child_raw_message
    return section_text
