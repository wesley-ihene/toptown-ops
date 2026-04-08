"""Detect and split explicit mixed-content reports into routed child sections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any

from packages.report_registry import route_for_family
from packages.signal_contracts.work_item import WorkItem

TEXT_FIELDS = ("text", "body", "message", "caption", "content")
SPLIT_STRATEGY = "explicit_report_headers"

SECTION_HEADERS: dict[str, tuple[str, ...]] = {
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
    ),
    "staff_performance": (
        "staff performance report",
        "staff assisting customers",
        "staff assisting report",
    ),
}

_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class SplitChildPlan:
    """One deterministic split child section plus lineage."""

    report_family: str
    target_agent: str
    specialist_report_type: str
    raw_text: str
    child_index: int
    child_count: int
    header_line_number: int
    header_line: str
    lineage: dict[str, Any] = field(default_factory=dict)
    evidence: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MixedSplitPlan:
    """Result of mixed report detection and splitting."""

    is_mixed: bool
    split_strategy: str | None = None
    common_prefix_lines: list[str] = field(default_factory=list)
    child_plans: list[SplitChildPlan] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)


def detect_and_split_mixed_report(work_item: WorkItem) -> MixedSplitPlan:
    """Detect explicit mixed-content sections and build routed child plans."""

    raw_text = _extract_raw_text(work_item)
    raw_lines = raw_text.splitlines()
    header_matches = _find_section_headers(raw_lines)
    distinct_families = []
    for family, *_ in header_matches:
        if family not in distinct_families:
            distinct_families.append(family)
    if len(distinct_families) < 2:
        return MixedSplitPlan(is_mixed=False)

    first_header_index = header_matches[0][1]
    common_prefix_lines = [line.rstrip() for line in raw_lines[:first_header_index] if line.strip()]
    parent_message_hash = _message_hash_from_work_item(work_item)
    raw_sha256 = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    child_plans: list[SplitChildPlan] = []
    for child_index, (family, start_index, raw_header, line_number) in enumerate(header_matches):
        route = route_for_family(family)
        if route.target_agent is None or route.specialist_type is None:
            continue
        end_index = len(raw_lines)
        for _, next_start_index, _, _ in header_matches[child_index + 1 :]:
            if next_start_index > start_index:
                end_index = next_start_index
                break
        section_lines = raw_lines[start_index:end_index]
        child_raw_text = "\n".join(common_prefix_lines + [line.rstrip() for line in section_lines if line.strip()]).strip()
        if not child_raw_text:
            continue
        lineage = {
            "message_role": "split_child",
            "split_strategy": SPLIT_STRATEGY,
            "parent_message_hash": parent_message_hash,
            "parent_raw_sha256": raw_sha256,
            "child_index": len(child_plans),
            "child_count": 0,
            "child_report_family": family,
            "header_line_number": line_number,
        }
        child_plans.append(
            SplitChildPlan(
                report_family=family,
                target_agent=route.target_agent,
                specialist_report_type=route.specialist_type,
                raw_text=child_raw_text,
                child_index=len(child_plans),
                child_count=0,
                header_line_number=line_number,
                header_line=raw_header,
                lineage=lineage,
                evidence=[f"section_header:{line_number}:{raw_header}"],
            )
        )

    if len(child_plans) < 2:
        return MixedSplitPlan(is_mixed=False)

    for index, child_plan in enumerate(child_plans):
        child_plan.child_index = index
        child_plan.child_count = len(child_plans)
        child_plan.lineage["child_index"] = index
        child_plan.lineage["child_count"] = len(child_plans)

    return MixedSplitPlan(
        is_mixed=True,
        split_strategy=SPLIT_STRATEGY,
        common_prefix_lines=common_prefix_lines,
        child_plans=child_plans,
        evidence=[f"mixed_sections:{','.join(child.report_family for child in child_plans)}"],
    )


def build_split_child_work_item(
    work_item: WorkItem,
    child_plan: SplitChildPlan,
) -> WorkItem:
    """Build one child work item from a split plan."""

    payload = dict(work_item.payload)
    payload["raw_message"] = _child_raw_message(payload.get("raw_message"), child_plan.raw_text)
    payload["classification"] = {
        "report_family": child_plan.report_family,
        "report_type": child_plan.specialist_report_type,
        "confidence": 1.0,
        "evidence": list(child_plan.evidence),
    }
    payload["routing"] = {
        "classification": child_plan.report_family,
        "target_agent": child_plan.target_agent,
        "route_status": "routed",
        "route_reason": "fanout_split_child",
        "branch_hint": _metadata_branch_hint(payload),
        "report_date": None,
        "raw_report_date": None,
        "confidence": 1.0,
        "evidence": list(child_plan.evidence),
        "normalized_header_candidates": [],
        "review_reason": None,
        "specialist_report_type": child_plan.specialist_report_type,
        "lineage": dict(child_plan.lineage),
    }
    payload["lineage"] = dict(child_plan.lineage)
    return WorkItem(kind=work_item.kind, payload=payload)


def _find_section_headers(raw_lines: list[str]) -> list[tuple[str, int, str, int]]:
    """Return explicit report header locations in original line order."""

    matches: list[tuple[str, int, str, int]] = []
    for index, raw_line in enumerate(raw_lines):
        normalized = _normalize_line(raw_line)
        if not normalized:
            continue
        for family, headers in SECTION_HEADERS.items():
            if any(_is_header_match(normalized, header) for header in headers):
                matches.append((family, index, raw_line.strip(), index + 1))
                break
    return _dedupe_consecutive_families(matches)


def _dedupe_consecutive_families(
    matches: list[tuple[str, int, str, int]],
) -> list[tuple[str, int, str, int]]:
    """Collapse immediately repeated family headers into one section boundary."""

    deduped: list[tuple[str, int, str, int]] = []
    for match in matches:
        if deduped and deduped[-1][0] == match[0]:
            continue
        deduped.append(match)
    return deduped


def _normalize_line(value: str) -> str:
    """Normalize one candidate section header line."""

    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", value.casefold())
    return " ".join(normalized.split())


def _is_header_match(normalized_line: str, header: str) -> bool:
    """Return whether a normalized line matches one explicit section header."""

    normalized_header = _normalize_line(header)
    return (
        normalized_line == normalized_header
        or normalized_line.startswith(f"{normalized_header} ")
    )


def _extract_raw_text(work_item: WorkItem) -> str:
    """Extract raw text from a work item for split detection."""

    raw_message = work_item.payload.get("raw_message", "")
    if isinstance(raw_message, str):
        return raw_message
    if isinstance(raw_message, Mapping):
        text_fields = []
        for field_name in TEXT_FIELDS:
            value = raw_message.get(field_name)
            if isinstance(value, str) and value.strip():
                text_fields.append(value)
        return "\n".join(text_fields)
    return str(raw_message)


def _child_raw_message(raw_message: Any, section_text: str) -> str | dict[str, Any]:
    """Substitute split section text into a raw-message payload."""

    if isinstance(raw_message, str):
        return section_text
    if isinstance(raw_message, Mapping):
        child_raw_message = dict(raw_message)
        for field_name in TEXT_FIELDS:
            if isinstance(child_raw_message.get(field_name), str):
                child_raw_message[field_name] = section_text
                return child_raw_message
        child_raw_message["text"] = section_text
        return child_raw_message
    return section_text


def _message_hash_from_work_item(work_item: WorkItem) -> str:
    """Return or synthesize a stable message hash for parent lineage."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    value = payload.get("message_hash")
    if isinstance(value, str) and value.strip():
        return value

    raw_message = payload.get("raw_message")
    if isinstance(raw_message, str):
        canonical = raw_message
    else:
        canonical = json.dumps(raw_message, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _metadata_branch_hint(payload: dict[str, Any]) -> str | None:
    """Return branch hint from payload metadata when present."""

    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    value = metadata.get("branch_hint")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
