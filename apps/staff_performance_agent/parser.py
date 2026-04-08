"""Deterministic parser for mixed-format staff performance WhatsApp reports."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from typing import Any

from apps.branch_resolver_agent.worker import resolve_branch
from apps.date_resolver_agent.worker import resolve_report_date
from apps.field_canonicalizer_agent.worker import canonicalize_field_line
from apps.header_normalizer_agent.worker import normalize_headers
from apps.hr_agent.block_detector import DetectedBlock, split_numbered_blocks
from apps.hr_agent.cleanup import cleanup_text
from apps.hr_agent.figures import PerformanceFigures, PerformanceRecord
from apps.hr_agent.normalizer import clean_name, parse_count
from apps.hr_agent.provenance import HrProvenance
from apps.hr_agent.section_resolver import resolve_section
from apps.hr_agent.staff_identity import duplicate_staff_names
from apps.hr_agent.warnings import WarningEntry, dedupe_warnings, make_warning
from apps.staff_status_resolver_agent.worker import resolve_staff_status
from packages.section_registry import resolve_section_alias
from packages.signal_contracts.work_item import WorkItem

_HEADER_SPLIT_PATTERN = re.compile(r"\s*[-/|]\s*", flags=re.IGNORECASE)
_SUMMARY_COUNT_PATTERN = re.compile(r"\b(\d+)\b")
_NUMBERED_LINE_PATTERN = re.compile(r"^\s*(\d+)\s*(?:[.)\-:]+|\s)\s*(.*)$")
_PRICE_ROOM_HEADER_PATTERN = re.compile(r"^staff who work in price room\s*:?\s*$", flags=re.IGNORECASE)
_PRICING_BY_PATTERN = re.compile(r"\bpricing\s*[-:]\s*(.+)$", flags=re.IGNORECASE)
_PAREN_CONTENT_PATTERN = re.compile(r"\(([^)]+)\)")


@dataclass(slots=True)
class PriceRoomStaffEntry:
    """One structured price-room staff entry."""

    name: str
    role: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SpecialAssignment:
    """One structured special assignment entry."""

    record_number: int
    staff_name: str
    role: str | None = None
    assignment_type: str | None = None
    pricing_by: str | None = None
    items_sold: int | None = None
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ParsedStaffPerformanceReport:
    """Structured performance parse result plus diagnostics."""

    branch: str | None = None
    branch_slug: str | None = None
    report_date: str | None = None
    records: list[PerformanceRecord] = field(default_factory=list)
    figures: PerformanceFigures = field(default_factory=PerformanceFigures)
    provenance: HrProvenance = field(default_factory=HrProvenance)
    warnings: list[WarningEntry] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    price_room_staff: list[PriceRoomStaffEntry] = field(default_factory=list)
    special_assignments: list[SpecialAssignment] = field(default_factory=list)


def parse_work_item(work_item: WorkItem) -> ParsedStaffPerformanceReport:
    """Parse one performance work item with deterministic normalization."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    lines = cleanup_text(raw_text)
    header_result = normalize_headers(raw_text)
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    branch_resolution = resolve_branch(
        header_result,
        metadata_branch_hint=metadata.get("branch_hint") if isinstance(metadata.get("branch_hint"), str) else None,
    )
    date_resolution = resolve_report_date(header_result)
    main_lines, price_room_lines = _split_price_room_lines(lines)
    blocks, remainder = split_numbered_blocks(main_lines)

    parsed = ParsedStaffPerformanceReport()
    parsed.branch = branch_resolution.raw_branch_line or branch_resolution.branch_display_name
    parsed.branch_slug = branch_resolution.branch_hint
    parsed.report_date = date_resolution.iso_date
    parsed.provenance.raw_branch = branch_resolution.raw_branch_line
    parsed.provenance.raw_date = date_resolution.raw_date
    parsed.provenance.detected_subtype = "staff_performance"
    parsed.provenance.notes.extend(
        [
            f"branch_confidence={branch_resolution.confidence}",
            f"date_confidence={date_resolution.confidence}",
        ]
    )
    parsed.diagnostics = {
        "normalized_header_candidates": header_result.normalized_lines(),
        "unmatched_lines": [],
        "remainder_lines": remainder,
        "section_resolution_stats": {
            "resolved_count": 0,
            "unresolved_count": 0,
            "unresolved_examples": [],
        },
        "special_assignment_count": 0,
        "price_room_staff_count": 0,
    }

    for block in blocks:
        if _is_special_assignment_block(block):
            assignment, warnings, unmatched_lines = _parse_special_assignment_block(block)
            parsed.warnings.extend(warnings)
            parsed.diagnostics["unmatched_lines"].extend(unmatched_lines)
            if assignment is not None:
                parsed.special_assignments.append(assignment)
            continue

        record, warnings, unmatched_lines, section_resolved = _parse_performance_block(block)
        parsed.warnings.extend(warnings)
        parsed.diagnostics["unmatched_lines"].extend(unmatched_lines)
        if record is not None:
            parsed.records.append(record)
            if record.raw_section:
                if section_resolved:
                    parsed.diagnostics["section_resolution_stats"]["resolved_count"] += 1
                else:
                    parsed.diagnostics["section_resolution_stats"]["unresolved_count"] += 1
                    unresolved_examples = parsed.diagnostics["section_resolution_stats"]["unresolved_examples"]
                    if len(unresolved_examples) < 5:
                        unresolved_examples.append(record.raw_section)

    parsed.price_room_staff = _parse_price_room_staff(price_room_lines)
    parsed.diagnostics["special_assignment_count"] = len(parsed.special_assignments)
    parsed.diagnostics["price_room_staff_count"] = len(parsed.price_room_staff)

    _apply_duplicate_warning(
        warnings=parsed.warnings,
        staff_names=[record.staff_name for record in parsed.records],
    )
    _apply_declared_totals(parsed.figures, remainder)
    _apply_section_resolution_warning(parsed)

    if not parsed.branch_slug or not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch or report date could not be resolved from the staff performance report.",
            )
        )
    if not parsed.records:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="No complete staff performance records were extracted from the report.",
            )
        )

    parsed.warnings = dedupe_warnings(parsed.warnings)
    return parsed


def _raw_text(payload: dict[str, Any]) -> str:
    """Return the raw message text for parsing."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""
    text = raw_message.get("text")
    if not isinstance(text, str):
        return ""
    return text.strip()


def _parse_performance_block(
    block: DetectedBlock,
) -> tuple[PerformanceRecord | None, list[WarningEntry], list[dict[str, Any]], bool]:
    """Parse one numbered performance block with explicit diagnostics."""

    warnings: list[WarningEntry] = []
    unmatched_lines: list[dict[str, Any]] = []
    staff_name, header_detail = _split_header_name_and_detail(block.header)
    status = resolve_staff_status(header_detail)
    raw_section = None
    role = status.role_annotation
    items_moved = None
    assisting_count = None
    notes: list[str] = []

    for line in block.lines:
        canonical = canonicalize_field_line(line)
        if canonical is not None:
            if canonical.key == "section":
                raw_section = canonical.normalized_value or raw_section
                section_status = resolve_staff_status(canonical.annotation, canonical.raw_value)
                if role is None and section_status.role_annotation is not None:
                    role = section_status.role_annotation
                if status.performance_grade is None and section_status.performance_grade is not None:
                    status.performance_grade = section_status.performance_grade
                if section_status.duty_status != "on_duty" and status.duty_status == "on_duty":
                    status.duty_status = section_status.duty_status
                if canonical.annotation:
                    notes.append(f"section_annotation:{canonical.annotation}")
                continue
            if canonical.key == "items_moved":
                items_moved = parse_count(canonical.normalized_value or "")
                continue
            if canonical.key == "assist_count":
                assisting_count = parse_count(canonical.normalized_value or "")
                continue

        stripped = line.strip()
        if stripped:
            unmatched_lines.append(
                {
                    "record_number": block.record_number,
                    "staff_name": staff_name,
                    "line": stripped,
                }
            )
            notes.append(stripped)

    if staff_name is None or (
        raw_section is None and items_moved is None and assisting_count is None and status.duty_status == "on_duty"
    ):
        warnings.append(
            make_warning(
                code="incomplete_record",
                severity="warning",
                message="A blank or incomplete numbered staff performance record was ignored.",
            )
        )
        return None, warnings, unmatched_lines, False

    canonical_section, preserved_raw_section = resolve_section(raw_section or role)
    return (
        PerformanceRecord(
            record_number=block.record_number,
            staff_name=staff_name,
            section=canonical_section,
            raw_section=preserved_raw_section,
            role=role,
            duty_status=status.duty_status,
            performance_grade=status.performance_grade,
            items_moved=items_moved or 0,
            assisting_count=assisting_count or 0,
            notes=notes,
        ),
        warnings,
        unmatched_lines,
        canonical_section is not None,
    )


def _parse_special_assignment_block(
    block: DetectedBlock,
) -> tuple[SpecialAssignment | None, list[WarningEntry], list[dict[str, Any]]]:
    """Parse one special assignment or continuation block."""

    warnings: list[WarningEntry] = []
    unmatched_lines: list[dict[str, Any]] = []
    header = block.header.strip()
    pricing_by = None
    pricing_match = _PRICING_BY_PATTERN.search(header)
    header_without_pricing = header
    if pricing_match is not None:
        pricing_by = clean_name(pricing_match.group(1))
        header_without_pricing = header[: pricing_match.start()].strip()

    paren_values = [clean_name(value) for value in _PAREN_CONTENT_PATTERN.findall(header_without_pricing)]
    paren_values = [value for value in paren_values if value]
    staff_name = clean_name(_PAREN_CONTENT_PATTERN.sub("", header_without_pricing))
    role = paren_values[0] if paren_values else None
    assignment_note = paren_values[1] if len(paren_values) > 1 else None
    items_sold = None
    notes: list[str] = []
    if assignment_note:
        notes.append(assignment_note)

    for line in block.lines:
        canonical = canonicalize_field_line(line)
        if canonical is not None and canonical.key == "items_moved":
            items_sold = parse_count(canonical.normalized_value or "")
            continue
        stripped = line.strip()
        if stripped:
            unmatched_lines.append(
                {
                    "record_number": block.record_number,
                    "staff_name": staff_name,
                    "line": stripped,
                }
            )
            notes.append(stripped)

    if staff_name is None:
        warnings.append(
            make_warning(
                code="incomplete_record",
                severity="warning",
                message="A special assignment row could not be parsed safely.",
            )
        )
        return None, warnings, unmatched_lines

    assignment_type = assignment_note.casefold().replace(" ", "_").replace("-", "_") if assignment_note else "special_assignment"
    assignment_type = re.sub(r"[^a-z0-9_]+", "", assignment_type)
    assignment_type = re.sub(r"_+", "_", assignment_type).strip("_") or "special_assignment"
    return (
        SpecialAssignment(
            record_number=block.record_number,
            staff_name=staff_name,
            role=role,
            assignment_type=assignment_type,
            pricing_by=pricing_by,
            items_sold=items_sold,
            notes=notes,
        ),
        warnings,
        unmatched_lines,
    )


def _split_header_name_and_detail(header: str) -> tuple[str | None, str | None]:
    """Split one numbered header into staff name and trailing status detail."""

    cleaned = header.strip(" .")
    if not cleaned:
        return None, None

    parts = _HEADER_SPLIT_PATTERN.split(cleaned, maxsplit=1)
    if len(parts) == 2:
        return clean_name(parts[0]), clean_name(parts[1])
    return clean_name(cleaned), None


def _apply_duplicate_warning(*, warnings: list[WarningEntry], staff_names: list[str]) -> None:
    """Append the duplicate warning once when normalized staff names repeat."""

    duplicates = duplicate_staff_names(staff_names)
    if duplicates:
        warnings.append(
            make_warning(
                code="duplicate_staff",
                severity="warning",
                message="One or more staff members appears more than once in the staff performance report.",
            )
        )


def _apply_declared_totals(figures: PerformanceFigures, lines: list[str]) -> None:
    """Parse simple summary totals from remainder lines when present."""

    for line in lines:
        lowered = line.casefold()
        matched = _SUMMARY_COUNT_PATTERN.search(line)
        if matched is None:
            continue
        count = parse_count(matched.group(1))
        if count is None:
            continue
        if "assist" in lowered and figures.declared_assisting_count is None:
            figures.declared_assisting_count = count
        elif ("item" in lowered or "moved" in lowered) and figures.declared_items_moved is None:
            figures.declared_items_moved = count
        elif "total staff" in lowered and figures.declared_record_count is None:
            figures.declared_record_count = count


def _split_price_room_lines(lines: list[str]) -> tuple[list[str], list[str]]:
    """Split main report lines from the trailing price-room block."""

    for index, line in enumerate(lines):
        if _PRICE_ROOM_HEADER_PATTERN.match(line):
            return lines[:index], lines[index + 1 :]
    return lines, []


def _parse_price_room_staff(lines: list[str]) -> list[PriceRoomStaffEntry]:
    """Parse the trailing price-room staff block."""

    staff_entries: list[PriceRoomStaffEntry] = []
    for line in lines:
        numbered_match = _NUMBERED_LINE_PATTERN.match(line)
        if numbered_match is None:
            continue
        detail = numbered_match.group(2).strip()
        if not detail:
            continue

        role = None
        notes: list[str] = []
        if "--" in detail:
            left, right = detail.split("--", 1)
            name = clean_name(left)
            role = clean_name(right)
        else:
            name = clean_name(_PAREN_CONTENT_PATTERN.sub("", detail))
            paren_values = [clean_name(value) for value in _PAREN_CONTENT_PATTERN.findall(detail)]
            if paren_values:
                notes.extend([value for value in paren_values if value])
        if name is None:
            continue
        staff_entries.append(PriceRoomStaffEntry(name=name, role=role, notes=notes))
    return staff_entries


def _is_special_assignment_block(block: DetectedBlock) -> bool:
    """Return whether a block should be parsed as a special assignment."""

    header = block.header.casefold()
    if "pricing-" in header or "pricing:" in header:
        return True
    return any("items sold" in line.casefold() for line in block.lines)


def _apply_section_resolution_warning(parsed: ParsedStaffPerformanceReport) -> None:
    """Emit one conservative warning when some usable sections remain unresolved."""

    stats = parsed.diagnostics["section_resolution_stats"]
    unresolved_count = stats["unresolved_count"]
    if unresolved_count <= 0:
        return
    parsed.warnings.append(
        make_warning(
            code="section_unresolved",
            severity="info",
            message=(
                "One or more section values were preserved in raw form because canonical mapping was uncertain."
            ),
        )
    )
