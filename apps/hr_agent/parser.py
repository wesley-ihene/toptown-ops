"""Parsers for WhatsApp-style HR reports."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from typing import Any

from apps.hr_agent.block_detector import DetectedBlock, split_numbered_blocks
from apps.hr_agent.cleanup import cleanup_text
from apps.hr_agent.date_branch_resolver import normalize_report_date, resolve_branch
from apps.hr_agent.figures import AttendanceFigures, AttendanceRecord, PerformanceFigures, PerformanceRecord
from apps.hr_agent.normalizer import clean_name, normalize_status, normalize_text, parse_count
from apps.hr_agent.provenance import HrProvenance
from apps.hr_agent.section_resolver import resolve_section
from apps.hr_agent.staff_identity import duplicate_staff_names
from apps.hr_agent.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.signal_contracts.work_item import WorkItem

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
_ITEMS_MOVED_PATTERNS = (
    re.compile(r"items?\s*moved\s*[:= -]?\s*(\d+)", flags=re.IGNORECASE),
    re.compile(r"moved\s*(\d+)\s*items?", flags=re.IGNORECASE),
)
_ASSISTING_PATTERNS = (
    re.compile(r"assisting(?:\s*count)?\s*[:= -]?\s*(\d+)", flags=re.IGNORECASE),
    re.compile(r"assist(?:ed)?\s*(\d+)", flags=re.IGNORECASE),
)
_DATE_SEARCH_PATTERN = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b")
_SUMMARY_COUNT_PATTERN = re.compile(
    r"\b(present|p|off|annual leave|anual leave|leave|suspend|suspended|absent|sick)\b\s*[:=\-]?\s*(\d+)\b",
    flags=re.IGNORECASE,
)


@dataclass(slots=True)
class ParsedStaffPerformanceReport:
    """Structured staff-performance report."""

    branch: str | None = None
    branch_slug: str | None = None
    report_date: str | None = None
    records: list[PerformanceRecord] = field(default_factory=list)
    figures: PerformanceFigures = field(default_factory=PerformanceFigures)
    provenance: HrProvenance = field(default_factory=HrProvenance)
    warnings: list[WarningEntry] = field(default_factory=list)


@dataclass(slots=True)
class ParsedStaffAttendanceReport:
    """Structured staff-attendance report."""

    branch: str | None = None
    branch_slug: str | None = None
    report_date: str | None = None
    records: list[AttendanceRecord] = field(default_factory=list)
    figures: AttendanceFigures = field(default_factory=AttendanceFigures)
    provenance: HrProvenance = field(default_factory=HrProvenance)
    warnings: list[WarningEntry] = field(default_factory=list)


def detect_report_subtype(
    payload: dict[str, Any],
    *,
    classification: object | None = None,
) -> str | None:
    """Return the best-effort HR subtype from classification and raw text."""

    if isinstance(classification, Mapping):
        report_type = classification.get("report_type")
        if report_type == "staff_performance":
            return "staff_performance"
        if report_type == "staff_attendance":
            return "staff_attendance"

    raw_text = _raw_text(payload)
    normalized = normalize_text(raw_text)
    performance_markers = (
        "items moved",
        "assisting",
        "assisting report",
        "performance",
        "off duty",
        "door guard",
        "pricing room",
        "cashier",
    )
    attendance_markers = (
        "attendance",
        "annual leave",
        "anual leave",
        "present",
        "absent",
        "sick",
        "suspend",
        "suspended",
    )

    if any(marker in normalized for marker in performance_markers):
        return "staff_performance"
    if any(marker in normalized for marker in attendance_markers):
        return "staff_attendance"
    return None


def parse_staff_performance(work_item: WorkItem) -> ParsedStaffPerformanceReport:
    """Parse a staff performance / assisting report."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    lines = cleanup_text(raw_text)
    blocks, remainder = split_numbered_blocks(lines)

    parsed = ParsedStaffPerformanceReport()
    parsed.provenance.detected_subtype = "staff_performance"
    _apply_heading(parsed, lines)

    for block in blocks:
        record, record_warnings = _parse_performance_block(block)
        parsed.warnings.extend(record_warnings)
        if record is not None:
            parsed.records.append(record)

    _apply_duplicate_warning(
        warnings=parsed.warnings,
        staff_names=[record.staff_name for record in parsed.records],
        message="One or more staff members appears more than once in the staff performance report.",
    )
    _apply_performance_declared_totals(parsed.figures, remainder)

    if not parsed.branch_slug or not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch or report date could not be resolved from the HR performance report.",
            )
        )
    if not parsed.records:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="No complete staff performance records were extracted from the HR report.",
            )
        )

    parsed.warnings = dedupe_warnings(parsed.warnings)
    return parsed


def parse_staff_attendance(work_item: WorkItem) -> ParsedStaffAttendanceReport:
    """Parse a staff attendance report."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    lines = cleanup_text(raw_text)
    blocks, remainder = split_numbered_blocks(lines)

    parsed = ParsedStaffAttendanceReport()
    parsed.provenance.detected_subtype = "staff_attendance"
    _apply_heading(parsed, lines)

    for block in blocks:
        record, record_warnings = _parse_attendance_block(block)
        parsed.warnings.extend(record_warnings)
        if record is not None:
            parsed.records.append(record)

    parsed.figures.declared_status_totals = _parse_attendance_declared_totals(remainder)
    _apply_duplicate_warning(
        warnings=parsed.warnings,
        staff_names=[record.staff_name for record in parsed.records],
        message="One or more staff members appears more than once in the staff attendance report.",
    )

    if not parsed.branch_slug or not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch or report date could not be resolved from the HR attendance report.",
            )
        )
    if not parsed.records:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="No complete staff attendance records were extracted from the HR report.",
            )
        )

    parsed.warnings = dedupe_warnings(parsed.warnings)
    return parsed


def _raw_text(payload: dict[str, Any]) -> str:
    """Return the strict raw-message text field when available."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""

    text = raw_message.get("text")
    if not isinstance(text, str):
        return ""
    return text.strip()


def _apply_heading(parsed: ParsedStaffPerformanceReport | ParsedStaffAttendanceReport, lines: list[str]) -> None:
    """Resolve branch and report date from report heading lines."""

    heading_lines = lines[:8]
    for index, line in enumerate(heading_lines):
        match = _KEY_VALUE_PATTERN.match(line)
        if match is not None:
            key = normalize_text(match.group(1))
            value = match.group(2).strip()
            if key == "branch":
                parsed.branch, parsed.branch_slug = resolve_branch(value)
                parsed.provenance.raw_branch = value
            elif key == "date":
                parsed.report_date = normalize_report_date(value)
                parsed.provenance.raw_date = value
            continue

        normalized_line = normalize_text(line)
        if parsed.branch is None and "branch" in normalized_line:
            branch_candidate = line
            if index + 1 < len(heading_lines):
                next_line = heading_lines[index + 1]
                normalized_next_line = normalize_text(next_line)
                if (
                    next_line.strip()
                    and _DATE_SEARCH_PATTERN.search(next_line) is None
                    and "attendance" not in normalized_next_line
                    and "staff" not in normalized_next_line
                    and "top town clothing" not in normalized_next_line
                ):
                    branch_candidate = f"{line} {next_line}"
            parsed.branch, parsed.branch_slug = resolve_branch(branch_candidate)
            parsed.provenance.raw_branch = branch_candidate
        if parsed.report_date is None:
            matched = _DATE_SEARCH_PATTERN.search(line)
            if matched is not None:
                parsed.report_date = normalize_report_date(matched.group(0))
                parsed.provenance.raw_date = matched.group(0)


def _parse_performance_block(block: DetectedBlock) -> tuple[PerformanceRecord | None, list[WarningEntry]]:
    """Parse one numbered staff performance block."""

    warnings: list[WarningEntry] = []
    staff_name, header_detail = _split_header_name_and_detail(block.header)
    raw_section = header_detail
    role = header_detail
    items_moved = None
    assisting_count = None
    duty_status = "on_duty"
    notes: list[str] = []

    if header_detail is not None and normalize_text(header_detail) == "off":
        duty_status = "off_duty"
        raw_section = None
        role = None

    for line in block.lines:
        match = _KEY_VALUE_PATTERN.match(line)
        if match is not None:
            key = normalize_text(match.group(1))
            value = match.group(2).strip()
            if key in {"staff", "name", "staff name"}:
                staff_name = clean_name(value) or staff_name
            elif key in {"section", "dept", "department", "role", "position"}:
                if normalize_text(value) == "off":
                    duty_status = "off_duty"
                    if key in {"role", "position"}:
                        role = None
                else:
                    raw_section = value
                    if key in {"role", "position"}:
                        role = value
            elif "total items assistin" in key:
                items_moved = parse_count(value)
            elif "assist" in key:
                assisting_count = parse_count(value)
            elif "item" in key or "moved" in key:
                items_moved = parse_count(value)
            elif "status" in key or "duty" in key:
                if "off" in normalize_text(value):
                    duty_status = "off_duty"
                else:
                    notes.append(value)
            else:
                notes.append(line)
            continue

        normalized_line = normalize_text(line)
        inline_items = _extract_count(line, _ITEMS_MOVED_PATTERNS)
        inline_assisting = _extract_count(line, _ASSISTING_PATTERNS)
        if inline_items is not None and items_moved is None:
            items_moved = inline_items
        elif inline_assisting is not None and assisting_count is None:
            assisting_count = inline_assisting
        elif "off duty" in normalized_line:
            duty_status = "off_duty"
        elif raw_section is None:
            raw_section = line
        else:
            notes.append(line)

    if not staff_name and block.header:
        header_without_detail = block.header.split("-", 1)[0].split("/", 1)[0]
        staff_name = clean_name(header_without_detail) or staff_name

    if staff_name is None or (
        not raw_section and items_moved is None and assisting_count is None and duty_status == "on_duty"
    ):
        warnings.append(
            make_warning(
                code="incomplete_record",
                severity="warning",
                message="A blank or incomplete numbered staff performance record was ignored.",
            )
        )
        return None, warnings

    section_source = raw_section or role
    if duty_status == "off_duty" and section_source is not None and normalize_text(section_source) == "off":
        section_source = None

    canonical_section, preserved_raw_section = resolve_section(section_source)

    return (
        PerformanceRecord(
            record_number=block.record_number,
            staff_name=staff_name,
            section=canonical_section,
            raw_section=preserved_raw_section,
            role=role,
            duty_status=duty_status,
            items_moved=items_moved or 0,
            assisting_count=assisting_count or 0,
            notes=notes,
        ),
        warnings,
    )


def _parse_attendance_block(block: DetectedBlock) -> tuple[AttendanceRecord | None, list[WarningEntry]]:
    """Parse one numbered staff attendance block."""

    warnings: list[WarningEntry] = []
    staff_name = None
    raw_section = None
    status = None
    raw_status = None

    if block.header:
        if "=" in block.header:
            left, right = block.header.split("=", 1)
            staff_name = clean_name(left)
            trailing = clean_name(right)
            candidate_status, candidate_raw_status = normalize_status(right)
        else:
            staff_name, trailing = _split_header_name_and_detail(block.header)
            candidate_status, candidate_raw_status = normalize_status(block.header)
        if candidate_status is not None:
            status = candidate_status
            raw_status = candidate_raw_status
            if trailing is not None:
                trailing_status, _ = normalize_status(trailing)
                if trailing_status != candidate_status:
                    raw_section = trailing
        elif trailing is not None:
            trailing_status, trailing_raw_status = normalize_status(trailing)
            if trailing_status is not None:
                status = trailing_status
                raw_status = trailing_raw_status
            else:
                raw_section = trailing

    for line in block.lines:
        match = _KEY_VALUE_PATTERN.match(line)
        if match is not None:
            key = normalize_text(match.group(1))
            value = match.group(2).strip()
            if key in {"staff", "name", "staff name"}:
                staff_name = clean_name(value) or staff_name
            elif key in {"section", "dept", "department", "role", "position"}:
                raw_section = value
            elif key == "status":
                status, raw_status = normalize_status(value)
            continue

        if status is None:
            inline_status, inline_raw_status = normalize_status(line)
            if inline_status is not None:
                status = inline_status
                raw_status = inline_raw_status

    if staff_name is None and block.header:
        text = block.header
        if raw_status is not None:
            text = re.sub(rf"\b{re.escape(raw_status)}\b", "", text, flags=re.IGNORECASE)
        staff_name = clean_name(text)

    if staff_name is None or status is None:
        warnings.append(
            make_warning(
                code="incomplete_record",
                severity="warning",
                message="A blank or incomplete numbered staff attendance record was ignored.",
            )
        )
        if status is None:
            warnings.append(
                make_warning(
                    code="attendance_anomaly",
                    severity="warning",
                    message="One or more attendance rows uses an unknown or missing status.",
                )
            )
        return None, warnings

    canonical_section, preserved_raw_section = resolve_section(raw_section)

    return (
        AttendanceRecord(
            record_number=block.record_number,
            staff_name=staff_name,
            status=status,
            raw_status=raw_status,
            section=canonical_section,
            raw_section=preserved_raw_section,
        ),
        warnings,
    )


def _split_header_name_and_detail(header: str) -> tuple[str | None, str | None]:
    """Split a numbered header line into a name candidate and trailing detail."""

    if not header.strip():
        return None, None

    for separator in (" - ", " / ", " | "):
        if separator in header:
            left, right = header.split(separator, 1)
            return clean_name(left), clean_name(right)

    paren_match = re.match(r"^(.*?)\(([^)]+)\)\s*$", header)
    if paren_match is not None:
        return clean_name(paren_match.group(1)), clean_name(paren_match.group(2))

    return clean_name(header), None


def _extract_count(line: str, patterns: tuple[re.Pattern[str], ...]) -> int | None:
    """Extract a count from a free-form line using the provided patterns."""

    for pattern in patterns:
        match = pattern.search(line)
        if match is not None:
            return parse_count(match.group(1))
    return None


def _apply_performance_declared_totals(figures: PerformanceFigures, lines: list[str]) -> None:
    """Parse declared performance grand totals from non-record summary lines."""

    in_grand_total = False
    for line in lines:
        normalized = normalize_text(line)
        if normalized.startswith("g/total") or normalized.startswith("grand total"):
            in_grand_total = True
            continue
        matched_count = parse_count(line)
        if matched_count is None:
            continue
        if in_grand_total and "assist" in normalized:
            figures.declared_assisting_count = matched_count
            continue
        if in_grand_total and ("item" in normalized or "moved" in normalized):
            figures.declared_items_moved = matched_count
            continue
        if "grand total" in normalized and "assist" in normalized:
            figures.declared_assisting_count = matched_count
        elif "grand total" in normalized and ("item" in normalized or "moved" in normalized):
            figures.declared_items_moved = matched_count
        elif ("total items" in normalized or "items moved total" in normalized) and figures.declared_items_moved is None:
            figures.declared_items_moved = matched_count
        elif ("total assisting" in normalized or "assisting total" in normalized) and figures.declared_assisting_count is None:
            figures.declared_assisting_count = matched_count
        elif "total staff" in normalized or "staff total" in normalized:
            figures.declared_record_count = matched_count


def _parse_attendance_declared_totals(lines: list[str]) -> dict[str, int]:
    """Parse declared attendance summary totals from non-record summary lines."""

    declared_totals: dict[str, int] = {}
    for line in lines:
        for matched in _SUMMARY_COUNT_PATTERN.finditer(line):
            canonical_status, _ = normalize_status(matched.group(1))
            count = parse_count(matched.group(2))
            if canonical_status is not None and count is not None:
                declared_totals[canonical_status] = count
    return declared_totals


def _apply_duplicate_warning(
    *,
    warnings: list[WarningEntry],
    staff_names: list[str],
    message: str,
) -> None:
    """Append a duplicate staff warning when normalized names repeat."""

    if duplicate_staff_names(staff_names):
        warnings.append(
            make_warning(
                code="duplicate_staff_entry",
                severity="warning",
                message=message,
            )
        )
