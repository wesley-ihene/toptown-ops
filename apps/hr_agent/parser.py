"""Conservative parser for WhatsApp-style HR attendance work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from typing import Any

from apps.hr_agent.date_branch_resolver import normalize_report_date, resolve_branch
from apps.hr_agent.normalizer import normalize_status, parse_count
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.normalization.engine import normalize_report
from packages.normalization.labels import internal_field_name
from packages.signal_contracts.work_item import WorkItem

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
_NUMBERED_LINE_PATTERN = re.compile(r"^\s*(\d+)\s*(?:[.)\-:]+|\s)\s*(.+?)\s*$")
_STRICT_RECORD_PATTERN = re.compile(r"^\s*(?P<staff>.+?)\s*(?:=|--+|-|:|\||/)\s*(?P<status>.+?)\s*$")
_DATE_ONLY_PATTERN = re.compile(r"^\s*\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}\s*$")
_WEEKDAY_DATE_PATTERN = re.compile(
    r"^\s*(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b.*\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}\s*$",
    flags=re.IGNORECASE,
)
_NOTE_HEADER_PATTERN = re.compile(r"^\s*(?:notes?|remarks?)\s*:?\s*(.*?)\s*$", flags=re.IGNORECASE)
_SUMMARY_METRIC_ALIASES = {
    "total staff": "total_staff",
    "total staffs": "total_staff",
    "staff present": "staff_present",
    "staffs present": "staff_present",
    "total staffs present": "staff_present",
    "present": "staff_present",
    "p": "staff_present",
    "not at work": "not_at_work",
    "staff off": "staff_off",
    "staffs day off": "staff_off",
    "day off": "staff_off",
    "off": "staff_off",
    "off duty": "staff_off",
    "staffs lay off": "staff_off",
    "suspend": "suspend",
    "suspended": "suspend",
    "staffs suspend": "suspend",
    "absent": "absent",
    "staffs absent with notice": "absent",
    "staffs absent without": "absent",
    "leave": "leave",
    "staffs on leavebreak": "leave",
    "on leave": "leave",
    "on leavebreak": "leave",
    "sick": "sick",
    "staffs sick": "sick",
}
_DECLARED_STATUS_KEYS = {
    "staff_present": "present",
    "not_at_work": "not_at_work",
    "staff_off": "off",
    "suspend": "suspend",
    "absent": "absent",
    "leave": "leave",
    "sick": "sick",
}
_NON_RECORD_PREFIXES = (
    "top town",
    "branch",
    "date",
    "staff attendance",
    "attendance",
    "total",
    "staff present",
    "staff off",
    "not at work",
    "suspend",
    "absent",
    "leave",
    "sick",
    "note",
    "notes",
    "remark",
    "remarks",
    "thankyou",
    "thank you",
    "thanks",
)


@dataclass(slots=True)
class ParsedAttendanceRecord:
    """One attendance row extracted from raw text."""

    staff_name: str
    status: str
    raw_status: str | None
    record_number: int | None = None


@dataclass(slots=True)
class ParsedHrReport:
    """Structured HR attendance parse result."""

    branch: str | None = None
    branch_slug: str | None = None
    report_date: str | None = None
    records: list[ParsedAttendanceRecord] = field(default_factory=list)
    declared_status_totals: dict[str, int] = field(default_factory=dict)
    declared_summary_metrics: dict[str, int] = field(default_factory=dict)
    declared_total_staff: int | None = None
    raw_branch: str | None = None
    raw_date: str | None = None
    notes: list[str] = field(default_factory=list)
    warnings: list[WarningEntry] = field(default_factory=list)


def parse_work_item(work_item: WorkItem) -> ParsedHrReport:
    """Parse one routed HR attendance work item into a structured view."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    parsed = ParsedHrReport()
    _seed_routing_metadata(parsed, payload)
    numbered_record_mode = _has_numbered_attendance_rows(raw_text)
    note_section_active = False

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            note_section_active = False
            continue

        note_match = _NOTE_HEADER_PATTERN.match(line)
        if note_match is not None:
            note_section_active = True
            inline_note = _clean_text(note_match.group(1) or "")
            if inline_note:
                parsed.notes.append(inline_note)
            continue

        metadata = _parse_metadata_line(line)
        if metadata is not None:
            field_name, value = metadata
            if field_name == "branch":
                parsed.raw_branch = value
                parsed.branch, parsed.branch_slug = resolve_branch(value)
            elif field_name == "report_date":
                parsed.raw_date = value
                parsed.report_date = normalize_report_date(value)
            elif field_name == "total_staff":
                parsed.declared_total_staff = value
                parsed.declared_summary_metrics["total_staff"] = value
            elif field_name == "notes":
                parsed.notes.append(value)
            continue

        explicit_date = _parse_explicit_date_line(line)
        if explicit_date is not None:
            parsed.raw_date = line
            parsed.report_date = explicit_date
            continue

        summary = _parse_summary_count_line(line)
        if summary is not None:
            metric_name, count = summary
            parsed.declared_summary_metrics[metric_name] = count
            if metric_name == "total_staff":
                parsed.declared_total_staff = count
            declared_status_key = _DECLARED_STATUS_KEYS.get(metric_name)
            if declared_status_key is not None:
                parsed.declared_status_totals[declared_status_key] = count
            continue

        if note_section_active:
            parsed.notes.append(line)
            continue

        record = _parse_record_line(line, allow_unnumbered=not numbered_record_mode)
        if record is not None:
            if record.status == "unknown":
                parsed.warnings.append(
                    make_warning(
                        code="unknown_attendance_status",
                        severity="warning",
                        message=f"Attendance status could not be normalized for staff line `{record.staff_name}`.",
                    )
                )
            parsed.records.append(record)
            continue

        parsed.notes.append(line)

    if not parsed.branch_slug:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch could not be resolved from the HR attendance report.",
            )
        )
    if not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Report date could not be resolved from the HR attendance report.",
            )
        )
    if not parsed.records and not parsed.declared_status_totals:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="No attendance rows or declared attendance totals were extracted from the HR report.",
            )
        )

    parsed.warnings = dedupe_warnings(parsed.warnings)
    return parsed


def _seed_routing_metadata(parsed: ParsedHrReport, payload: dict[str, Any]) -> None:
    """Apply orchestrator-resolved branch/date hints before line parsing."""

    routing = payload.get("routing")
    if not isinstance(routing, Mapping):
        return

    branch_hint = routing.get("branch_hint")
    if isinstance(branch_hint, str) and branch_hint.strip():
        parsed.branch_slug = branch_hint.strip()
        if parsed.branch is None:
            parsed.branch = branch_hint.strip()
        if parsed.raw_branch is None:
            parsed.raw_branch = branch_hint.strip()

    normalized_report_date = routing.get("normalized_report_date") or routing.get("report_date")
    if isinstance(normalized_report_date, str) and normalized_report_date.strip():
        parsed.report_date = normalized_report_date.strip()
        if parsed.raw_date is None:
            raw_report_date = routing.get("raw_report_date")
            parsed.raw_date = raw_report_date.strip() if isinstance(raw_report_date, str) and raw_report_date.strip() else normalized_report_date.strip()


def _parse_metadata_line(line: str) -> tuple[str, Any] | None:
    match = _KEY_VALUE_PATTERN.match(line)
    if match is None:
        return None

    raw_key = match.group(1).strip()
    raw_value = match.group(2).strip()
    field_name = internal_field_name(raw_key, report_family="attendance")
    if field_name == "total_staff":
        count = parse_count(raw_value)
        return (field_name, count) if count is not None else None
    if field_name in {"branch", "report_date", "notes"}:
        return field_name, raw_value
    return None


def _parse_summary_count_line(line: str) -> tuple[str, int] | None:
    match = _KEY_VALUE_PATTERN.match(line)
    if match is None:
        return None

    raw_key = match.group(1).strip()
    raw_value = match.group(2).strip()
    count = parse_count(raw_value)
    if count is None:
        return None

    metric_name = _SUMMARY_METRIC_ALIASES.get(_normalize_key(raw_key))
    return (metric_name, count) if metric_name is not None else None


def _parse_record_line(line: str, *, allow_unnumbered: bool) -> ParsedAttendanceRecord | None:
    record_number = None
    numbered = False

    numbered_match = _NUMBERED_LINE_PATTERN.match(line)
    if numbered_match is not None:
        record_number = int(numbered_match.group(1))
        content = numbered_match.group(2).strip()
        numbered = True
    elif allow_unnumbered:
        content = line
    else:
        return None

    record_parts = _split_record_content(content)
    if record_parts is None:
        return None

    staff_name, raw_status = record_parts
    if _looks_like_non_record_label(staff_name, numbered=numbered):
        return None

    canonical_status = _canonical_status(raw_status) or "unknown"

    return ParsedAttendanceRecord(
        record_number=record_number,
        staff_name=staff_name,
        status=canonical_status,
        raw_status=raw_status,
    )


def _raw_text(payload: dict[str, Any]) -> str:
    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""
    text = raw_message.get("normalized_text")
    if not isinstance(text, str):
        text = raw_message.get("text")
    if not isinstance(text, str):
        return ""
    stripped = text.strip()
    if isinstance(raw_message.get("normalized_text"), str):
        return stripped
    normalization = normalize_report(
        stripped,
        report_family="attendance",
        routing_context=payload.get("routing") if isinstance(payload.get("routing"), Mapping) else None,
    )
    return (normalization.normalized_text or stripped).strip()


def _canonical_status(value: str) -> str | None:
    canonical_status, _ = normalize_status(value)
    return canonical_status


def _parse_explicit_date_line(line: str) -> str | None:
    stripped = line.strip()
    normalized_key = _normalize_key(stripped)
    if _DATE_ONLY_PATTERN.match(stripped) or _WEEKDAY_DATE_PATTERN.match(stripped) or normalized_key.startswith("date "):
        return normalize_report_date(stripped)
    return None


def _split_record_content(content: str) -> tuple[str, str] | None:
    match = _STRICT_RECORD_PATTERN.match(content)
    if match is None:
        return None
    staff_name = _clean_text(match.group("staff"))
    raw_status = _clean_text(match.group("status"))
    if not staff_name or not raw_status:
        return None
    return staff_name, raw_status


def _has_numbered_attendance_rows(raw_text: str) -> bool:
    return any(_NUMBERED_LINE_PATTERN.match(line.strip()) for line in raw_text.splitlines())


def _looks_like_non_record_label(value: str, *, numbered: bool) -> bool:
    normalized = _normalize_key(value)
    if not normalized:
        return True
    if any(char.isdigit() for char in normalized):
        return True
    if normalized.startswith(_NON_RECORD_PREFIXES):
        return True
    if not numbered and len(normalized.split()) == 1:
        return True
    return False


def _clean_text(value: str) -> str | None:
    cleaned = " ".join(value.replace("_", " ").split()).strip(" -|/=:,")
    return cleaned or None


def _normalize_key(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").split())
