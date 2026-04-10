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
_STATUS_PATTERN = re.compile(r"\b(leave|off|present|absent|p|sick|suspended|suspend)\b", flags=re.IGNORECASE)


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

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
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
            elif field_name == "notes":
                parsed.notes.append(value)
            continue

        record = _parse_record_line(line)
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

        summary = _parse_summary_count_line(line)
        if summary is not None:
            status, count = summary
            parsed.declared_status_totals[status] = count
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

    canonical_status, _ = normalize_status(raw_key)
    return (canonical_status, count) if canonical_status is not None else None


def _parse_record_line(line: str) -> ParsedAttendanceRecord | None:
    record_number = None
    content = line

    numbered_match = _NUMBERED_LINE_PATTERN.match(line)
    if numbered_match is not None:
        record_number = int(numbered_match.group(1))
        content = numbered_match.group(2).strip()

    normalized_content = re.sub(r"\s+", " ", content).strip(" -|/=")
    segments = [segment.strip() for segment in re.split(r"\s*(?:\||/|=|-)\s*", normalized_content) if segment.strip()]
    if len(segments) < 2:
        return None

    status_match = _STATUS_PATTERN.search(content)
    canonical_status = _canonical_status(status_match.group(1)) if status_match is not None else "unknown"
    raw_status = status_match.group(1).strip() if status_match is not None else segments[-1]

    staff_name = None
    for segment in segments:
        segment_status = _canonical_status(segment)
        if segment_status is not None:
            continue
        if staff_name is None:
            staff_name = _clean_text(segment)
            continue

    if staff_name is None:
        if status_match is not None:
            staff_name = _clean_text(_STATUS_PATTERN.sub("", content, count=1))
        else:
            staff_name = _clean_text(" ".join(segments[:-1]))

    if not staff_name:
        return None

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


def _clean_text(value: str) -> str | None:
    cleaned = " ".join(value.replace("_", " ").split()).strip(" -|/=:,")
    return cleaned or None


def _normalize_key(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").split())
