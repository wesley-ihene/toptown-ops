"""Conservative parser for WhatsApp-style HR attendance work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from typing import Any

from apps.hr_agent.date_branch_resolver import normalize_report_date, resolve_branch
from apps.hr_agent.normalizer import normalize_key_text, normalize_status, parse_count, strip_formatting_noise
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.normalization.engine import normalize_report
from packages.normalization.labels import internal_field_name
from packages.signal_contracts.work_item import WorkItem

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
_NUMBERED_LINE_PATTERN = re.compile(r"^\s*(\d+)\s*(?:[.)\-:]+|\s)\s*(.+?)\s*$")
_STRICT_RECORD_PATTERN = re.compile(r"^\s*(?P<staff>.+?)\s*(?:=|--+|-|:|\||/)\s*(?P<status>.+?)\s*$")
_CONTINUATION_STATUS_PATTERN = re.compile(r"^\s*(?:=|--+|-|:|\||/)\s*(?P<status>.+?)\s*$")
_DATE_ONLY_PATTERN = re.compile(r"^\s*\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}\s*$")
_WEEKDAY_DATE_PATTERN = re.compile(
    r"^\s*(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b.*\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}\s*[.,]?\s*$",
    flags=re.IGNORECASE,
)
_DATE_FALLBACK_PATTERN = re.compile(r"\b\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}\b")
_NOTE_HEADER_PATTERN = re.compile(r"^\s*(?:notes?|remarks?|notices?)\s*:?\s*(.*?)\s*$", flags=re.IGNORECASE)
_SUMMARY_METRIC_ALIASES = {
    "total staff": "total_staff",
    "total staffs": "total_staff",
    "total current staff": "total_staff",
    "current staff": "total_staff",
    "staff total": "total_staff",
    "staff present": "staff_present",
    "staff press": "staff_present",
    "staff pres": "staff_present",
    "total staff present": "staff_present",
    "staffs present": "staff_present",
    "total staff press": "staff_present",
    "total staff pres": "staff_present",
    "total staffs present": "staff_present",
    "total staffs pres": "staff_present",
    "total staffs press": "staff_present",
    "present": "staff_present",
    "press": "staff_present",
    "pres": "staff_present",
    "p": "staff_present",
    "not at work": "not_at_work",
    "staff off": "staff_off",
    "staff day off": "staff_off",
    "total staff day off": "staff_off",
    "total staff off": "staff_off",
    "staffs day off": "staff_off",
    "total staffs day off": "staff_off",
    "total staffs off": "staff_off",
    "day off": "staff_off",
    "off": "staff_off",
    "off duty": "staff_off",
    "total leave": "leave",
    "total staff leave": "leave",
    "staff leave": "leave",
    "staffs lay off": "lay_off",
    "lay off": "lay_off",
    "layoff": "lay_off",
    "staffs late": "late",
    "late": "late",
    "suspend": "suspend",
    "suspended": "suspend",
    "staffs suspend": "suspend",
    "absent": "absent",
    "staffs absent with notice": "absent_with_notice",
    "absent with notice": "absent_with_notice",
    "awn": "absent_with_notice",
    "staffs absent without": "absent_without_notice",
    "staffs absent without notice": "absent_without_notice",
    "absent without": "absent_without_notice",
    "absent without notice": "absent_without_notice",
    "awon": "absent_without_notice",
    "leave": "leave",
    "staffs on leave": "leave",
    "staffs on leavebreak": "leave",
    "on leave": "leave",
    "on leavebreak": "leave",
    "sick": "sick",
    "staffs sick": "sick",
    "resign": "non_active",
    "resigned": "non_active",
    "resignation pending": "non_active",
    "decision pending": "non_active",
    "decission pending": "non_active",
    "terminated": "non_active",
    "termination pending": "non_active",
    "non active": "non_active",
    "staffs transfer": "transfer",
    "transfer": "transfer",
}
_DECLARED_STATUS_KEYS = {
    "staff_present": "present",
    "not_at_work": "not_at_work",
    "staff_off": "off",
    "lay_off": "lay_off",
    "late": "late",
    "suspend": "suspend",
    "absent": "absent",
    "absent_with_notice": "awn",
    "absent_without_notice": "awon",
    "leave": "leave",
    "sick": "sick",
    "non_active": "non_active",
    "transfer": "transfer",
}
_NIL_COUNT_VALUES = {"nil", "nill"}
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
    "summary",
    "notice",
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
    normalized_status: str | None = None
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
    normalization_attempts: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[WarningEntry] = field(default_factory=list)


@dataclass(slots=True)
class ParsedSummaryCount:
    """One summary metric extracted from the report body."""

    metric_name: str
    count: int
    raw_key: str
    raw_value: str


def parse_work_item(work_item: WorkItem) -> ParsedHrReport:
    """Parse one routed HR attendance work item into a structured view."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    parsed = ParsedHrReport()
    _seed_routing_metadata(parsed, payload)
    raw_lines = _coalesced_lines(raw_text)
    numbered_record_mode = _has_numbered_attendance_rows(raw_lines)
    note_section_active = False

    for raw_line in raw_lines:
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
                _record_normalization_attempt(
                    parsed,
                    field="branch",
                    raw_value=value,
                    normalized_value=parsed.branch_slug or parsed.branch,
                    layer="hr_parser",
                )
            elif field_name == "report_date":
                parsed.raw_date = value
                parsed.report_date = normalize_report_date(value)
                _record_normalization_attempt(
                    parsed,
                    field="report_date",
                    raw_value=value,
                    normalized_value=parsed.report_date,
                    layer="hr_parser",
                )
            elif field_name == "notes":
                parsed.notes.append(value)
            continue

        explicit_date = _parse_explicit_date_line(line)
        if explicit_date is not None:
            parsed.raw_date = line
            parsed.report_date = explicit_date
            _record_normalization_attempt(
                parsed,
                field="report_date",
                raw_value=line,
                normalized_value=explicit_date,
                layer="hr_parser",
            )
            continue

        summary = _parse_summary_count_line(line)
        if summary is not None:
            parsed.declared_summary_metrics[summary.metric_name] = summary.count
            if summary.metric_name == "total_staff":
                parsed.declared_total_staff = summary.count
            declared_status_key = _DECLARED_STATUS_KEYS.get(summary.metric_name)
            if declared_status_key is not None:
                parsed.declared_status_totals[declared_status_key] = summary.count
            _record_normalization_attempt(
                parsed,
                field="summary_label",
                raw_value=summary.raw_key,
                normalized_value=_summary_metric_display(summary.metric_name),
                layer="hr_parser",
            )
            _record_normalization_attempt(
                parsed,
                field="summary_count",
                raw_value=summary.raw_value,
                normalized_value=summary.count,
                layer="hr_parser",
                context={"metric": _summary_metric_display(summary.metric_name)},
            )
            continue

        if note_section_active:
            parsed.notes.append(line)
            continue

        record = _parse_record_line(line, allow_unnumbered=not numbered_record_mode)
        if record is not None:
            _record_normalization_attempt(
                parsed,
                field="attendance_status",
                raw_value=record.raw_status,
                normalized_value=record.normalized_status,
                layer="hr_normalizer",
                context={"staff_name": record.staff_name},
            )
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
                code="missing_branch",
                severity="error",
                message="Branch could not be resolved from the HR attendance report.",
            )
        )
    if not parsed.report_date:
        fallback_raw_date, fallback_report_date = _resolve_fallback_report_date(raw_lines)
        if fallback_report_date is not None:
            parsed.raw_date = fallback_raw_date
            parsed.report_date = fallback_report_date
            _record_normalization_attempt(
                parsed,
                field="report_date",
                raw_value=fallback_raw_date,
                normalized_value=fallback_report_date,
                layer="hr_parser",
                context={"strategy": "first_10_non_empty_lines"},
            )
    if not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_report_date",
                severity="error",
                message="Report date could not be resolved from the HR attendance report.",
            )
        )
    if not parsed.records and not parsed.declared_status_totals:
        parsed.warnings.append(
            make_warning(
                code="no_attendance_rows",
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
    if field_name in {"branch", "report_date", "notes"}:
        return field_name, raw_value
    return None


def _parse_summary_count_line(line: str) -> ParsedSummaryCount | None:
    match = _KEY_VALUE_PATTERN.match(line)
    if match is None:
        return None

    raw_key = match.group(1).strip()
    raw_value = match.group(2).strip()
    count = _parse_summary_count(raw_value)
    if count is None:
        return None

    metric_name = _SUMMARY_METRIC_ALIASES.get(_normalize_key(raw_key))
    if metric_name is None:
        return None
    return ParsedSummaryCount(metric_name=metric_name, count=count, raw_key=raw_key, raw_value=raw_value)


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

    canonical_status, normalized_status = normalize_status(raw_status)
    canonical_status = canonical_status or "unknown"

    return ParsedAttendanceRecord(
        record_number=record_number,
        staff_name=staff_name,
        status=canonical_status,
        raw_status=raw_status,
        normalized_status=normalized_status,
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


def _coalesced_lines(raw_text: str) -> list[str]:
    """Join split attendance continuation lines before field parsing."""

    raw_lines = raw_text.splitlines()
    coalesced: list[str] = []
    index = 0

    while index < len(raw_lines):
        current_line = raw_lines[index]
        next_line = raw_lines[index + 1] if index + 1 < len(raw_lines) else None
        if next_line is not None and _should_merge_record_continuation(current_line, next_line):
            coalesced.append(f"{current_line.rstrip()} {next_line.lstrip()}")
            index += 2
            continue
        coalesced.append(current_line)
        index += 1

    return coalesced

def _parse_explicit_date_line(line: str) -> str | None:
    stripped = line.strip()
    normalized_key = _normalize_key(stripped)
    if (
        _DATE_ONLY_PATTERN.match(stripped.rstrip(".,"))
        or _WEEKDAY_DATE_PATTERN.match(stripped)
        or normalized_key.startswith("date")
    ):
        return normalize_report_date(stripped)
    return None


def _resolve_fallback_report_date(lines: list[str], *, limit: int = 10) -> tuple[str | None, str | None]:
    """Return the first recoverable report date from the early non-empty lines."""

    for line in _first_non_empty_lines(lines, limit=limit):
        stripped = line.strip()
        if not stripped or _DATE_FALLBACK_PATTERN.search(stripped) is None:
            continue
        normalized_date = normalize_report_date(stripped)
        if normalized_date is None:
            continue
        return stripped, normalized_date
    return None, None


def _first_non_empty_lines(lines: list[str], *, limit: int) -> list[str]:
    """Return the first `limit` non-empty raw lines."""

    first_lines: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        first_lines.append(line)
        if len(first_lines) >= limit:
            break
    return first_lines


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
    if isinstance(raw_text, str):
        lines = raw_text.splitlines()
    else:
        lines = raw_text
    return any(_NUMBERED_LINE_PATTERN.match(line.strip()) for line in lines)


def _should_merge_record_continuation(current_line: str, next_line: str) -> bool:
    """Return whether a numbered staff row continues on the next line."""

    numbered_match = _NUMBERED_LINE_PATTERN.match(current_line.strip())
    if numbered_match is None:
        return False

    content = numbered_match.group(2).strip()
    if _split_record_content(content) is not None:
        return False

    if _looks_like_non_record_label(_clean_text(content) or "", numbered=True):
        return False

    continuation_match = _CONTINUATION_STATUS_PATTERN.match(next_line.strip())
    if continuation_match is None:
        return False

    raw_status = _clean_text(continuation_match.group("status") or "")
    if raw_status is None:
        return False

    canonical_status, _ = normalize_status(raw_status)
    return canonical_status is not None


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
    cleaned = strip_formatting_noise(value).strip(" -*|/:=,")
    return cleaned or None


def _normalize_key(value: str) -> str:
    return normalize_key_text(value)


def _parse_summary_count(raw_value: str) -> int | None:
    cleaned_value = strip_formatting_noise(raw_value)
    normalized = _normalize_key(cleaned_value)
    if normalized in _NIL_COUNT_VALUES:
        return 0
    count = parse_count(cleaned_value)
    if count is not None:
        return count
    leading_count_match = re.match(r"^\s*[([]?(\d+)\b", cleaned_value)
    if leading_count_match is not None:
        return int(leading_count_match.group(1))
    return None


def _summary_metric_display(metric_name: str) -> str:
    labels = {
        "total_staff": "Total_Staff",
        "staff_present": "Total_Present",
        "not_at_work": "Total_Not_At_Work",
        "staff_off": "Total_Off",
        "lay_off": "Total_Lay_Off",
        "late": "Total_Late",
        "suspend": "Total_Suspend",
        "absent": "Total_Absent",
        "absent_with_notice": "Total_Absent_With_Notice",
        "absent_without_notice": "Total_Absent_Without_Notice",
        "leave": "Total_Leave",
        "sick": "Total_Sick",
        "non_active": "Total_Non_Active",
        "transfer": "Total_Transfer",
    }
    return labels.get(metric_name, metric_name)


def _record_normalization_attempt(
    parsed: ParsedHrReport,
    *,
    field: str,
    raw_value: object,
    normalized_value: object,
    layer: str,
    context: Mapping[str, object] | None = None,
) -> None:
    """Capture one normalization attempt for downstream accountability."""

    parsed.normalization_attempts.append(
        {
            "field": field,
            "layer": layer,
            "raw_value": raw_value,
            "normalized_value": normalized_value,
            **({"context": dict(context)} if context is not None else {}),
        }
    )
