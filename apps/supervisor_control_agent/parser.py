"""Conservative parser for supervisor control work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from packages.common.branch import canonical_branch_slug
from packages.common.date import normalize_report_date
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.signal_contracts.work_item import WorkItem

_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "branch": ("branch", "shop", "location"),
    "report_date": ("date", "report date"),
    "exception_type": ("exception type", "issue type"),
    "details": ("details", "detail", "description"),
    "action_taken": ("action taken", "action"),
    "escalated_by": ("escalated by",),
    "time": ("time",),
    "supervisor_confirmed": ("supervisor confirmed", "confirmed"),
    "notes": ("notes", "note", "remarks", "remark"),
}


@dataclass(slots=True)
class ParsedExceptionEntry:
    """One parsed exception record from the report."""

    exception_type: str | None = None
    details: str | None = None
    action_taken: str | None = None
    escalated_by: str | None = None
    time: str | None = None
    supervisor_confirmed: str | None = None


@dataclass(slots=True)
class ParsedSupervisorControlReport:
    """Structured parse result for one supervisor control report."""

    branch: str | None = None
    branch_slug: str | None = None
    report_date: str | None = None
    exception_entries: list[ParsedExceptionEntry] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    warnings: list[WarningEntry] = field(default_factory=list)


def parse_work_item(work_item: WorkItem) -> ParsedSupervisorControlReport:
    """Parse one routed supervisor-control work item into a structured view."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    parsed = ParsedSupervisorControlReport()
    current_entry: ParsedExceptionEntry | None = None

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line or _is_title_line(line):
            continue

        key_value = _parse_key_value(line)
        if key_value is None:
            parsed.notes.append(line)
            continue

        field_name, value = key_value
        if field_name == "branch":
            parsed.branch = value
            parsed.branch_slug = canonical_branch_slug(value)
            continue
        if field_name == "report_date":
            parsed.report_date = normalize_report_date(value)
            continue
        if field_name == "notes":
            parsed.notes.append(value)
            continue

        if field_name == "exception_type":
            if current_entry is not None:
                parsed.exception_entries.append(current_entry)
            current_entry = ParsedExceptionEntry(exception_type=value)
            continue

        if current_entry is None:
            current_entry = ParsedExceptionEntry()

        if field_name == "details":
            current_entry.details = value
        elif field_name == "action_taken":
            current_entry.action_taken = value
        elif field_name == "escalated_by":
            current_entry.escalated_by = value
        elif field_name == "time":
            current_entry.time = value
        elif field_name == "supervisor_confirmed":
            current_entry.supervisor_confirmed = value
        else:
            parsed.notes.append(line)

    if current_entry is not None:
        parsed.exception_entries.append(current_entry)

    if not parsed.branch:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch could not be resolved from the supervisor control report.",
            )
        )
    if not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Report date could not be resolved from the supervisor control report.",
            )
        )
    if not parsed.exception_entries:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="No supervisor control exception entries were extracted from the report.",
            )
        )

    parsed.warnings = dedupe_warnings(parsed.warnings)
    return parsed


def _raw_text(payload: dict[str, Any]) -> str:
    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""
    text = raw_message.get("text")
    if not isinstance(text, str):
        return ""
    return text.strip()


def _is_title_line(line: str) -> bool:
    lowered = _normalize_key(line)
    return "supervisor control report" in lowered or lowered == "supervisor control report"


def _parse_key_value(line: str) -> tuple[str, str] | None:
    if ":" not in line:
        return None
    key, value = line.split(":", 1)
    raw_key = _normalize_key(key)
    raw_value = value.strip()
    if not raw_value:
        return None

    for canonical_name, aliases in _FIELD_ALIASES.items():
        if raw_key in {_normalize_key(alias) for alias in aliases}:
            return canonical_name, raw_value
    return None


def _normalize_key(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").split())
