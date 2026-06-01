"""Conservative parser for supervisor control work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from typing import Any

from packages.common.branch import canonical_branch_slug_or_none
from packages.common.date import normalize_report_date
from packages.common.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.signal_contracts.work_item import WorkItem

_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "branch": ("branch", "shop", "location"),
    "report_date": ("date", "report date"),
    "supervisor": ("supervisor",),
    "supervisor_confirmation": ("supervisor confirmation",),
    "exception_type": ("exception type", "issue type"),
    "details": ("details", "detail", "description"),
    "action_taken": ("action taken", "action"),
    "escalated_by": ("escalated by",),
    "time": ("time",),
    "supervisor_confirmed": ("supervisor confirmed", "confirmed"),
    "notes": ("notes", "note", "remarks", "remark"),
}
_CHECKLIST_FIELD_NORMALIZATIONS: dict[str, str] = {
    "cash variance": "Cash_Variance",
    "staffing issues": "Staffing_Issues",
    "stock issues affecting sales": "Stock_Issues",
    "pricing or system issues": "Pricing_System_Issues",
    "exceptions escalated to ops manager": "Exceptions",
}
_SUPERVISOR_CONTROL_TITLE_ALIASES: frozenset[str] = frozenset(
    {
        "supervisor control report",
        "supervisor control summary",
    }
)
_IGNORABLE_EMPTY_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "cash variance",
        "staffing issues",
        "stock issues affecting sales",
        "stock issues",
        "pricing or system issues",
        "pricing system issues",
        "exceptions escalated to ops manager",
        "exceptions",
        "supervisor confirmation",
    }
)
_KNOWN_CHECKLIST_FIELD_KEYS: frozenset[str] = frozenset(_CHECKLIST_FIELD_NORMALIZATIONS.values())
_YES_LIKE_VALUE_TOKENS: frozenset[str] = frozenset(
    {
        "yes",
        "y",
        "true",
        "present",
        "issue",
        "issues",
    }
)
_NO_LIKE_VALUE_TOKENS: frozenset[str] = frozenset(
    {
        "no",
        "none",
        "nil",
        "na",
        "n a",
        "no issue",
        "no issues",
        "nothing",
    }
)
_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")


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
    supervisor: str | None = None
    supervisor_confirmation: str | None = None
    sop_compliance: str = "strict"
    exception_entries: list[ParsedExceptionEntry] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    warnings: list[WarningEntry] = field(default_factory=list)


def parse_work_item(work_item: WorkItem) -> ParsedSupervisorControlReport:
    """Parse one routed supervisor-control work item into a structured view."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _normalize_input_text(_raw_text(payload))
    parsed = ParsedSupervisorControlReport()
    current_entry: ParsedExceptionEntry | None = None
    pending_multiline_field: str | None = None

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if _is_title_line(line):
            pending_multiline_field = None
            continue

        key_value = _parse_key_value(line)
        if pending_multiline_field == "supervisor_confirmation" and key_value is None and ":" not in line:
            parsed.supervisor_confirmation = line
            parsed.notes.append(f"Supervisor confirmation: {line}")
            pending_multiline_field = None
            continue

        if key_value is None:
            empty_field_name = _empty_field_name(line)
            if empty_field_name == "supervisor confirmation":
                pending_multiline_field = "supervisor_confirmation"
                continue
            if empty_field_name is not None:
                continue
            synthesized_entry = _synthesize_exception_entry(line)
            if synthesized_entry is not None:
                parsed.sop_compliance = "fallback"
                if current_entry is not None:
                    parsed.exception_entries.append(current_entry)
                    current_entry = None
                parsed.exception_entries.append(synthesized_entry)
            else:
                parsed.notes.append(line)
            continue

        pending_multiline_field = None
        field_name, value = key_value
        if field_name == "branch":
            parsed.branch = value
            parsed.branch_slug = canonical_branch_slug_or_none(value)
            continue
        if field_name == "report_date":
            parsed.report_date = normalize_report_date(value)
            continue
        if field_name == "supervisor":
            parsed.supervisor = value
            parsed.notes.append(f"Supervisor: {value}")
            continue
        if field_name == "supervisor_confirmation":
            parsed.supervisor_confirmation = value
            parsed.notes.append(f"Supervisor confirmation: {value}")
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
                severity="warning",
                message="Branch could not be resolved from the supervisor control report.",
            )
        )
    if not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="warning",
                message="Report date could not be resolved from the supervisor control report.",
            )
        )
    if not parsed.exception_entries:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="warning",
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


def _normalize_input_text(text: str) -> str:
    """Return raw supervisor-control input with canonical checklist labels."""

    return "\n".join(_normalize_input_line(line) for line in text.splitlines())


def _normalize_input_line(line: str) -> str:
    """Return one raw input line with canonical field labels and YES/NO values."""

    stripped_line = line.strip()
    if _normalize_key(stripped_line) in _SUPERVISOR_CONTROL_TITLE_ALIASES:
        return "SUPERVISOR CONTROL REPORT"
    if ":" not in stripped_line:
        return stripped_line

    key, value = stripped_line.split(":", 1)
    normalized_key = _normalize_checklist_field_name(key.strip())
    normalized_value = _normalize_yes_no_value(value.strip())
    return f"{normalized_key}: {normalized_value}" if normalized_value else f"{normalized_key}:"


def _is_title_line(line: str) -> bool:
    lowered = _normalize_key(line)
    return lowered in _SUPERVISOR_CONTROL_TITLE_ALIASES or "supervisor control report" in lowered


def _is_ignorable_empty_field_line(line: str) -> bool:
    """Return whether one blank metadata or checklist line should be skipped."""

    return _empty_field_name(line) is not None


def _empty_field_name(line: str) -> str | None:
    """Return the normalized empty field name when one blank key line should be ignored."""

    if ":" not in line:
        return None
    key, value = line.split(":", 1)
    if value.strip():
        return None
    normalized_key = _normalize_key(key)
    if normalized_key in _IGNORABLE_EMPTY_FIELD_NAMES:
        return normalized_key
    return None


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


def _normalize_checklist_field_name(value: str) -> str:
    """Return the canonical checklist field name when one is configured."""

    return _CHECKLIST_FIELD_NORMALIZATIONS.get(_normalize_key(value), value.strip())


def _normalize_yes_no_value(value: str) -> str:
    """Return canonical YES/NO tokens for exact yes-or-no values."""

    normalized = _normalize_value_token(value)
    if normalized in _YES_LIKE_VALUE_TOKENS:
        return "YES"
    if normalized in _NO_LIKE_VALUE_TOKENS:
        return "NO"
    return value.strip()


def _synthesize_exception_entry(line: str) -> ParsedExceptionEntry | None:
    if ":" in line:
        raw_key, raw_value = line.split(":", 1)
        key = raw_key.strip()
        value = raw_value.strip()
        if key and value:
            canonical_key = _normalize_checklist_field_name(key)
            return ParsedExceptionEntry(
                details=f"{canonical_key}: {value}",
                action_taken=_fallback_action_taken(key=canonical_key, value=value),
                supervisor_confirmed=_fallback_confirmation(value, is_key_value_entry=True),
            )

    bullet_text = _strip_bullet_prefix(line)
    if bullet_text is None:
        return None

    return ParsedExceptionEntry(
        details=bullet_text,
        action_taken=bullet_text,
        supervisor_confirmed=_fallback_confirmation(bullet_text, is_key_value_entry=False),
    )


def _fallback_action_taken(*, key: str, value: str) -> str:
    normalized_value = value.strip()
    normalized_key = _normalize_checklist_field_name(key.strip())
    normalized_indicator = _normalize_yes_no_value(normalized_value)
    if _normalize_key(normalized_key) == "exceptions" and normalized_indicator != "NO":
        return "Escalated"
    if normalized_indicator in {"YES", "NO"}:
        return "Verified"
    if normalized_key in _KNOWN_CHECKLIST_FIELD_KEYS:
        return "Verified"
    return normalized_value


def _fallback_confirmation(value: str, *, is_key_value_entry: bool) -> str:
    if is_key_value_entry:
        return "YES"

    lowered = _normalize_key(value)
    negative_tokens = {
        "no",
        "not ok",
        "not okay",
        "not confirmed",
        "open",
        "pending",
        "failed",
        "fail",
        "escalated",
        "missing",
    }
    affirmative_tokens = {
        "yes",
        "ok",
        "okay",
        "passed",
        "pass",
        "checked",
        "complete",
        "completed",
        "done",
        "resolved",
        "closed",
        "cleared",
        "approved",
        "signed",
        "locked",
        "reconciled",
        "verified",
        "confirm",
        "confirmed",
    }
    if any(token in lowered for token in negative_tokens):
        return "NO"
    if any(token in lowered for token in affirmative_tokens):
        return "YES"
    return "NO"


def _strip_bullet_prefix(line: str) -> str | None:
    stripped = line.strip()
    for prefix in ("- ", "* ", "• "):
        if stripped.startswith(prefix):
            bullet_text = stripped[len(prefix) :].strip()
            return bullet_text or None
    return None


def _normalize_key(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").split())


def _normalize_value_token(value: str) -> str:
    """Return one compact tokenized value for tolerant YES/NO normalization."""

    return " ".join(_NON_ALPHANUMERIC_PATTERN.sub(" ", value.casefold()).split())
