"""Conservative parsing helpers for WhatsApp-style bale summary work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import re
from typing import Any

from packages.signal_contracts.work_item import WorkItem

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:]+):\s*(.+?)\s*$")
_BALE_HEADER_PATTERN = re.compile(r"^#\s*(\d+)\.(.+?)\s*$")
_QTY_LINE_PATTERN = re.compile(r"^\(?\s*qty\s*:\s*([^)]+?)\s*\)?$", flags=re.IGNORECASE)
_AMOUNT_LINE_PATTERN = re.compile(r"^\s*amt\s*:\s*([A-Za-z$]?[0-9,]+(?:\.\d+)?)\s*$", flags=re.IGNORECASE)
_WORD_NUMBER_PATTERN = re.compile(
    r"\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b",
    flags=re.IGNORECASE,
)
_PAREN_NUMBER_PATTERN = re.compile(r"\(\s*0*(\d+)\s*\)")

_WORD_NUMBERS: dict[str, int] = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
}


@dataclass(slots=True)
class WarningEntry:
    """Structured warning object for agent outputs."""

    code: str
    severity: str
    message: str

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-safe warning payload."""

        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
        }


@dataclass(slots=True)
class ParsedBaleItem:
    """Structured row parsed from a WhatsApp bale summary block."""

    bale_id: str
    item_name: str
    qty: int | float
    amount: float


@dataclass(slots=True)
class ParsedBaleSummary:
    """Structured bale summary extracted from a routed work item."""

    branch: str | None = None
    report_date: str | None = None
    prepared_by: str | None = None
    role: str | None = None
    items: list[ParsedBaleItem] = field(default_factory=list)
    declared_bales_processed: int | None = None
    declared_bales_released: int | None = None
    declared_bales_pending_approval: int | None = None
    declared_total_amount: float | None = None
    warnings: list[WarningEntry] = field(default_factory=list)
    source_text: str = ""


def parse_work_item(work_item: WorkItem) -> ParsedBaleSummary:
    """Parse WhatsApp-style bale-summary text into a structured view."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]

    parsed = ParsedBaleSummary(source_text=raw_text)
    line_index = 0
    while line_index < len(lines):
        line = lines[line_index]

        metadata = _parse_metadata_line(line)
        if metadata is not None:
            field_name, value = metadata
            if field_name == "prepared_by_role":
                parsed.prepared_by, parsed.role = value
            else:
                setattr(parsed, field_name, value)
            line_index += 1
            continue

        summary_count = _parse_summary_count_line(line)
        if summary_count is not None:
            field_name, value = summary_count
            setattr(parsed, field_name, value)
            line_index += 1
            continue

        item, consumed_lines = _parse_bale_block(lines, line_index)
        if item is not None:
            parsed.items.append(item)
            line_index += consumed_lines
            continue

        line_index += 1

    if parsed.declared_total_amount is None and parsed.items:
        parsed.declared_total_amount = round(sum(item.amount for item in parsed.items), 2)

    if not parsed.branch or not parsed.report_date or not parsed.prepared_by or not parsed.role:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch, date, prepared_by, or role could not be fully extracted.",
            )
        )
    if not parsed.items:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="No bale blocks were extracted from the bale summary text.",
            )
        )

    parsed.warnings = dedupe_warnings(parsed.warnings, keep="first", default_severity="warning")
    return parsed


def make_warning(*, code: str, severity: str, message: str) -> WarningEntry:
    """Create a structured warning entry."""

    return WarningEntry(code=code, severity=severity, message=message)


def dedupe_warnings(
    warnings: list[WarningEntry],
    *,
    keep: str = "first",
    default_severity: str = "warning",
) -> list[WarningEntry]:
    """Return de-duplicated warnings keyed by warning code."""

    unique: dict[str, WarningEntry] = {}
    for warning in warnings:
        if warning.code not in unique or keep == "last":
            unique[warning.code] = WarningEntry(
                code=warning.code,
                severity=warning.severity or default_severity,
                message=warning.message,
            )
    return list(unique.values())


def _raw_text(payload: dict[str, Any]) -> str:
    """Return the strict `raw_message.text` field when available."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""

    text = raw_message.get("text")
    if not isinstance(text, str):
        return ""
    return text.strip()


def _parse_metadata_line(line: str) -> tuple[str, Any] | None:
    """Return parsed metadata when the line matches a supported field."""

    match = _KEY_VALUE_PATTERN.match(line)
    if match is None:
        return None

    raw_key = _normalize_key(match.group(1))
    raw_value = match.group(2).strip()

    if raw_key == "branch":
        return "branch", raw_value
    if raw_key in {"date", "report date"}:
        return "report_date", raw_value
    if raw_key == "prepared by":
        return "prepared_by_role", _split_prepared_by(raw_value)
    if raw_key == "total amount":
        amount = _parse_amount(raw_value)
        return ("declared_total_amount", amount) if amount is not None else None
    return None


def _parse_summary_count_line(line: str) -> tuple[str, int] | None:
    """Return parsed summary counts from WhatsApp-style prose lines."""

    count = _parse_count_phrase(line)
    if count is None:
        return None

    normalized_line = _normalize_key(line)
    if "released" in normalized_line:
        return "declared_bales_released", count
    if "waiting for approval" in normalized_line or "pending approval" in normalized_line:
        return "declared_bales_pending_approval", count
    if normalized_line == _normalize_key(_strip_count_markers(line)):
        return None
    return "declared_bales_processed", count


def _parse_bale_block(lines: list[str], start_index: int) -> tuple[ParsedBaleItem | None, int]:
    """Parse one three-line WhatsApp bale block."""

    if start_index + 2 >= len(lines):
        return None, 1

    header_match = _BALE_HEADER_PATTERN.match(lines[start_index])
    if header_match is None:
        return None, 1

    qty_match = _QTY_LINE_PATTERN.match(lines[start_index + 1])
    amount_match = _AMOUNT_LINE_PATTERN.match(lines[start_index + 2])
    if qty_match is None or amount_match is None:
        return None, 1

    qty_value = _parse_count_phrase(qty_match.group(1))
    amount_value = _parse_amount(amount_match.group(1))
    if qty_value is None or amount_value is None:
        return None, 1

    return (
        ParsedBaleItem(
            bale_id=header_match.group(1),
            item_name=header_match.group(2).strip(),
            qty=qty_value,
            amount=amount_value,
        ),
        3,
    )


def _split_prepared_by(raw_value: str) -> tuple[str | None, str | None]:
    """Split a prepared-by field into name and role conservatively."""

    value = raw_value.strip()
    match = re.match(r"^(.*?)\s*\(([^)]+)\)\s*$", value)
    if match is not None:
        return _clean_text(match.group(1)), _clean_text(match.group(2))

    for separator in (" - ", " / ", ", "):
        if separator in value:
            name, role = value.split(separator, 1)
            return _clean_text(name), _clean_text(role)

    return _clean_text(value), None


def _parse_count_phrase(raw_value: str) -> int | None:
    """Parse WhatsApp-style counts such as `Five(05)` or `Two (02)`."""

    paren_match = _PAREN_NUMBER_PATTERN.search(raw_value)
    word_match = _WORD_NUMBER_PATTERN.search(raw_value)

    count_from_parens = int(paren_match.group(1)) if paren_match is not None else None
    count_from_word = (
        _WORD_NUMBERS.get(word_match.group(1).casefold()) if word_match is not None else None
    )

    if count_from_parens is not None:
        return count_from_parens
    if count_from_word is not None:
        return count_from_word

    number = _parse_number(raw_value)
    return int(number) if number is not None else None


def _parse_amount(raw_value: str) -> float | None:
    """Parse amount fields such as `K240` or `$240`."""

    number = _parse_number(raw_value)
    return float(number) if number is not None else None


def _parse_number(raw_value: str) -> Decimal | None:
    """Parse an integer or decimal value from a free-form numeric string."""

    cleaned = raw_value.strip()
    if not cleaned:
        return None

    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.replace("$", "")
    cleaned = cleaned.replace("K", "")
    cleaned = cleaned.replace("k", "")
    cleaned = re.sub(r"[A-Za-z() ]+", "", cleaned)
    if not cleaned:
        return None

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _strip_count_markers(raw_value: str) -> str:
    """Remove count tokens to help identify bare count lines."""

    stripped = _WORD_NUMBER_PATTERN.sub("", raw_value)
    stripped = _PAREN_NUMBER_PATTERN.sub("", stripped)
    return stripped.strip()


def _clean_text(raw_value: str) -> str | None:
    """Return cleaned text or `None`."""

    cleaned = raw_value.strip()
    return cleaned or None


def _normalize_key(value: str) -> str:
    """Normalize keys for case-insensitive matching."""

    lowered = value.casefold().strip()
    return " ".join(lowered.replace("_", " ").split())
