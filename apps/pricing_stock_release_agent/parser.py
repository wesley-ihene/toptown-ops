"""Conservative parsing helpers for WhatsApp-style bale summary work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
import re
from typing import Any

from apps.pricing_stock_release_agent.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.normalization.branches import normalize_branch
from packages.normalization.currency import normalize_money
from packages.normalization.dates import normalize_report_date as normalize_strict_report_date
from packages.normalization.engine import normalize_report
from packages.normalization.labels import internal_field_name
from packages.normalization.numbers import normalize_decimal
from packages.signal_contracts.work_item import WorkItem

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
_BALE_HEADER_PATTERN = re.compile(r"^\s*#?\s*(\d+)\s*(?:\.\s*|\s+)(.+?)\s*$")
_QTY_LINE_PATTERN = re.compile(r"^\(?\s*qty\s*:\s*([^)]+?)\s*\)?$", flags=re.IGNORECASE)
_AMOUNT_LINE_PATTERN = re.compile(r"^\s*(?:amt|amount)\s*:\s*(.+?)\s*$", flags=re.IGNORECASE)
_ITEM_DETAIL_FIELD_PATTERN = re.compile(
    r"^[^A-Za-z]*(?P<key>qty|quantity|amt|amount|value)\b[^0-9A-Za-z]*(?P<value>.+?)\s*[\W_]*$",
    flags=re.IGNORECASE,
)
_MONEY_FRAGMENT_PATTERN = re.compile(
    r"(?P<amount>(?:PGK\s*|K\s*)?\d[\d, ]*\.\s*\d+|(?:PGK\s*|K\s*)\d[\d, ]*)",
    flags=re.IGNORECASE,
)
_WORD_NUMBER_PATTERN = re.compile(
    r"\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b",
    flags=re.IGNORECASE,
)
_PAREN_NUMBER_PATTERN = re.compile(r"\(\s*0*(\d+)\s*\)")
_COUNT_UNIT_SUFFIX_PATTERN = re.compile(r"(?<=\d)\s*(?:pcs?|pce)\b", flags=re.IGNORECASE)

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
    declared_total_qty: int | float | None = None
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

        standalone_report_date = _parse_standalone_report_date_line(line)
        if standalone_report_date is not None and parsed.report_date is None:
            parsed.report_date = standalone_report_date
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

    parsed.warnings = dedupe_warnings(parsed.warnings)
    return parsed


def _raw_text(payload: dict[str, Any]) -> str:
    """Return the strict `raw_message.text` field when available."""

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
        report_family="bale_summary",
        routing_context=payload.get("routing") if isinstance(payload.get("routing"), Mapping) else None,
    )
    return (normalization.normalized_text or stripped).strip()


def _parse_metadata_line(line: str) -> tuple[str, Any] | None:
    """Return parsed metadata when the line matches a supported field."""

    match = _KEY_VALUE_PATTERN.match(line)
    if match is None:
        return None

    raw_key = _sanitize_label_key(match.group(1))
    raw_value = match.group(2).strip()
    field_name = internal_field_name(raw_key, report_family="bale_summary")

    if field_name == "branch":
        branch_result = normalize_branch(raw_value)
        return ("branch", branch_result.normalized_value) if branch_result.normalized_value is not None else None
    if field_name == "report_date":
        return "report_date", _normalize_report_date(raw_value)
    if field_name == "prepared_by":
        return "prepared_by_role", _split_prepared_by(raw_value)
    if _normalize_key(raw_key) in {"total bales on rail", "total bales", "bales processed"}:
        count = _parse_count_phrase(raw_value)
        return ("declared_bales_processed", count) if count is not None else None
    if field_name == "total_qty":
        quantity_count = _parse_count_phrase(raw_value)
        if quantity_count is not None:
            return "declared_total_qty", quantity_count
        quantity = _parse_number(raw_value)
        if quantity is not None:
            return "declared_total_qty", int(quantity) if quantity == quantity.to_integral_value() else float(quantity)
        return None
    if field_name == "total_amount":
        amount = _parse_amount(raw_value)
        return ("declared_total_amount", amount) if amount is not None else None
    return None


def _parse_summary_count_line(line: str) -> tuple[str, int] | None:
    """Return parsed summary counts from WhatsApp-style prose lines."""

    if _MONEY_FRAGMENT_PATTERN.search(line) is not None:
        return None

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
    """Parse one WhatsApp bale block across supported row layouts."""

    if start_index >= len(lines):
        return None, 1

    header_match = _BALE_HEADER_PATTERN.match(lines[start_index])
    if header_match is None:
        return None, 1

    qty_value: int | None = None
    amount_value: float | None = None
    consumed_lines = 1

    for offset in range(1, 4):
        detail_index = start_index + offset
        if detail_index >= len(lines):
            break
        detail_line = lines[detail_index]
        if _is_block_boundary(detail_line):
            break

        detail_field = _parse_item_detail_line(detail_line)
        if detail_field is not None:
            field_name, value = detail_field
            if field_name == "qty":
                qty_value = value
            elif field_name == "amount":
                amount_value = value
            consumed_lines = offset + 1
            continue

        compact_values = _parse_compact_item_values(detail_line)
        if compact_values is not None:
            qty_value, amount_value = compact_values
            consumed_lines = offset + 1
            continue

    if qty_value is None or amount_value is None:
        return None, 1

    return (
        ParsedBaleItem(
            bale_id=header_match.group(1),
            item_name=header_match.group(2).strip(),
            qty=qty_value,
            amount=amount_value,
        ),
        consumed_lines,
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

    candidate = _COUNT_UNIT_SUFFIX_PATTERN.sub("", raw_value)
    paren_match = _PAREN_NUMBER_PATTERN.search(candidate)
    word_match = _WORD_NUMBER_PATTERN.search(candidate)

    count_from_parens = int(paren_match.group(1)) if paren_match is not None else None
    count_from_word = (
        _WORD_NUMBERS.get(word_match.group(1).casefold()) if word_match is not None else None
    )

    if count_from_parens is not None:
        return count_from_parens
    if count_from_word is not None:
        return count_from_word

    number = _parse_number(candidate)
    return int(number) if number is not None else None


def _parse_amount(raw_value: str) -> float | None:
    """Parse amount fields such as `K240` or `$240`."""

    normalized = normalize_money(raw_value)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    return float(normalized.normalized_value)


def _normalize_report_date(raw_value: str) -> str:
    """Return an ISO date string when a supported report date format is recognized."""

    return normalize_strict_report_date(raw_value).normalized_value or raw_value.strip()


def _parse_standalone_report_date_line(line: str) -> str | None:
    """Parse bare date lines such as `Thursday 02/04/26`."""

    normalized = normalize_strict_report_date(line)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    return normalized.normalized_value


def _parse_number(raw_value: str) -> Decimal | None:
    """Parse an integer or decimal value from a free-form numeric string."""

    normalized = normalize_decimal(raw_value)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    try:
        return Decimal(normalized.normalized_value)
    except Exception:
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


def _sanitize_label_key(value: str) -> str:
    """Remove WhatsApp bullets and wrapper punctuation from metadata keys."""

    sanitized = value.strip()
    sanitized = sanitized.lstrip("●•*-")
    sanitized = sanitized.strip()
    sanitized = re.sub(r"\s+", " ", sanitized)
    return sanitized


def _parse_item_detail_line(line: str) -> tuple[str, Any] | None:
    """Parse labeled bale detail rows using shared bale label aliases."""

    match = _ITEM_DETAIL_FIELD_PATTERN.match(line)
    if match is not None:
        raw_key = _sanitize_label_key(match.group("key"))
        raw_value = match.group("value").strip()
        field_name = internal_field_name(raw_key, report_family="bale_summary")
        if field_name == "qty":
            qty_value = _parse_count_phrase(raw_value)
            return ("qty", qty_value) if qty_value is not None else None
        if field_name == "amount":
            amount_value = _parse_amount(raw_value)
            return ("amount", amount_value) if amount_value is not None else None

    qty_match = _QTY_LINE_PATTERN.match(line)
    if qty_match is not None:
        qty_value = _parse_count_phrase(qty_match.group(1))
        return ("qty", qty_value) if qty_value is not None else None

    amount_match = _AMOUNT_LINE_PATTERN.match(line)
    if amount_match is not None:
        amount_value = _parse_amount(amount_match.group(1))
        return ("amount", amount_value) if amount_value is not None else None

    return None


def _parse_compact_item_values(line: str) -> tuple[int, float] | None:
    """Parse compact item rows like `(154)--K2,635.00`."""

    matches = list(_MONEY_FRAGMENT_PATTERN.finditer(line))
    if not matches:
        return None

    amount_fragment = matches[-1].group("amount")
    amount_value = _parse_amount(amount_fragment)
    if amount_value is None:
        return None

    prefix = line[: matches[-1].start()].strip()
    qty_value = _parse_count_phrase(prefix)
    if qty_value is None:
        return None

    return qty_value, amount_value


def _is_block_boundary(line: str) -> bool:
    """Return True when the line starts a new block or summary section."""

    normalized = _normalize_key(line)
    if _BALE_HEADER_PATTERN.match(line) is not None:
        return True
    if _parse_metadata_line(line) is not None:
        return True
    if _parse_summary_count_line(line) is not None:
        return True
    if normalized.startswith(("total ", "prepared by", "note", "thanks", "day")):
        return True
    return False
