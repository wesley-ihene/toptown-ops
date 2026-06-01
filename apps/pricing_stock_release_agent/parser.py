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
_TOTAL_SECTION_HEADER_PATTERN = re.compile(r"^\s*total\s*:?\s*$", flags=re.IGNORECASE)
_ITEM_DETAIL_FIELD_PATTERN = re.compile(
    r"^[^A-Za-z]*(?P<key>qty|quantity|amt|amount|value)\b[^0-9A-Za-z]*(?P<value>.+?)\s*[\W_]*$",
    flags=re.IGNORECASE,
)
_MONEY_FRAGMENT_PATTERN = re.compile(
    r"(?P<amount>(?:PGK\s*|K\s*)?\d[\d,'’ ]*\.\s*\d+|(?:PGK\s*|K\s*)\d[\d,'’ ]*)",
    flags=re.IGNORECASE,
)
_APOSTROPHE_MISSING_DECIMAL_MONEY_PATTERN = re.compile(
    r"^(?P<currency>PGK|K)?(?P<int_groups>\d+(?:['’]\d{3})+)(?P<cents>\d{2})$",
    flags=re.IGNORECASE,
)
_CURRENCY_SPACE_BEFORE_DIGIT_PATTERN = re.compile(r"\b(?P<currency>PGK|K)\s+(?=\d)", flags=re.IGNORECASE)
_SPACED_COMMA_NUMBER_PATTERN = re.compile(r"(?<=\d),\s+(?=\d)")
_SAFE_FORMAT_CLEANUP_PATTERN = re.compile(
    r"(?:['’]|\b(?:PGK|K)\s+(?=\d)|(?<=\d),\s+(?=\d)|(?<=\d)\s*(?:pcs?|pce)\b)",
    flags=re.IGNORECASE,
)
_COUNT_TOKEN_PATTERN = (
    r"(?:\b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b"
    r"(?:\s*\(\s*0*\d+\s*\))?|\(\s*0*\d+\s*\)|0*\d+)"
)
_WORD_NUMBER_PATTERN = re.compile(
    r"\b(zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b",
    flags=re.IGNORECASE,
)
_PAREN_NUMBER_PATTERN = re.compile(r"\(\s*0*(\d+)\s*\)")
_COUNT_UNIT_SUFFIX_PATTERN = re.compile(r"(?<=\d)\s*(?:pcs?|pce)\b", flags=re.IGNORECASE)
_INLINE_COUNT_FRAGMENT_PATTERN = re.compile(r"(?<!\d)(\d+)(?!\d)")
_RELEASED_COUNT_PATTERN = re.compile(
    rf"(?P<count>{_COUNT_TOKEN_PATTERN})\s*bales?\s*released\b",
    flags=re.IGNORECASE,
)
_PENDING_APPROVAL_COUNT_PATTERN = re.compile(
    rf"(?P<count>{_COUNT_TOKEN_PATTERN})\s*bales?\s*(?:waiting\s*for\s*approval|pending\s*approval)\b",
    flags=re.IGNORECASE,
)
_PROCESSED_COUNT_PATTERNS = (
    re.compile(
        rf"(?P<count>{_COUNT_TOKEN_PATTERN})\s*bales?\s*processed\b",
        flags=re.IGNORECASE,
    ),
    re.compile(
        rf"(?:total\s*bales?(?:\s*on\s*rail)?|total\s*bales?\s*break\s*today)\D*(?P<count>{_COUNT_TOKEN_PATTERN})",
        flags=re.IGNORECASE,
    ),
)

_WORD_NUMBERS: dict[str, int] = {
    "zero": 0,
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
    checked_by: str | None = None
    checked_role: str | None = None
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
            elif field_name == "checked_by_role":
                parsed.checked_by, parsed.checked_role = value
            else:
                setattr(parsed, field_name, value)
            line_index += 1
            continue

        standalone_report_date = _parse_standalone_report_date_line(line)
        if standalone_report_date is not None and parsed.report_date is None:
            parsed.report_date = standalone_report_date
            line_index += 1
            continue

        total_fields, consumed_total_lines = _parse_total_section(lines, line_index)
        if total_fields is not None:
            for field_name, value in total_fields.items():
                setattr(parsed, field_name, value)
            line_index += consumed_total_lines
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

    if parsed.items and (parsed.declared_total_qty is None or parsed.declared_total_amount is None):
        inferred_total_qty = _calculated_total_qty(parsed.items)
        inferred_total_amount = round(sum(item.amount for item in parsed.items), 2)
        if parsed.declared_total_qty is None:
            parsed.declared_total_qty = inferred_total_qty
        if parsed.declared_total_amount is None:
            parsed.declared_total_amount = inferred_total_amount
        parsed.warnings.append(
            make_warning(
                code="totals_inferred",
                severity="warning",
                message="One or more declared totals was missing and was safely inferred from the extracted bale rows.",
            )
        )

    if parsed.items and _has_safe_format_cleanup(raw_text):
        parsed.warnings.append(
            make_warning(
                code="format_cleanup",
                severity="warning",
                message="One or more bale rows required safe currency or quantity format cleanup before parsing.",
            )
        )

    if (
        not parsed.branch
        or not parsed.report_date
        or not parsed.items
        or parsed.declared_total_qty is None
        or parsed.declared_total_amount is None
    ):
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch, date, bale items, total quantity, or total amount could not be fully extracted.",
            )
        )
    if parsed.branch and parsed.report_date and parsed.items and (
        parsed.prepared_by is None or parsed.checked_by is None
    ):
        parsed.warnings.append(
            make_warning(
                code="missing_provenance",
                severity="warning",
                message="Prepared By or Checked By could not be fully extracted from the bale summary.",
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
        return "prepared_by_role", _split_person_role(raw_value)
    if field_name == "checked_by":
        return "checked_by_role", _split_person_role(raw_value)
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
    if _QTY_LINE_PATTERN.match(line) is not None or _AMOUNT_LINE_PATTERN.match(line) is not None:
        return None

    pending_approval = _extract_count_from_pattern(line, _PENDING_APPROVAL_COUNT_PATTERN)
    if pending_approval is not None:
        return "declared_bales_pending_approval", pending_approval

    released = _extract_count_from_pattern(line, _RELEASED_COUNT_PATTERN)
    if released is not None:
        return "declared_bales_released", released

    processed = _extract_count_from_patterns(line, _PROCESSED_COUNT_PATTERNS)
    if processed is not None:
        return "declared_bales_processed", processed

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
    if "bale" not in normalized_line:
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


def _parse_total_section(lines: list[str], start_index: int) -> tuple[dict[str, Any] | None, int]:
    """Parse a `TOTAL:` block with separate quantity and amount lines."""

    if start_index >= len(lines):
        return None, 1
    if _TOTAL_SECTION_HEADER_PATTERN.match(lines[start_index]) is None:
        return None, 1

    total_fields: dict[str, Any] = {}
    consumed_lines = 1
    for offset in range(1, 5):
        detail_index = start_index + offset
        if detail_index >= len(lines):
            break

        detail_line = lines[detail_index]
        detail_field = _parse_item_detail_line(detail_line)
        if detail_field is not None:
            field_name, value = detail_field
            if field_name == "qty":
                total_fields["declared_total_qty"] = value
            elif field_name == "amount":
                total_fields["declared_total_amount"] = value
            consumed_lines = offset + 1
            continue

        if _is_total_section_boundary(detail_line):
            break
        if total_fields:
            break
        return None, 1

    return (total_fields or None), consumed_lines


def _split_person_role(raw_value: str) -> tuple[str | None, str | None]:
    """Split one provenance field into name and role conservatively."""

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
    if number is not None:
        return int(number)

    inline_number = _INLINE_COUNT_FRAGMENT_PATTERN.search(candidate)
    if inline_number is not None:
        return int(inline_number.group(1))
    return None


def _parse_amount(raw_value: str) -> float | None:
    """Parse amount fields such as `K240` or `$240`."""

    if not raw_value:
        return None

    cleaned = _normalize_money_input(raw_value)
    normalized = normalize_money(cleaned)
    if not normalized.succeeded or normalized.normalized_value is None:
        return None
    return float(normalized.normalized_value)


def _normalize_money_input(raw_value: str) -> str:
    """Normalize supported non-standard money layouts before money parsing."""

    normalized = raw_value.replace("’", "'").strip()
    normalized = _CURRENCY_SPACE_BEFORE_DIGIT_PATTERN.sub(lambda match: match.group("currency"), normalized)
    normalized = _SPACED_COMMA_NUMBER_PATTERN.sub(",", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    compact = re.sub(r"\s+", "", normalized)
    match = _APOSTROPHE_MISSING_DECIMAL_MONEY_PATTERN.fullmatch(compact)
    if match is not None and "." not in compact:
        whole = match.group("int_groups").replace("'", "")
        cents = match.group("cents")
        currency = (match.group("currency") or "").upper()
        if currency == "PGK":
            return f"PGK {whole}.{cents}"
        if currency:
            return f"{currency}{whole}.{cents}"
        return f"{whole}.{cents}"
    return normalized.replace("'", "")


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


def _calculated_total_qty(items: list[ParsedBaleItem]) -> int | float:
    """Return the numeric total quantity from extracted bale items."""

    total_qty = sum((float(item.qty) for item in items), 0.0)
    if total_qty.is_integer():
        return int(total_qty)
    return round(total_qty, 2)


def _has_safe_format_cleanup(raw_text: str) -> bool:
    """Return whether the source text required safe currency or qty cleanup."""

    return _SAFE_FORMAT_CLEANUP_PATTERN.search(raw_text) is not None


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
    if _TOTAL_SECTION_HEADER_PATTERN.match(line) is not None:
        return True
    if _parse_metadata_line(line) is not None:
        return True
    if _parse_summary_count_line(line) is not None:
        return True
    if normalized.startswith(("total ", "prepared by", "note", "thanks", "day")):
        return True
    return False


def _is_total_section_boundary(line: str) -> bool:
    """Return True when a `TOTAL:` block should stop consuming lines."""

    normalized = _normalize_key(line)
    if _BALE_HEADER_PATTERN.match(line) is not None:
        return True
    if _TOTAL_SECTION_HEADER_PATTERN.match(line) is not None:
        return True
    metadata = _parse_metadata_line(line)
    if metadata is not None:
        return metadata[0] not in {"declared_total_qty", "declared_total_amount"}
    if _parse_summary_count_line(line) is not None:
        return True
    return normalized.startswith(("prepared by", "note", "thanks", "day"))


def _extract_count_from_pattern(line: str, pattern: re.Pattern[str]) -> int | None:
    """Return one parsed count from a regex group named `count`."""

    match = pattern.search(line)
    if match is None:
        return None
    return _parse_count_phrase(match.group("count"))


def _extract_count_from_patterns(line: str, patterns: tuple[re.Pattern[str], ...]) -> int | None:
    """Return the first parsed count matched by any supported summary pattern."""

    for pattern in patterns:
        count = _extract_count_from_pattern(line, pattern)
        if count is not None:
            return count
    return None
