"""Narrow parser for WhatsApp-style sales income reports."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from typing import Any

from apps.sales_income_agent.block_detector import DetectedBlock, detect_blocks
from apps.sales_income_agent.cleanup import cleanup_text
from apps.sales_income_agent.date_branch_resolver import normalize_report_date, resolve_branch
from apps.sales_income_agent.field_mapper import canonical_field_name
from apps.sales_income_agent.figures import SalesFigures
from apps.sales_income_agent.normalizer import parse_count, parse_hours, parse_money
from apps.sales_income_agent.provenance import SalesProvenance
from apps.sales_income_agent.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.normalization.engine import normalize_report
from packages.signal_contracts.work_item import WorkItem
from packages.validation.sales_reconciliation import TOLERANCE

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
_MONEY_TOKEN_PATTERN = re.compile(r"(?<![#/])(?:[Kk]\s*)?-?\d(?:[\d, ]*\d)?(?:\.\d+)?")
_OPERATOR_LINE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("cashier", re.compile(r"^\s*(cashier|served by)\s*(?:[:=\-]\s*|\s+)(.+?)\s*$", flags=re.IGNORECASE)),
    ("assistant", re.compile(r"^\s*(assistant|assistant cashier)\s*(?:[:=\-]\s*|\s+)(.+?)\s*$", flags=re.IGNORECASE)),
    ("balanced_by", re.compile(r"^\s*(balanced by|balance by)\s*(?:[:=\-]\s*|\s+)(.+?)\s*$", flags=re.IGNORECASE)),
)


@dataclass(slots=True)
class ParsedSalesReport:
    """Structured sales report extracted from a work item."""

    branch: str | None = None
    branch_slug: str | None = None
    report_date: str | None = None
    figures: SalesFigures = field(default_factory=SalesFigures)
    provenance: SalesProvenance = field(default_factory=SalesProvenance)
    blocks: list[DetectedBlock] = field(default_factory=list)
    warnings: list[WarningEntry] = field(default_factory=list)


def parse_work_item(work_item: WorkItem) -> ParsedSalesReport:
    """Parse a sales work item into a normalized structured report."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    raw_text = _raw_text(payload)
    cleaned_lines = cleanup_text(raw_text)
    embedded_adjustments = _collect_embedded_adjustments(cleaned_lines)
    blocks = detect_blocks("\n".join(cleaned_lines))
    parsed = ParsedSalesReport(blocks=blocks)
    _apply_branch_heading_fallback(parsed, cleaned_lines)

    notes: list[str] = []
    for block in blocks:
        for line in block.lines:
            match = _KEY_VALUE_PATTERN.match(line)
            if match is None:
                if _apply_operator_provenance_line(parsed, line):
                    continue
                if block.block_type == "additional_information":
                    notes.append(line)
                continue

            raw_key, raw_value = match.group(1).strip(), match.group(2).strip()
            field_name = canonical_field_name(raw_key)
            if field_name == "branch":
                parsed.branch, parsed.branch_slug = resolve_branch(raw_value)
            elif field_name == "report_date":
                parsed.report_date = normalize_report_date(raw_value)
            elif field_name == "traffic":
                _assign_count(parsed, field_name="traffic", raw_value=raw_value)
            elif field_name == "served":
                _assign_count(parsed, field_name="served", raw_value=raw_value)
            elif field_name == "customer_count":
                customer_count = parse_count(raw_value)
                if parsed.figures.served is None:
                    parsed.figures.served = customer_count
            elif field_name == "labor_hours":
                parsed.figures.labor_hours = parse_hours(raw_value)
            elif field_name in {
                "gross_sales",
                "net_sales",
                "item_returns",
                "cash_over",
                "cash_down",
                "cash_sales",
                "eftpos_sales",
                "mobile_money_sales",
                "till_total",
                "deposit_total",
                "z_reading",
            }:
                amount = parse_money(raw_value)
                if amount is not None:
                    if field_name == "item_returns":
                        _assign_additive_amount(
                            parsed.figures,
                            field_name="item_returns",
                            amount=abs(amount),
                            raw_key=raw_key,
                        )
                    elif field_name == "net_sales":
                        parsed.figures.net_sales = amount
                    elif field_name in {"cash_over", "cash_down"}:
                        setattr(parsed.figures, field_name, abs(amount))
                    elif field_name == "z_reading":
                        _assign_additive_amount(
                            parsed.figures,
                            field_name="z_reading",
                            amount=amount,
                            raw_key=raw_key,
                        )
                    else:
                        setattr(parsed.figures, field_name, amount)
            elif field_name == "cashier":
                parsed.provenance.cashier = raw_value
            elif field_name == "assistant":
                parsed.provenance.assistant = raw_value
            elif field_name == "balanced_by":
                parsed.provenance.balanced_by = raw_value
            elif field_name == "supervisor":
                parsed.provenance.supervisor = raw_value
            elif field_name == "supervisor_confirmation":
                parsed.provenance.supervisor_confirmation = raw_value
            elif field_name == "notes":
                notes.append(raw_value)
            elif field_name == "variance_reason":
                _append_note(notes, f"Variance Reason: {raw_value}")
            elif field_name == "return_type":
                _append_note(notes, f"Return Type: {raw_value}")

    if parsed.figures.item_returns is None and embedded_adjustments["item_returns"] is not None:
        parsed.figures.item_returns = embedded_adjustments["item_returns"]
    if parsed.figures.cash_over is None and embedded_adjustments["cash_over"] is not None:
        parsed.figures.cash_over = embedded_adjustments["cash_over"]
    if parsed.figures.cash_down is None and embedded_adjustments["cash_down"] is not None:
        parsed.figures.cash_down = embedded_adjustments["cash_down"]
    for note in embedded_adjustments["notes"]:
        _append_note(notes, note)

    parsed.provenance.notes = notes
    _apply_routing_fallbacks(parsed, payload)
    _reconcile_sales_amounts(parsed.figures)

    if not parsed.branch or not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch or report date could not be resolved from the sales report.",
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
        report_family="sales",
        routing_context=payload.get("routing") if isinstance(payload.get("routing"), Mapping) else None,
    )
    return (normalization.normalized_text or stripped).strip()


def _apply_branch_heading_fallback(parsed: ParsedSalesReport, cleaned_lines: list[str]) -> None:
    """Resolve branch from a free-text leading heading when no explicit branch field exists."""

    if parsed.branch or not cleaned_lines:
        return

    candidate = cleaned_lines[0]
    normalized = candidate.casefold()
    if "branch" not in normalized:
        return
    if any(marker in normalized for marker in ("date", "cashier", "sales report")):
        return

    parsed.branch, parsed.branch_slug = resolve_branch(candidate)


def _apply_routing_fallbacks(parsed: ParsedSalesReport, payload: dict[str, Any]) -> None:
    """Resolve branch/date from routed orchestrator metadata when text omitted explicit labels."""

    routing = payload.get("routing")
    if not isinstance(routing, Mapping):
        return

    if parsed.branch_slug is None:
        branch_hint = routing.get("branch_hint")
        if isinstance(branch_hint, str) and branch_hint.strip():
            parsed.branch, parsed.branch_slug = resolve_branch(branch_hint)

    if parsed.report_date is None:
        for field_name in ("normalized_report_date", "report_date", "raw_report_date"):
            candidate = routing.get(field_name)
            if not isinstance(candidate, str) or not candidate.strip():
                continue
            normalized = normalize_report_date(candidate)
            if normalized is not None:
                parsed.report_date = normalized
                break


def _assign_count(parsed: ParsedSalesReport, *, field_name: str, raw_value: str) -> None:
    """Assign one explicitly labeled count field when parsing succeeds."""

    count = parse_count(raw_value)
    if count is not None:
        setattr(parsed.figures, field_name, count)


def _assign_additive_amount(figures: SalesFigures, *, field_name: str, amount: float, raw_key: str) -> None:
    """Accumulate repeated till totals while letting explicit summary labels override."""

    current = getattr(figures, field_name)
    if current is None or _is_summary_amount_label(raw_key) or abs(current - amount) <= TOLERANCE:
        setattr(figures, field_name, round(amount, 2))
        return
    setattr(figures, field_name, round(current + amount, 2))


def _is_summary_amount_label(raw_key: str) -> bool:
    normalized = " ".join(raw_key.casefold().replace("_", " ").replace("/", " ").replace("-", " ").split())
    return "total" in normalized or "gross" in normalized


def _reconcile_sales_amounts(figures: SalesFigures) -> None:
    """Align gross, net, and returns without changing no-return report semantics."""

    gross_sales = figures.gross_sales
    net_sales = figures.net_sales
    z_reading = figures.z_reading
    item_returns = figures.item_returns

    if z_reading is not None and gross_sales is None:
        figures.gross_sales = z_reading
        gross_sales = z_reading

    if item_returns is None:
        return

    if z_reading is not None and gross_sales is not None and abs(z_reading - gross_sales - item_returns) <= TOLERANCE:
        figures.net_sales = gross_sales
        figures.gross_sales = z_reading
        gross_sales = figures.gross_sales
        net_sales = figures.net_sales

    if gross_sales is None and z_reading is not None:
        figures.gross_sales = z_reading
        gross_sales = z_reading

    if net_sales is None and gross_sales is not None:
        figures.net_sales = round(gross_sales - item_returns, 2)
        net_sales = figures.net_sales

    if gross_sales is None and net_sales is not None:
        figures.gross_sales = round(net_sales + item_returns, 2)


def _apply_operator_provenance_line(parsed: ParsedSalesReport, line: str) -> bool:
    """Apply provenance fields from WhatsApp operator lines with flexible separators."""

    for field_name, pattern in _OPERATOR_LINE_PATTERNS:
        matched = pattern.match(line)
        if matched is None:
            continue
        cleaned_name = _clean_operator_value(matched.group(2))
        if cleaned_name is None:
            return False
        setattr(parsed.provenance, field_name, cleaned_name)
        return True
    return False


def _clean_operator_value(raw_value: str) -> str | None:
    """Return a cleaned provenance name value or ``None``."""

    cleaned = " ".join(raw_value.strip().strip(" -:=|").split())
    return cleaned or None


def _collect_embedded_adjustments(cleaned_lines: list[str]) -> dict[str, float | list[str] | None]:
    """Extract adjustment amounts and notes from non-tabular supervisor summary lines."""

    item_returns: float | None = None
    cash_over: float | None = None
    cash_down: float | None = None
    notes: list[str] = []

    for line in cleaned_lines:
        normalized = " ".join(line.casefold().replace("_", " ").replace("/", " ").split())
        if _mentions_item_return(normalized):
            amounts = _extract_money_values(line)
            if amounts:
                item_returns = round(sum(abs(amount) for amount in amounts), 2)
        if _mentions_cash_over(normalized):
            amounts = _extract_money_values(line)
            if amounts:
                cash_over = round(sum(abs(amount) for amount in amounts), 2)
        if _mentions_cash_down(normalized):
            amounts = _extract_money_values(line)
            if amounts:
                cash_down = round(sum(abs(amount) for amount in amounts), 2)
        if "declined card" in normalized and "return" in normalized:
            _append_note(notes, f"Return Type: {line.strip()}")

    return {
        "item_returns": item_returns,
        "cash_over": cash_over,
        "cash_down": cash_down,
        "notes": notes,
    }


def _extract_money_values(line: str) -> list[float]:
    """Return all safely parseable money-like values from one free-form line."""

    values: list[float] = []
    for match in _MONEY_TOKEN_PATTERN.finditer(line):
        amount = parse_money(match.group(0))
        if amount is None:
            continue
        values.append(amount)
    return values


def _mentions_cash_over(normalized_line: str) -> bool:
    return (
        "cash over" in normalized_line
        or "c over" in normalized_line
        or ("cash variance" in normalized_line and "over" in normalized_line)
    )


def _mentions_cash_down(normalized_line: str) -> bool:
    return (
        "cash down" in normalized_line
        or "c down" in normalized_line
        or ("cash variance" in normalized_line and "down" in normalized_line)
    )


def _mentions_item_return(normalized_line: str) -> bool:
    return any(
        token in normalized_line
        for token in (
            "item return",
            "items return",
            "returned item",
            "refund amount",
            "refund",
        )
    )


def _append_note(notes: list[str], note: str) -> None:
    """Append a note only once while preserving existing structured notes."""

    cleaned = " ".join(note.split())
    if cleaned and cleaned not in notes:
        notes.append(cleaned)
