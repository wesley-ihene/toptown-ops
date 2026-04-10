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

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
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
                "cash_sales",
                "eftpos_sales",
                "mobile_money_sales",
                "till_total",
                "deposit_total",
                "z_reading",
            }:
                amount = parse_money(raw_value)
                if amount is not None:
                    if field_name == "z_reading":
                        if parsed.figures.gross_sales is None:
                            parsed.figures.gross_sales = amount
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

    parsed.provenance.notes = notes
    _apply_routing_fallbacks(parsed, payload)

    if not parsed.branch or not parsed.report_date:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch or report date could not be resolved from the sales report.",
            )
        )
    if parsed.figures.gross_sales is None:
        parsed.warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Gross sales could not be mapped from the sales report.",
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
