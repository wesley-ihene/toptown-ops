"""Unified report normalization before strict validation."""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any

from .branches import normalize_branch
from .currency import normalize_money
from .dates import normalize_report_date
from .labels import internal_field_name, normalize_label
from .numbers import normalize_decimal, normalize_int
from .types import AppliedRule, NormalizationResult, NormalizedValue

_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
_MONEY_FIELDS = {
    "gross_sales",
    "cash_sales",
    "eftpos_sales",
    "mobile_money_sales",
    "till_total",
    "deposit_total",
    "z_reading",
    "amount",
    "total_amount",
}
_INT_FIELDS = {
    "traffic",
    "served",
    "customer_count",
    "total_staff",
    "qty",
    "total_qty",
}


def normalize_report(
    raw_text: str,
    report_family: str | None = None,
    routing_context: Mapping[str, Any] | None = None,
) -> NormalizationResult:
    """Return a reusable report-level normalization result."""

    family = _normalize_family(report_family)
    result = NormalizationResult(
        report_family=family,
        report_type=_routing_report_type(routing_context),
        raw_snapshots={"raw_text": raw_text, "routing_context": dict(routing_context or {})},
    )
    if not raw_text.strip():
        result.hard_errors.append("empty_raw_text")
        result.normalized_text = raw_text
        result.confidence_summary = {"overall": 0.0}
        return result

    normalized_lines: list[str] = []
    label_map: dict[str, NormalizedValue] = {}
    numeric_fields: dict[str, NormalizedValue] = {}
    money_fields: dict[str, NormalizedValue] = {}
    branch_result: NormalizedValue | None = None
    date_result: NormalizedValue | None = None

    for raw_line in raw_text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            normalized_lines.append(stripped)
            continue

        if family == "sales":
            header_label = normalize_label(stripped, report_family="sales")
            if header_label.succeeded and header_label.normalized_value == "DAY-END SALES REPORT":
                normalized_lines.append(header_label.normalized_value)
                label_map[stripped] = header_label
                continue

        match = _KEY_VALUE_PATTERN.match(stripped)
        if match is None:
            normalized_lines.append(stripped)
            continue

        raw_label = match.group(1).strip()
        raw_value = match.group(2).strip()
        label_result = normalize_label(raw_label, report_family=family)
        label_map[raw_label] = label_result
        if not label_result.succeeded:
            normalized_lines.append(stripped)
            continue

        canonical_label = label_result.normalized_value or raw_label
        field_name = internal_field_name(raw_label, family)
        normalized_value = raw_value
        value_result: NormalizedValue | None = None

        if field_name == "branch":
            value_result = normalize_branch(raw_value)
            branch_result = value_result
        elif field_name == "report_date":
            value_result = normalize_report_date(raw_value)
            date_result = value_result
        elif field_name in _MONEY_FIELDS:
            value_result = normalize_money(raw_value)
            money_fields[field_name] = value_result
        elif field_name in _INT_FIELDS:
            value_result = normalize_int(raw_value)
            numeric_fields[field_name] = value_result
        elif field_name == "labor_hours":
            value_result = normalize_decimal(raw_value)
            numeric_fields[field_name] = value_result

        if value_result is not None and value_result.succeeded and value_result.normalized_value is not None:
            normalized_value = value_result.normalized_value
            result.provenance.extend(value_result.applied_rules)
        elif value_result is not None:
            result.warnings.extend(value_result.warnings)
            result.hard_errors.extend(value_result.hard_errors)

        if label_result.applied_rules:
            result.provenance.extend(label_result.applied_rules)
        normalized_lines.append(f"{canonical_label}: {normalized_value}")

    if branch_result is None and routing_context is not None:
        branch_hint = _context_string(routing_context, "branch_hint")
        if branch_hint is not None:
            branch_result = normalize_branch(branch_hint)
            if branch_result.succeeded:
                result.provenance.append(
                    AppliedRule(
                        name="routing_branch_hint_used",
                        raw_value=branch_hint,
                        normalized_value=branch_result.normalized_value,
                    )
                )

    if date_result is None and routing_context is not None:
        for key in ("raw_report_date", "report_date"):
            raw_date = _context_string(routing_context, key)
            if raw_date is None:
                continue
            date_result = normalize_report_date(raw_date)
            if date_result.succeeded:
                result.provenance.append(
                    AppliedRule(
                        name="routing_report_date_used",
                        raw_value=raw_date,
                        normalized_value=date_result.normalized_value,
                        details={"field": key},
                    )
                )
                break

    result.normalized_text = "\n".join(normalized_lines)
    result.normalized_fields = {
        "branch": branch_result.normalized_value if branch_result is not None and branch_result.succeeded else None,
        "report_date": date_result.normalized_value if date_result is not None and date_result.succeeded else None,
    }
    result.branch = branch_result
    result.report_date = date_result
    result.label_map = label_map
    result.numeric_fields = numeric_fields
    result.money_fields = money_fields

    confidences = [item.confidence for item in (branch_result, date_result) if item is not None]
    confidences.extend(value.confidence for value in numeric_fields.values() if value.succeeded)
    confidences.extend(value.confidence for value in money_fields.values() if value.succeeded)
    overall = round(sum(confidences) / len(confidences), 4) if confidences else 0.0
    result.confidence_summary = {
        "overall": overall,
        "branch": branch_result.confidence if branch_result is not None else 0.0,
        "report_date": date_result.confidence if date_result is not None else 0.0,
    }
    return result


def _normalize_family(report_family: str | None) -> str | None:
    if report_family is None:
        return None
    normalized = report_family.strip().casefold()
    if normalized in {"sales_income", "sales"}:
        return "sales"
    if normalized in {"attendance", "staff_attendance", "hr_attendance"}:
        return "attendance"
    if normalized in {"pricing_stock_release", "bale_summary", "bale_release"}:
        return "bale_summary"
    return normalized or None


def _routing_report_type(routing_context: Mapping[str, Any] | None) -> str | None:
    if routing_context is None:
        return None
    report_type = routing_context.get("report_type")
    return report_type if isinstance(report_type, str) and report_type.strip() else None


def _context_string(routing_context: Mapping[str, Any], field_name: str) -> str | None:
    value = routing_context.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
