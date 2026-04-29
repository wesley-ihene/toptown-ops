"""Deterministic Phase 1 SOP validation for sales payloads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field as dataclass_field
import re
from typing import Any

from packages.common.normalizer import parse_money
from packages.validation import build_rejection

from .common import (
    add_non_negative_number,
    build_result,
    get_metrics,
    make_rejection,
    validate_common_fields,
)
from .contracts import ValidationResult
from .rejection_codes import INVALID_NUMERIC_VALUE, INVALID_TOTALS

TOLERANCE = 0.01
SALES_TOTALS_MISMATCH = "sales_totals_mismatch"
_TOTAL_CASH_PATTERN = re.compile(r"^\s*total\s+cash\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)
_TOTAL_CARD_PATTERN = re.compile(r"^\s*total\s+card\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)
_TOTAL_SALES_PATTERN = re.compile(r"^\s*total\s+sales\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)
_TILL_CASH_PATTERN = re.compile(r"^\s*t\s*/?\s*cash\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)
_TILL_CARD_PATTERN = re.compile(r"^\s*t\s*/?\s*card\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)
_Z_READING_PATTERN = re.compile(r"^\s*z\s*/?\s*reading\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)


@dataclass(slots=True, frozen=True)
class _StructuredRejection:
    """One sales validation rejection with explicit numeric detail."""

    code: str
    message: str
    field: str | None = None
    extra: dict[str, Any] = dataclass_field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe rejection payload."""

        return build_rejection(
            reason_code=self.code,
            reason_detail=self.message,
            field=self.field,
            **self.extra,
        )


@dataclass(slots=True, frozen=True)
class _SalesTotalsEvidence:
    """Canonical sales totals parsed from the full report text."""

    declared_total_cash: float | None = None
    declared_total_card: float | None = None
    declared_total_sales: float | None = None
    till_cash_total: float | None = None
    till_card_total: float | None = None
    z_reading_total: float | None = None


def validate_sales(payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one structured sales payload conservatively."""

    rejections = validate_common_fields(payload)
    metrics = get_metrics(payload, rejections)

    gross_sales = add_non_negative_number(rejections, value=metrics.get("gross_sales"), field="metrics.gross_sales")
    cash_sales = add_non_negative_number(rejections, value=metrics.get("cash_sales"), field="metrics.cash_sales")
    eftpos_sales = add_non_negative_number(rejections, value=metrics.get("eftpos_sales"), field="metrics.eftpos_sales")
    mobile_money_sales = add_non_negative_number(
        rejections,
        value=metrics.get("mobile_money_sales"),
        field="metrics.mobile_money_sales",
    )
    traffic = add_non_negative_number(rejections, value=metrics.get("traffic"), field="metrics.traffic")
    served = add_non_negative_number(rejections, value=metrics.get("served"), field="metrics.served")

    payment_total = 0.0
    payment_part_count = 0
    for value in (cash_sales, eftpos_sales, mobile_money_sales):
        if value is not None:
            payment_total += value
            payment_part_count += 1

    canonical_rejection = _sales_totals_mismatch_rejection(
        payload=payload,
        declared_total_cash=cash_sales,
        declared_total_card=_declared_card_total(eftpos_sales, mobile_money_sales),
        declared_total_sales=gross_sales,
    )
    if canonical_rejection is not None:
        rejections.append(canonical_rejection)
    elif gross_sales is not None and payment_part_count >= 2 and abs(gross_sales - payment_total) > TOLERANCE:
        rejections.append(
            make_rejection(
                code=INVALID_TOTALS,
                message="Gross sales must match the sum of payment totals.",
                field="metrics.gross_sales",
            )
        )

    if traffic is not None and served is not None and served > traffic:
        rejections.append(
            make_rejection(
                code=INVALID_NUMERIC_VALUE,
                message="Served customer count cannot exceed traffic count.",
                field="metrics.served",
            )
        )

    return build_result("sales", rejections)


def _sales_totals_mismatch_rejection(
    *,
    payload: Mapping[str, Any],
    declared_total_cash: float | None,
    declared_total_card: float | None,
    declared_total_sales: float | None,
) -> _StructuredRejection | None:
    """Return one canonical totals mismatch rejection when raw report evidence exists."""

    evidence = _extract_sales_totals_evidence(_raw_sales_text(payload))
    if not any(
        value is not None
        for value in (evidence.declared_total_cash, evidence.declared_total_card, evidence.declared_total_sales)
    ):
        return None

    resolved_declared_total_cash = evidence.declared_total_cash if evidence.declared_total_cash is not None else declared_total_cash
    resolved_declared_total_card = evidence.declared_total_card if evidence.declared_total_card is not None else declared_total_card
    resolved_declared_total_sales = evidence.declared_total_sales if evidence.declared_total_sales is not None else declared_total_sales
    expected_total_cash = evidence.till_cash_total
    expected_total_card = evidence.till_card_total
    expected_total_sales = _expected_total_sales(evidence)

    if not any(
        _money_differs(left, right)
        for left, right in (
            (resolved_declared_total_cash, expected_total_cash),
            (resolved_declared_total_card, expected_total_card),
            (resolved_declared_total_sales, expected_total_sales),
        )
    ):
        return None

    return _StructuredRejection(
        code=SALES_TOTALS_MISMATCH,
        message="Sales totals do not match till/payment totals.",
        field="metrics.gross_sales",
        extra={
            "declared_total_cash": resolved_declared_total_cash,
            "expected_total_cash": expected_total_cash,
            "declared_total_card": resolved_declared_total_card,
            "expected_total_card": expected_total_card,
            "declared_total_sales": resolved_declared_total_sales,
            "expected_total_sales": expected_total_sales,
            "cash_difference": _money_difference(resolved_declared_total_cash, expected_total_cash),
            "card_difference": _money_difference(resolved_declared_total_card, expected_total_card),
            "sales_difference": _money_difference(resolved_declared_total_sales, expected_total_sales),
        },
    )


def _declared_card_total(
    eftpos_sales: float | None,
    mobile_money_sales: float | None,
) -> float | None:
    """Return the declared non-cash total carried by structured metrics."""

    parts = [value for value in (eftpos_sales, mobile_money_sales) if value is not None]
    if not parts:
        return None
    return round(sum(parts), 2)


def _expected_total_sales(evidence: _SalesTotalsEvidence) -> float | None:
    """Return the canonical expected total sales from till evidence."""

    if evidence.till_cash_total is not None and evidence.till_card_total is not None:
        return round(evidence.till_cash_total + evidence.till_card_total, 2)
    return evidence.z_reading_total


def _extract_sales_totals_evidence(raw_text: str | None) -> _SalesTotalsEvidence:
    """Parse declared and expected totals from the full raw sales text."""

    if not isinstance(raw_text, str) or not raw_text.strip():
        return _SalesTotalsEvidence()

    declared_total_cash: float | None = None
    declared_total_card: float | None = None
    declared_total_sales: float | None = None
    till_cash_total = 0.0
    till_card_total = 0.0
    z_reading_total = 0.0
    till_cash_count = 0
    till_card_count = 0
    z_reading_count = 0

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        money_value = _pattern_money_value(_TOTAL_CASH_PATTERN, line)
        if money_value is not None:
            declared_total_cash = money_value
            continue

        money_value = _pattern_money_value(_TOTAL_CARD_PATTERN, line)
        if money_value is not None:
            declared_total_card = money_value
            continue

        money_value = _pattern_money_value(_TOTAL_SALES_PATTERN, line)
        if money_value is not None:
            declared_total_sales = money_value
            continue

        money_value = _pattern_money_value(_TILL_CASH_PATTERN, line)
        if money_value is not None:
            till_cash_total += money_value
            till_cash_count += 1
            continue

        money_value = _pattern_money_value(_TILL_CARD_PATTERN, line)
        if money_value is not None:
            till_card_total += money_value
            till_card_count += 1
            continue

        money_value = _pattern_money_value(_Z_READING_PATTERN, line)
        if money_value is not None:
            z_reading_total += money_value
            z_reading_count += 1

    return _SalesTotalsEvidence(
        declared_total_cash=declared_total_cash,
        declared_total_card=declared_total_card,
        declared_total_sales=declared_total_sales,
        till_cash_total=round(till_cash_total, 2) if till_cash_count else None,
        till_card_total=round(till_card_total, 2) if till_card_count else None,
        z_reading_total=round(z_reading_total, 2) if z_reading_count else None,
    )


def _pattern_money_value(pattern: re.Pattern[str], line: str) -> float | None:
    """Return one parsed money value when a line matches the expected pattern."""

    match = pattern.match(line)
    if match is None:
        return None
    return parse_money(match.group(1))


def _raw_sales_text(payload: Mapping[str, Any]) -> str | None:
    """Return raw sales text when it is available to the validator."""

    raw_text = payload.get("raw_text")
    if isinstance(raw_text, str) and raw_text.strip():
        return raw_text

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return None

    text = raw_message.get("text")
    if isinstance(text, str) and text.strip():
        return text
    return None


def _money_differs(left: float | None, right: float | None) -> bool:
    """Return whether two optional money values differ materially."""

    return left is not None and right is not None and abs(left - right) > TOLERANCE


def _money_difference(left: float | None, right: float | None) -> float | None:
    """Return the signed expected-minus-declared difference when both values exist."""

    if left is None or right is None:
        return None
    return round(right - left, 2)
