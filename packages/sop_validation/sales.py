"""Deterministic Phase 1 SOP validation for sales payloads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field as dataclass_field
import re
from typing import Any

from packages.validation import build_rejection
from packages.validation.sales_reconciliation import SalesReconciliation, TOLERANCE, build_sales_reconciliation

from .common import (
    add_non_negative_number,
    build_result,
    get_metrics,
    make_rejection,
    validate_common_fields,
)
from .contracts import ValidationResult
from .rejection_codes import INVALID_NUMERIC_VALUE, INVALID_TOTALS, UNSUPPORTED_REPORT_TITLE

SALES_TOTALS_MISMATCH = "sales_totals_mismatch"
_CANONICAL_SALES_TITLE_PATTERN = re.compile(
    r"^\s*day[\s-]*end\s+sales\s+report\.?\s*$",
    flags=re.IGNORECASE,
)
_WEAK_SALES_TITLE_PATTERN = re.compile(r"^\s*sales\s+report\.?\s*$", flags=re.IGNORECASE)


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


def validate_sales(payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one structured sales payload conservatively."""

    rejections = validate_common_fields(payload)
    report_title_rejection = _sales_title_rejection(payload)
    if report_title_rejection is not None:
        rejections.append(report_title_rejection)
    metrics = get_metrics(payload, rejections)

    gross_sales = add_non_negative_number(rejections, value=metrics.get("gross_sales"), field="metrics.gross_sales")
    net_sales = add_non_negative_number(rejections, value=metrics.get("net_sales"), field="metrics.net_sales")
    cash_sales = add_non_negative_number(rejections, value=metrics.get("cash_sales"), field="metrics.cash_sales")
    eftpos_sales = add_non_negative_number(rejections, value=metrics.get("eftpos_sales"), field="metrics.eftpos_sales")
    mobile_money_sales = add_non_negative_number(
        rejections,
        value=metrics.get("mobile_money_sales"),
        field="metrics.mobile_money_sales",
    )
    traffic = add_non_negative_number(rejections, value=metrics.get("traffic"), field="metrics.traffic")
    served = add_non_negative_number(rejections, value=metrics.get("served"), field="metrics.served")
    reconciliation = build_sales_reconciliation(metrics=metrics, raw_text=_raw_sales_text(payload))
    if isinstance(payload, dict):
        payload["reconciliation"] = reconciliation.to_payload()

    payment_total = 0.0
    payment_part_count = 0
    for value in (cash_sales, eftpos_sales, mobile_money_sales):
        if value is not None:
            payment_total += value
            payment_part_count += 1

    canonical_rejection = _sales_totals_mismatch_rejection(reconciliation=reconciliation)
    if canonical_rejection is not None:
        rejections.append(canonical_rejection)
    else:
        compared_sales_total = net_sales if net_sales is not None else gross_sales
        if compared_sales_total is None and gross_sales is not None:
            item_returns_total = add_non_negative_number(
                rejections,
                value=metrics.get("item_returns_total", metrics.get("total_returns", metrics.get("item_returns"))),
                field="metrics.item_returns",
            )
            if item_returns_total is not None:
                compared_sales_total = round(gross_sales - item_returns_total, 2)
        if compared_sales_total is None or payment_part_count < 2:
            compared_sales_total = None
        else:
            expected_total_sales = reconciliation.expected_total_sales
            if expected_total_sales is None:
                expected_total_sales = round(payment_total, 2)
            if abs(compared_sales_total - expected_total_sales) > TOLERANCE:
                rejections.append(
                    make_rejection(
                        code=INVALID_TOTALS,
                        message="Sales total must match the sum of payment totals after returns.",
                        field="metrics.net_sales" if net_sales is not None else "metrics.gross_sales",
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


def _sales_title_rejection(payload: Mapping[str, Any]) -> _StructuredRejection | None:
    """Reject weak title aliases like `Sales report.` while keeping canonical titles valid."""

    raw_text = _raw_sales_text(payload)
    if not isinstance(raw_text, str) or not raw_text.strip():
        return None

    title_candidates = [line.strip() for line in raw_text.splitlines()[:8] if line.strip()]
    if any(_CANONICAL_SALES_TITLE_PATTERN.match(line) for line in title_candidates):
        return None

    weak_title = next((line for line in title_candidates if _WEAK_SALES_TITLE_PATTERN.match(line)), None)
    if weak_title is None:
        return None

    return _StructuredRejection(
        code=UNSUPPORTED_REPORT_TITLE,
        message="Unsupported report title.",
        field="raw_text.title",
        extra={
            "expected_title": "DAY-END SALES REPORT",
            "received_title": weak_title,
        },
    )


def _sales_totals_mismatch_rejection(
    *,
    reconciliation: SalesReconciliation,
) -> _StructuredRejection | None:
    """Return one canonical totals mismatch rejection when diagnostics are available."""

    if reconciliation.unexplained_variance <= TOLERANCE or not reconciliation.diagnostics_present:
        return None

    return _StructuredRejection(
        code=SALES_TOTALS_MISMATCH,
        message="Sales totals do not match till/payment totals.",
        field="metrics.gross_sales",
        extra={
            "declared_total_cash": reconciliation.declared_total_cash,
            "expected_total_cash": reconciliation.expected_total_cash,
            "declared_total_card": reconciliation.declared_total_card,
            "expected_total_card": reconciliation.expected_total_card,
            "declared_total_sales": reconciliation.declared_total_sales,
            "expected_total_sales": reconciliation.expected_total_sales,
            "declared_z_reading": reconciliation.declared_z_reading,
            "expected_z_reading": reconciliation.expected_z_reading,
            "cash_over": reconciliation.cash_over,
            "cash_down": reconciliation.cash_down,
            "item_return_adjustment": reconciliation.item_return_adjustment,
            "eftpos_reconciliation_adjustment": reconciliation.eftpos_reconciliation_adjustment,
            "variance_explained_by_cash_over": reconciliation.variance_explained_by_cash_over,
            "variance_explained_by_cash_down": reconciliation.variance_explained_by_cash_down,
            "variance_explained": reconciliation.variance_explained,
            "unexplained_variance": reconciliation.unexplained_variance,
            "cash_difference": _money_difference(reconciliation.declared_total_cash, reconciliation.expected_total_cash),
            "card_difference": _money_difference(reconciliation.declared_total_card, reconciliation.expected_total_card),
            "sales_difference": _money_difference(
                reconciliation.declared_total_sales,
                reconciliation.expected_total_sales,
            ),
            "z_reading_difference": _money_difference(
                reconciliation.declared_z_reading,
                reconciliation.expected_z_reading,
            ),
        },
    )


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


def _money_difference(left: float | None, right: float | None) -> float | None:
    """Return the signed expected-minus-declared difference when both values exist."""

    if left is None or right is None:
        return None
    return round(right - left, 2)
