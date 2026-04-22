"""Deterministic Phase 1 SOP validation for sales payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .common import (
    add_non_negative_number,
    build_result,
    get_metrics,
    make_rejection,
    validate_common_fields,
)
from .contracts import ValidationResult
from .rejection_codes import INVALID_NUMERIC_VALUE, INVALID_TOTALS


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

    if gross_sales is not None and payment_part_count >= 2 and abs(gross_sales - payment_total) > 0.01:
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
