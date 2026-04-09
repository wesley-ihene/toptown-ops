"""Deterministic Phase 1 SOP validation for bale release payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .common import (
    add_non_negative_number,
    build_result,
    get_items,
    get_metrics,
    make_rejection,
    require_fields,
    validate_common_fields,
    within_money_tolerance,
)
from .contracts import ValidationResult
from .rejection_codes import INVALID_COUNT_MISMATCH, INVALID_TOTALS


def validate_bale_release(payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one structured bale release payload."""

    rejections = validate_common_fields(payload)
    metrics = get_metrics(payload, rejections)
    items = get_items(payload, rejections)

    bales_processed = add_non_negative_number(
        rejections,
        value=metrics.get("bales_processed"),
        field="metrics.bales_processed",
    )
    bales_released = add_non_negative_number(
        rejections,
        value=metrics.get("bales_released"),
        field="metrics.bales_released",
    )
    total_qty = add_non_negative_number(rejections, value=metrics.get("total_qty"), field="metrics.total_qty")
    total_amount = add_non_negative_number(rejections, value=metrics.get("total_amount"), field="metrics.total_amount")

    item_total_qty = 0
    item_total_amount = 0.0
    for index, item in enumerate(items):
        rejections.extend(require_fields(item, ("bale_id", "item_name"), item_prefix=f"items[{index}]"))
        qty = add_non_negative_number(rejections, value=item.get("qty"), field=f"items[{index}].qty")
        amount = add_non_negative_number(rejections, value=item.get("amount"), field=f"items[{index}].amount")
        item_total_qty += int(qty or 0)
        item_total_amount += float(amount or 0.0)

    if bales_processed is not None and bales_released is not None and bales_released > bales_processed:
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="Released bale count cannot exceed processed bale count.",
                field="metrics.bales_released",
            )
        )

    if total_qty is not None and int(total_qty) != item_total_qty:
        rejections.append(
            make_rejection(
                code=INVALID_TOTALS,
                message="The total quantity metric must match the sum of item quantities.",
                field="metrics.total_qty",
            )
        )

    if total_amount is not None and not within_money_tolerance(total_amount, item_total_amount):
        rejections.append(
            make_rejection(
                code=INVALID_TOTALS,
                message="The total amount metric must match the sum of item amounts.",
                field="metrics.total_amount",
            )
        )

    return build_result("bale_release", rejections)
