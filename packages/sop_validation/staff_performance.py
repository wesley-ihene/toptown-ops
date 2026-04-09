"""Deterministic Phase 1 SOP validation for staff performance payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .common import (
    add_non_negative_number,
    build_result,
    counts_match,
    get_items,
    get_metrics,
    make_rejection,
    require_fields,
    validate_common_fields,
)
from .contracts import ValidationResult
from .rejection_codes import INVALID_COUNT_MISMATCH


def validate_staff_performance(payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one structured staff performance payload."""

    rejections = validate_common_fields(payload)
    metrics = get_metrics(payload, rejections)
    items = get_items(payload, rejections)

    total_staff_records = add_non_negative_number(
        rejections,
        value=metrics.get("total_staff_records"),
        field="metrics.total_staff_records",
    )
    total_items_moved = add_non_negative_number(
        rejections,
        value=metrics.get("total_items_moved"),
        field="metrics.total_items_moved",
    )

    parsed_items_moved = 0
    for index, item in enumerate(items):
        rejections.extend(require_fields(item, ("staff_name", "duty_status"), item_prefix=f"items[{index}]"))
        moved = add_non_negative_number(
            rejections,
            value=item.get("items_moved"),
            field=f"items[{index}].items_moved",
        )
        parsed_items_moved += int(moved or 0)

    if items and not counts_match(total_staff_records, len(items)):
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The total staff record metric must match the item count.",
                field="metrics.total_staff_records",
            )
        )

    if total_items_moved is not None and total_items_moved != parsed_items_moved:
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The total items moved metric must match the sum of item rows.",
                field="metrics.total_items_moved",
            )
        )

    return build_result("staff_performance", rejections)
