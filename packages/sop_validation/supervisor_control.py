"""Deterministic Phase 1 SOP validation for supervisor control payloads."""

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
from .rejection_codes import INVALID_COUNT_MISMATCH, MISSING_CONFIRMATION, UNRESOLVED_EXCEPTION

_OPEN_ACTIONS = {"escalated", "open", "pending"}


def validate_supervisor_control(payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one structured supervisor control payload."""

    rejections = validate_common_fields(payload)
    metrics = get_metrics(payload, rejections)
    items = get_items(payload, rejections)

    exception_count = add_non_negative_number(
        rejections,
        value=metrics.get("exception_count"),
        field="metrics.exception_count",
    )

    unresolved_found = False
    for index, item in enumerate(items):
        rejections.extend(
            require_fields(
                item,
                ("exception_type", "action_taken", "supervisor_confirmed"),
                item_prefix=f"items[{index}]",
            )
        )
        confirmation = item.get("supervisor_confirmed")
        if isinstance(confirmation, str) and confirmation.strip().upper() != "YES":
            rejections.append(
                make_rejection(
                    code=MISSING_CONFIRMATION,
                    message="Every supervisor control exception must include explicit YES confirmation.",
                    field=f"items[{index}].supervisor_confirmed",
                )
            )

        action_taken = item.get("action_taken")
        if isinstance(action_taken, str) and action_taken.strip().lower() in _OPEN_ACTIONS:
            unresolved_found = True

    if items and not counts_match(exception_count, len(items)):
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The exception count metric must match the item count.",
                field="metrics.exception_count",
            )
        )

    if unresolved_found:
        rejections.append(
            make_rejection(
                code=UNRESOLVED_EXCEPTION,
                message="Supervisor control items with open or escalated actions cannot pass validation.",
                field="items",
            )
        )

    return build_result("supervisor_control", rejections)
