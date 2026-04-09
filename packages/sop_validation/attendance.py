"""Deterministic Phase 1 SOP validation for attendance payloads."""

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
from .rejection_codes import INVALID_COUNT_MISMATCH, INVALID_STATUS

_ALLOWED_STATUSES = {"present", "absent", "off", "leave"}


def validate_attendance(payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one structured attendance payload."""

    rejections = validate_common_fields(payload)
    metrics = get_metrics(payload, rejections)
    items = get_items(payload, rejections)

    total_staff_listed = add_non_negative_number(
        rejections,
        value=metrics.get("total_staff_listed"),
        field="metrics.total_staff_listed",
    )
    present_count = add_non_negative_number(rejections, value=metrics.get("present_count"), field="metrics.present_count")
    absent_count = add_non_negative_number(rejections, value=metrics.get("absent_count"), field="metrics.absent_count")
    off_count = add_non_negative_number(rejections, value=metrics.get("off_count"), field="metrics.off_count")
    leave_count = add_non_negative_number(rejections, value=metrics.get("leave_count"), field="metrics.leave_count")

    counted_statuses = {status: 0 for status in _ALLOWED_STATUSES}
    for index, item in enumerate(items):
        rejections.extend(require_fields(item, ("staff_name", "status"), item_prefix=f"items[{index}]"))
        status_value = item.get("status")
        normalized_status = status_value.strip().lower() if isinstance(status_value, str) else ""
        if normalized_status and normalized_status not in _ALLOWED_STATUSES:
            rejections.append(
                make_rejection(
                    code=INVALID_STATUS,
                    message="Attendance status must be present, absent, off, or leave.",
                    field=f"items[{index}].status",
                )
            )
        elif normalized_status:
            counted_statuses[normalized_status] += 1

    if items and not counts_match(total_staff_listed, len(items)):
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The total staff listed metric must match the item count.",
                field="metrics.total_staff_listed",
            )
        )

    metric_pairs = (
        ("present_count", present_count, counted_statuses["present"]),
        ("absent_count", absent_count, counted_statuses["absent"]),
        ("off_count", off_count, counted_statuses["off"]),
        ("leave_count", leave_count, counted_statuses["leave"]),
    )
    for field_name, expected_value, actual_value in metric_pairs:
        if expected_value is not None and int(expected_value) != actual_value:
            rejections.append(
                make_rejection(
                    code=INVALID_COUNT_MISMATCH,
                    message=f"The metric `{field_name}` must match the item status counts.",
                    field=f"metrics.{field_name}",
                )
            )

    return build_result("attendance", rejections)
