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

_ALLOWED_STATUSES = {
    "present",
    "present_half",
    "absent",
    "off",
    "leave",
    "sick",
    "suspend",
    "awn",
    "awon",
    "lay_off",
    "non_active",
    "transfer",
    "late",
    "nil",
}
_STATUS_BUCKETS = {
    "present": "present",
    "present_half": "present_half",
    "absent": "absent",
    "off": "off",
    "leave": "leave",
    "sick": "absent",
    "suspend": "absent",
    "awn": "absent",
    "awon": "absent",
    "late": "absent",
    "lay_off": "off",
    "non_active": "non_active",
    "transfer": "off",
    "nil": "off",
}


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
    total_staff = add_non_negative_number(rejections, value=metrics.get("total_staff"), field="metrics.total_staff")
    non_active_count = add_non_negative_number(rejections, value=metrics.get("non_active"), field="metrics.non_active")

    counted_statuses = {"present": 0, "present_half": 0, "absent": 0, "off": 0, "leave": 0, "non_active": 0}
    for index, item in enumerate(items):
        rejections.extend(require_fields(item, ("staff_name", "status"), item_prefix=f"items[{index}]"))
        status_value = item.get("status")
        normalized_status = status_value.strip().lower() if isinstance(status_value, str) else ""
        if normalized_status and normalized_status not in _ALLOWED_STATUSES:
            rejections.append(
                make_rejection(
                    code=INVALID_STATUS,
                    message="Attendance status must use one supported attendance value.",
                    field=f"items[{index}].status",
                )
            )
        elif normalized_status:
            counted_statuses[_STATUS_BUCKETS[normalized_status]] += 1

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

    derived_total_staff = len(items) - counted_statuses["non_active"] if items else 0
    if total_staff is not None and int(total_staff) != derived_total_staff:
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The metric `total_staff` must match the active staff headcount derived from item statuses.",
                field="metrics.total_staff",
            )
        )

    if non_active_count is not None and int(non_active_count) != counted_statuses["non_active"]:
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The metric `non_active` must match the item status counts.",
                field="metrics.non_active",
            )
        )

    return build_result("attendance", rejections)
