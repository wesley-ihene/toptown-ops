"""Deterministic Phase 1 SOP validation for store monitoring payloads."""

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

_ALLOWED_STATUSES = {"ok", "issue", "critical"}


def validate_store_monitoring(payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one structured store monitoring payload."""

    rejections = validate_common_fields(payload)
    metrics = get_metrics(payload, rejections)
    items = get_items(payload, rejections)

    total_checks = add_non_negative_number(rejections, value=metrics.get("total_checks"), field="metrics.total_checks")
    issue_count = add_non_negative_number(rejections, value=metrics.get("issue_count"), field="metrics.issue_count")
    critical_count = add_non_negative_number(
        rejections,
        value=metrics.get("critical_count"),
        field="metrics.critical_count",
    )

    counted_issues = 0
    counted_critical = 0
    for index, item in enumerate(items):
        rejections.extend(require_fields(item, ("check_name", "status"), item_prefix=f"items[{index}]"))
        status_value = item.get("status")
        normalized_status = status_value.strip().lower() if isinstance(status_value, str) else ""
        if normalized_status and normalized_status not in _ALLOWED_STATUSES:
            rejections.append(
                make_rejection(
                    code=INVALID_STATUS,
                    message="Store monitoring status must be ok, issue, or critical.",
                    field=f"items[{index}].status",
                )
            )
        elif normalized_status == "issue":
            counted_issues += 1
        elif normalized_status == "critical":
            counted_critical += 1

    if items and not counts_match(total_checks, len(items)):
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The total checks metric must match the item count.",
                field="metrics.total_checks",
            )
        )

    if issue_count is not None and int(issue_count) != counted_issues:
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The issue count metric must match issue items.",
                field="metrics.issue_count",
            )
        )

    if critical_count is not None and int(critical_count) != counted_critical:
        rejections.append(
            make_rejection(
                code=INVALID_COUNT_MISMATCH,
                message="The critical count metric must match critical items.",
                field="metrics.critical_count",
            )
        )

    return build_result("store_monitoring", rejections)
