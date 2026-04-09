"""Shared deterministic helpers for Phase 1 SOP validation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from numbers import Real
from typing import Any

from .contracts import Rejection, ValidationResult
from .rejection_codes import (
    INVALID_NUMERIC_VALUE,
    INVALID_REPORT_DATE,
    MISSING_BRANCH,
    MISSING_ITEMS,
    MISSING_METRICS,
    MISSING_REPORT_DATE,
    MISSING_REQUIRED_FIELD,
)


def make_rejection(*, code: str, message: str, field: str | None = None) -> Rejection:
    """Create one explicit rejection entry."""

    return Rejection(code=code, message=message, field=field)


def build_result(report_type: str, rejections: list[Rejection]) -> ValidationResult:
    """Return the final deterministic validation result."""

    return ValidationResult(
        report_type=report_type,
        accepted=not rejections,
        rejections=rejections,
    )


def validate_common_fields(payload: Mapping[str, Any]) -> list[Rejection]:
    """Return base payload rejections shared across validators."""

    rejections: list[Rejection] = []
    branch = payload.get("branch")
    report_date = payload.get("report_date")

    if not isinstance(branch, str) or not branch.strip():
        rejections.append(
            make_rejection(
                code=MISSING_BRANCH,
                message="The payload branch must be a non-empty string.",
                field="branch",
            )
        )

    if not isinstance(report_date, str) or not report_date.strip():
        rejections.append(
            make_rejection(
                code=MISSING_REPORT_DATE,
                message="The payload report_date must be a non-empty ISO date string.",
                field="report_date",
            )
        )
    elif not is_iso_date(report_date):
        rejections.append(
            make_rejection(
                code=INVALID_REPORT_DATE,
                message="The payload report_date must use YYYY-MM-DD format.",
                field="report_date",
            )
        )

    return rejections


def get_metrics(payload: Mapping[str, Any], rejections: list[Rejection]) -> Mapping[str, Any]:
    """Return the metrics mapping or append one rejection."""

    metrics = payload.get("metrics")
    if isinstance(metrics, Mapping):
        return metrics

    rejections.append(
            make_rejection(
                code=MISSING_METRICS,
                message="The payload metrics field must be a mapping.",
                field="metrics",
            )
    )
    return {}


def get_items(payload: Mapping[str, Any], rejections: list[Rejection]) -> list[Mapping[str, Any]]:
    """Return normalized item mappings or append one rejection."""

    items = payload.get("items")
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes)):
        rejections.append(
            make_rejection(
                code=MISSING_ITEMS,
                message="The payload items field must be a non-empty list.",
                field="items",
            )
        )
        return []

    normalized = [item for item in items if isinstance(item, Mapping)]
    if not normalized:
        rejections.append(
            make_rejection(
                code=MISSING_ITEMS,
                message="The payload items field must include at least one mapping item.",
                field="items",
            )
        )
    return normalized


def require_fields(item: Mapping[str, Any], fields: Sequence[str], *, item_prefix: str) -> list[Rejection]:
    """Return field-level rejections for one mapping item."""

    rejections: list[Rejection] = []
    for field_name in fields:
        value = item.get(field_name)
        if not isinstance(value, str) or not value.strip():
            rejections.append(
                make_rejection(
                    code=MISSING_REQUIRED_FIELD,
                    message=f"The field `{field_name}` is required.",
                    field=f"{item_prefix}.{field_name}",
                )
            )
    return rejections


def add_non_negative_number(
    rejections: list[Rejection],
    *,
    value: Any,
    field: str,
) -> float | None:
    """Validate one non-negative numeric field and return it as float."""

    if value is None:
        return None
    if not isinstance(value, Real) or isinstance(value, bool):
        rejections.append(
            make_rejection(
                code=INVALID_NUMERIC_VALUE,
                message=f"The field `{field}` must be numeric.",
                field=field,
            )
        )
        return None

    numeric_value = float(value)
    if numeric_value < 0:
        rejections.append(
            make_rejection(
                code=INVALID_NUMERIC_VALUE,
                message=f"The field `{field}` must be non-negative.",
                field=field,
            )
        )
        return None
    return numeric_value


def counts_match(expected: float | None, actual: int) -> bool:
    """Return whether an optional numeric metric matches an integer count."""

    return expected is None or int(expected) == actual


def within_money_tolerance(left: float | None, right: float | None, *, tolerance: float = 0.01) -> bool:
    """Return whether two optional money values match within a small tolerance."""

    if left is None or right is None:
        return True
    return abs(left - right) <= tolerance


def is_iso_date(value: str) -> bool:
    """Return whether the value is a valid ISO calendar date."""

    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True
