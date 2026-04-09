"""Deterministic report-type router for Phase 1 SOP validation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from .attendance import validate_attendance
from .bale_release import validate_bale_release
from .common import build_result, make_rejection
from .contracts import ValidationResult
from .rejection_codes import UNSUPPORTED_REPORT_TYPE
from .sales import validate_sales
from .staff_performance import validate_staff_performance
from .store_monitoring import validate_store_monitoring
from .supervisor_control import validate_supervisor_control

Validator = Callable[[Mapping[str, Any]], ValidationResult]

_VALIDATORS: dict[str, Validator] = {
    "sales": validate_sales,
    "sales_income": validate_sales,
    "staff_performance": validate_staff_performance,
    "attendance": validate_attendance,
    "staff_attendance": validate_attendance,
    "hr_attendance": validate_attendance,
    "bale_release": validate_bale_release,
    "pricing_stock_release": validate_bale_release,
    "bale_summary": validate_bale_release,
    "supervisor_control": validate_supervisor_control,
    "store_monitoring": validate_store_monitoring,
}


def get_validator(report_type: str) -> Validator | None:
    """Return the validator for one supported report type."""

    return _VALIDATORS.get(report_type)


def validate_report(report_type: str, payload: Mapping[str, Any]) -> ValidationResult:
    """Validate one payload via its routed Phase 1 validator."""

    validator = get_validator(report_type)
    if validator is None:
        return build_result(
            report_type,
            [
                make_rejection(
                    code=UNSUPPORTED_REPORT_TYPE,
                    message=f"Unsupported SOP validator report type: {report_type}.",
                    field="report_type",
                )
            ],
        )
    return validator(payload)
