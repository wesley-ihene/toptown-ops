"""Hard safety rules and target constraints for adaptive SOP learning."""

from __future__ import annotations

import re

REGISTRY_VERSION = "v1"
SUGGESTION_THRESHOLD = 3
AUTO_APPLY_CONFIDENCE = 0.85

FINANCIAL_FIELDS = frozenset(
    {
        "Total_Sales",
        "Total_Cash",
        "Total_Card",
        "Z_Reading",
        "Cash_Over",
        "Cash_Down",
        "Amount",
        "Total_Amount",
    }
)

ALLOWED_TARGETS: dict[str, frozenset[str]] = {
    "sales_income": frozenset(
        {
            "Traffic",
            "Served",
            "Sales_Per_Customer",
            "Conversion_Rate",
            "Monitor",
            "Cash_Variance_Reason",
        }
    ),
    "pricing_stock_release": frozenset(
        {
            "Item",
            "Qty",
            "Weight",
            "Prepared_By",
            "Checked_By",
            "Note",
        }
    ),
    "hr_attendance": frozenset({"Name", "Status"}),
    "supervisor_control": frozenset(
        {
            "Cash variance",
            "Staffing issues",
            "Stock issues affecting sales",
            "Pricing/system issues",
            "Escalation",
        }
    ),
}

_INTERNAL_TO_TARGET: dict[str, dict[str, str]] = {
    "sales_income": {
        "traffic": "Traffic",
        "served": "Served",
        "customer_count": "Served",
        "sales_per_customer": "Sales_Per_Customer",
        "conversion_rate": "Conversion_Rate",
        "monitor": "Monitor",
        "cash_variance_reason": "Cash_Variance_Reason",
    },
    "pricing_stock_release": {
        "item_name": "Item",
        "qty": "Qty",
        "weight": "Weight",
        "prepared_by": "Prepared_By",
        "checked_by": "Checked_By",
        "notes": "Note",
    },
    "hr_attendance": {
        "staff_name": "Name",
        "status": "Status",
    },
    "supervisor_control": {
        "cash_variance": "Cash variance",
        "staffing_issues": "Staffing issues",
        "stock_issues_affecting_sales": "Stock issues affecting sales",
        "pricing_system_issues": "Pricing/system issues",
        "escalation": "Escalation",
    },
}

_TARGET_PAYLOAD_PATHS: dict[str, dict[str, tuple[str, ...]]] = {
    "sales_income": {
        "Traffic": ("metrics", "traffic"),
        "Served": ("metrics", "served"),
    },
    "pricing_stock_release": {
        "Prepared_By": ("provenance", "prepared_by"),
        "Checked_By": ("provenance", "checked_by"),
    },
    "hr_attendance": {},
    "supervisor_control": {},
}

_SOURCE_FIELDS_BY_TARGET: dict[str, dict[str, str]] = {
    "sales_income": {
        "Traffic": "customer_count",
        "Served": "customer_count",
        "Sales_Per_Customer": "customer_count",
        "Conversion_Rate": "customer_count",
        "Monitor": "notes",
        "Cash_Variance_Reason": "notes",
    },
    "pricing_stock_release": {
        "Item": "line_item",
        "Qty": "line_item",
        "Weight": "line_item",
        "Prepared_By": "provenance",
        "Checked_By": "provenance",
        "Note": "notes",
    },
    "hr_attendance": {
        "Name": "name",
        "Status": "status",
    },
    "supervisor_control": {
        "Cash variance": "checklist",
        "Staffing issues": "checklist",
        "Stock issues affecting sales": "checklist",
        "Pricing/system issues": "checklist",
        "Escalation": "checklist",
    },
}

_PREFERRED_FORMATS: dict[str, str] = {
    "sales_income": "CUSTOMER COUNT\nTraffic: <number>\nServed: <number>",
    "pricing_stock_release": "PREPARED BY\nPrepared_By: <name>\nChecked_By: <name>\nQty: <number>\nNote: <text>",
    "hr_attendance": "ATTENDANCE\n<Name> - <Status>",
    "supervisor_control": (
        "SUPERVISOR CONTROL REPORT\n"
        "Cash variance: <YES/NO>\n"
        "Staffing issues: <YES/NO>\n"
        "Stock issues affecting sales: <YES/NO>\n"
        "Pricing/system issues: <YES/NO>\n"
        "Escalation: <details>"
    ),
}

_NORMALIZE_PATTERN = re.compile(r"[^a-z0-9]+")


def normalize_token(value: object) -> str:
    """Return a comparison-safe lowercase token string."""

    if not isinstance(value, str):
        return ""
    return " ".join(_NORMALIZE_PATTERN.sub(" ", value.casefold()).split())


def is_allowed_target(report_type: str, target: str) -> bool:
    """Return whether one adaptive target is allowed for a report type."""

    allowed = ALLOWED_TARGETS.get(report_type)
    if allowed is None:
        return False
    normalized_target = normalize_token(target)
    return any(normalize_token(candidate) == normalized_target for candidate in allowed)


def is_financial_target(target: str) -> bool:
    """Return whether one target falls under the hard financial boundary."""

    normalized_target = normalize_token(target)
    return any(normalize_token(candidate) == normalized_target for candidate in FINANCIAL_FIELDS)


def target_display_for_internal(report_type: str, internal_name: str | None) -> str | None:
    """Return the display target for one internal field when configured."""

    if internal_name is None:
        return None
    report_targets = _INTERNAL_TO_TARGET.get(report_type, {})
    return report_targets.get(internal_name)


def payload_path_for_target(report_type: str, target: str) -> tuple[str, ...] | None:
    """Return the structured payload path for one safely auto-applicable target."""

    report_paths = _TARGET_PAYLOAD_PATHS.get(report_type, {})
    for candidate, path in report_paths.items():
        if normalize_token(candidate) == normalize_token(target):
            return path
    return None


def source_field_for_target(report_type: str, target: str) -> str:
    """Return the registry source-field bucket for one target."""

    report_fields = _SOURCE_FIELDS_BY_TARGET.get(report_type, {})
    for candidate, source_field in report_fields.items():
        if normalize_token(candidate) == normalize_token(target):
            return source_field
    return "adaptive_field"


def preferred_format(report_type: str) -> str | None:
    """Return the recommended future format text for one report type."""

    return _PREFERRED_FORMATS.get(report_type)

