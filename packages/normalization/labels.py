"""Family-aware label normalization."""

from __future__ import annotations

import re

from .types import AppliedRule, NormalizedValue

_NORMALIZE_PATTERN = re.compile(r"[^a-z0-9]+")

_LABEL_RULES: dict[str, dict[str, tuple[str, ...]]] = {
    "sales": {
        "DAY-END SALES REPORT": ("day end sales report", "day-end sales report"),
        "Branch": ("branch", "shop", "location"),
        "Date": ("date", "report date", "sales date"),
        "Cashier": ("cashier", "served by"),
        "Assistant": ("assistant", "assistant cashier"),
        "Balanced_By": ("balanced by", "balance by"),
        "Supervisor": ("manager",),
        "Supervisor_Confirmation": ("supervisor confirmation", "confirmed by"),
        "Notes": ("notes", "note", "comment", "remark", "remarks"),
        "Total_Sales": ("gross sales", "sales", "total sales"),
        "Total_Cash": ("cash sales", "cash", "t cash", "t/cash", "total cash"),
        "Total_Card": ("eftpos sales", "eftpos", "card sales", "t card", "t/card", "total card"),
        "Mobile_Money": ("mobile money", "mobile sales", "mobile money sales"),
        "Till_Total": ("till total", "till", "cash in till"),
        "Deposit_Total": ("deposit total", "banking", "deposit"),
        "Customer_Count": ("customer count", "customers", "total customers"),
        "Traffic": ("traffic", "foot traffic", "store traffic", "main door"),
        "Served": ("served", "customers served", "guest customer serve", "guest/customer serve"),
        "Labor_Hours": ("labor hours", "labour hours", "hours worked"),
        "Z_Reading": ("z reading", "z/reading"),
    },
    "attendance": {
        "Branch": ("branch", "shop", "location"),
        "Date": ("date", "report date", "attendance date"),
        "Total_Staff": ("total staff", "staff total", "headcount", "total headcount"),
        "Notes": ("notes", "note", "remark", "remarks"),
        "P": ("p", "present"),
        "OFF": ("off", "off duty"),
        "LEAVE": ("leave", "annual leave", "anual leave"),
        "ABSENT": ("absent",),
        "SICK": ("sick",),
        "SUSPENDED": ("suspend", "suspended"),
    },
    "bale_summary": {
        "Branch": ("branch", "shop", "location"),
        "Date": ("date", "report date"),
        "Prepared_By": ("prepared by",),
        "Qty": ("qty", "quantity"),
        "Amount": ("amt", "amount", "value"),
        "Total_Qty": ("total qty", "total quantity"),
        "Total_Amount": ("total amount",),
    },
}

_FAMILY_ALIASES = {
    "sales_income": "sales",
    "sales": "sales",
    "attendance": "attendance",
    "staff_attendance": "attendance",
    "hr_attendance": "attendance",
    "bale_release": "bale_summary",
    "pricing_stock_release": "bale_summary",
    "bale_summary": "bale_summary",
}

_INTERNAL_FIELD_NAMES = {
    "Branch": "branch",
    "Date": "report_date",
    "Cashier": "cashier",
    "Assistant": "assistant",
    "Balanced_By": "balanced_by",
    "Supervisor": "supervisor",
    "Supervisor_Confirmation": "supervisor_confirmation",
    "Notes": "notes",
    "Total_Sales": "gross_sales",
    "Total_Cash": "cash_sales",
    "Total_Card": "eftpos_sales",
    "Mobile_Money": "mobile_money_sales",
    "Till_Total": "till_total",
    "Deposit_Total": "deposit_total",
    "Customer_Count": "customer_count",
    "Traffic": "traffic",
    "Served": "served",
    "Labor_Hours": "labor_hours",
    "Z_Reading": "z_reading",
    "Total_Staff": "total_staff",
    "Prepared_By": "prepared_by",
    "Qty": "qty",
    "Amount": "amount",
    "Total_Qty": "total_qty",
    "Total_Amount": "total_amount",
}


def normalize_label(raw_value: str, report_family: str | None = None) -> NormalizedValue:
    """Return one canonical label for a known report family."""

    raw = raw_value
    normalized_input = _normalize_label_key(raw_value)
    result = NormalizedValue(raw_value=raw, metadata={"value_type": "label"})
    family = _FAMILY_ALIASES.get((report_family or "").strip().casefold(), (report_family or "").strip().casefold())
    family_rules = _LABEL_RULES.get(family)
    if not normalized_input or family_rules is None:
        result.hard_errors.append("unknown_label_family" if family_rules is None else "empty_label")
        return result

    for canonical, aliases in family_rules.items():
        normalized_aliases = {_normalize_label_key(alias) for alias in aliases}
        normalized_aliases.add(_normalize_label_key(canonical))
        if normalized_input not in normalized_aliases:
            continue
        result.normalized_value = canonical
        result.confidence = 1.0
        result.metadata.update(
            {
                "report_family": family,
                "field_name": _INTERNAL_FIELD_NAMES.get(canonical),
            }
        )
        result.applied_rules.append(
            AppliedRule(
                name="label_alias_matched",
                raw_value=raw,
                normalized_value=canonical,
                details={"report_family": family},
            )
        )
        return result

    result.hard_errors.append("unknown_label")
    result.metadata["report_family"] = family
    return result


def internal_field_name(raw_value: str, report_family: str | None = None) -> str | None:
    """Return the internal field name for a label when known."""

    result = normalize_label(raw_value, report_family=report_family)
    field_name = result.metadata.get("field_name")
    return field_name if isinstance(field_name, str) else None


def _normalize_label_key(value: str) -> str:
    cleaned = value.casefold().strip()
    cleaned = cleaned.replace("_", " ").replace("/", " ").replace("-", " ")
    cleaned = _NORMALIZE_PATTERN.sub(" ", cleaned)
    return " ".join(cleaned.split())
