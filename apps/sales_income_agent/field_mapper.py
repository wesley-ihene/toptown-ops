"""Canonical field mapping helpers for sales report wording variants."""

from __future__ import annotations

from typing import Final

from packages.branch_registry import canonical_branch_slug as canonical_upstream_branch_slug
from packages.normalization.labels import internal_field_name

SECTION_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "sales_header": ("sales report", "sales header", "sales summary", "daily sales"),
    "totals": ("totals", "summary", "totals section"),
    "customer_count": ("customers", "customer count", "total customers"),
    "additional_information": ("additional information", "additional info", "notes"),
}

FIELD_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "branch": ("branch", "shop", "location"),
    "report_date": ("date", "report date", "sales date"),
    "cashier": ("cashier", "served by"),
    "assistant": ("assistant", "assistant cashier"),
    "balanced_by": ("balanced by", "balance by"),
    "supervisor": ("supervisor", "manager"),
    "supervisor_confirmation": ("supervisor confirmation", "supervisor", "confirmed by"),
    "notes": ("notes", "comment", "remark"),
    "gross_sales": ("gross sales", "sales", "total sales"),
    "cash_sales": ("cash sales", "cash", "t/cash", "total cash"),
    "eftpos_sales": ("eftpos sales", "eftpos", "card sales", "t/card", "total card"),
    "mobile_money_sales": ("mobile money", "mobile sales"),
    "till_total": ("till total", "till", "cash in till"),
    "deposit_total": ("deposit total", "banking", "deposit"),
    "customer_count": ("customer count", "customers", "total customers", "guest/customer serve"),
    "traffic": ("traffic", "foot traffic", "store traffic", "main door"),
    "served": ("served", "customers served", "guest/customer serve"),
    "labor_hours": ("labor hours", "labour hours", "hours worked"),
    "z_reading": ("z/reading", "z reading"),
}

def canonical_section_name(value: str) -> str | None:
    """Return the canonical section name for a heading when recognized."""

    normalized = _normalize(value)
    for canonical_name, aliases in SECTION_ALIASES.items():
        if normalized in {_normalize(alias) for alias in aliases}:
            return canonical_name
    return None


def canonical_field_name(value: str) -> str | None:
    """Return the canonical field name for a wording variant when recognized."""

    normalized_internal = internal_field_name(value, report_family="sales")
    if normalized_internal is not None:
        return normalized_internal

    normalized = _normalize(value)
    for canonical_name, aliases in FIELD_ALIASES.items():
        if normalized in {_normalize(alias) for alias in aliases}:
            return canonical_name
    return None


def canonical_branch_slug(value: str) -> str:
    """Return a canonical branch slug from a free-form branch value."""

    return canonical_upstream_branch_slug(value)


def _normalize(value: str) -> str:
    """Normalize free-form keys for case-insensitive matching."""

    return " ".join(value.casefold().replace("_", " ").split())
