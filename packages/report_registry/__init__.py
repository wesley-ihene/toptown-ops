"""Deterministic report-family registry for intake routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

ReportFamily = str

UNKNOWN_REPORT_FAMILY: Final[str] = "unknown"

FAMILY_TO_TARGET_AGENT: Final[dict[str, str]] = {
    "sales_income": "sales_income_agent",
    "pricing_stock_release": "pricing_stock_release_agent",
    "attendance": "hr_agent",
    "staff_performance": "hr_agent",
    "supervisor_control": "supervisor_control_agent",
}

FAMILY_TO_SPECIALIST_TYPE: Final[dict[str, str]] = {
    "sales_income": "sales",
    "pricing_stock_release": "bale_summary",
    "attendance": "staff_attendance",
    "staff_performance": "staff_performance",
    "supervisor_control": "supervisor_control",
}

FAMILY_TO_STORAGE_BUCKET: Final[dict[str, str]] = {
    "sales_income": "sales",
    "pricing_stock_release": "bale_release",
    "attendance": "hr_attendance",
    "staff_performance": "hr_performance",
    "staff_sales": "unknown",
    "supervisor_control": "supervisor_control",
    UNKNOWN_REPORT_FAMILY: "unknown",
}

FAMILY_BOUNDARY_HEADERS: Final[dict[str, tuple[str, ...]]] = {
    "sales_income": (
        "day end sales report",
        "sales income report",
        "sales report",
    ),
    "staff_performance": (
        "staff performance report",
        "staff assisting customers",
        "staff assisting report",
    ),
    "staff_sales": (
        "staff sales report",
        "sales by staff",
        "staff sales",
    ),
    "attendance": (
        "staff attendance report",
        "staff attendance",
        "attendance report",
    ),
    "pricing_stock_release": (
        "daily bale summary",
        "daily bale summary released to rail",
        "released to rail",
        "pricing stock release",
        "stock release",
    ),
    "supervisor_control": (
        "supervisor control report",
        "supervisor control",
        "supervisor report",
        "supervisor checklist",
        "control report",
    ),
}

NONCRITICAL_TRAILING_NOTE_HEADERS: Final[tuple[str, ...]] = (
    "notes",
    "note",
    "operational notes",
    "operations note",
    "remarks",
    "remark",
    "staff room note",
    "staff room notes",
    "extra note",
    "additional note",
)


@dataclass(slots=True)
class FamilyRoute:
    """Final deterministic routing lookup for a report family."""

    family: str
    target_agent: str | None
    specialist_type: str | None
    storage_bucket: str


def route_for_family(family: str) -> FamilyRoute:
    """Return the canonical routing config for one family."""

    return FamilyRoute(
        family=family,
        target_agent=FAMILY_TO_TARGET_AGENT.get(family),
        specialist_type=FAMILY_TO_SPECIALIST_TYPE.get(family),
        storage_bucket=FAMILY_TO_STORAGE_BUCKET.get(family, "unknown"),
    )
