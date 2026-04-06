"""Conservative routing helpers for Orchestra work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

from packages.signal_contracts.work_item import WorkItem

ROUTER_STAGE: Final[str] = "router"

ReportType = Literal[
    "sales",
    "staff_attendance",
    "bale_summary",
    "supervisor_control",
    "mixed",
    "unknown",
]

RouteDestination = Literal[
    "income_agent",
    "hr_agent",
    "pricing_agent",
    "quarantine",
    "splitter_required",
]

RouteStatus = Literal["routed", "quarantine", "requires_split"]

ROUTE_MAP: Final[dict[ReportType, tuple[RouteDestination, RouteStatus, str]]] = {
    "sales": ("income_agent", "routed", "sales_reports_route_to_income_agent"),
    "staff_attendance": ("hr_agent", "routed", "attendance_reports_route_to_hr_agent"),
    "bale_summary": ("pricing_agent", "routed", "bale_reports_route_to_pricing_agent"),
    "supervisor_control": (
        "quarantine",
        "quarantine",
        "supervisor_control_routes_to_quarantine",
    ),
    "mixed": ("splitter_required", "requires_split", "mixed_reports_require_splitter"),
    "unknown": ("quarantine", "quarantine", "unknown_reports_route_to_quarantine"),
}


@dataclass(slots=True)
class RoutingResult:
    """Typed routing decision for a work item."""

    work_item: WorkItem
    destination: RouteDestination
    route_status: RouteStatus
    route_reason: str


def route_work_item(work_item: WorkItem) -> RoutingResult:
    """Attach a conservative routing decision to a work item."""

    report_type = _report_type_from_work_item(work_item)
    destination, route_status, route_reason = ROUTE_MAP[report_type]

    routing_payload = {
        "destination": destination,
        "route_status": route_status,
        "route_reason": route_reason,
    }
    work_item.payload["routing"] = routing_payload

    return RoutingResult(
        work_item=work_item,
        destination=destination,
        route_status=route_status,
        route_reason=route_reason,
    )


def _report_type_from_work_item(work_item: WorkItem) -> ReportType:
    """Return a supported report type from payload classification."""

    classification = work_item.payload.get("classification", {})
    if not isinstance(classification, Mapping):
        return "unknown"

    report_type = classification.get("report_type")
    if not isinstance(report_type, str):
        return "unknown"
    if report_type in ROUTE_MAP:
        return report_type
    return "unknown"
