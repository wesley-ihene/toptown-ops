"""Conservative routing helpers for Orchestra work items."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

from packages.sop_validation.router import validate_report
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
    "rejection_feedback_agent",
    "quarantine",
    "splitter_required",
]

RouteStatus = Literal["routed", "quarantine", "requires_split", "rejected", "needs_split"]

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
    if report_type == "mixed":
        return _route_to_feedback_agent(
            work_item,
            report_type=report_type,
            route_status="needs_split",
            route_reason="mixed_reports_require_split_feedback",
            rejections=[
                {
                    "code": "needs_split",
                    "message": "Mixed reports must be split before specialist routing.",
                }
            ],
        )

    if report_type == "unknown":
        return _route_to_feedback_agent(
            work_item,
            report_type=report_type,
            route_status="rejected",
            route_reason="unknown_reports_require_rejection_feedback",
            rejections=[
                {
                    "code": "unknown_report_type",
                    "message": "The report type could not be identified safely.",
                }
            ],
        )

    destination, route_status, route_reason = ROUTE_MAP[report_type]
    validation_payload = _validation_payload_from_work_item(work_item)
    validation_result = None
    if validation_payload is not None and destination in {"income_agent", "hr_agent", "pricing_agent"}:
        validation_result = validate_report(report_type, validation_payload)
        work_item.payload["validation"] = validation_result.to_payload()
        if not validation_result.accepted:
            return _route_to_feedback_agent(
                work_item,
                report_type=report_type,
                route_status="rejected",
                route_reason="sop_validation_rejected_report",
                rejections=[rejection.to_payload() for rejection in validation_result.rejections],
            )

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


def _validation_payload_from_work_item(work_item: WorkItem) -> Mapping[str, Any] | None:
    """Return a report payload suitable for SOP validation when available."""

    normalized_report = work_item.payload.get("normalized_report")
    if isinstance(normalized_report, Mapping):
        return normalized_report

    raw_message = work_item.payload.get("raw_message")
    if isinstance(raw_message, Mapping) and any(
        field_name in raw_message
        for field_name in ("branch", "report_date", "metrics", "items")
    ):
        return raw_message
    return None


def _route_to_feedback_agent(
    work_item: WorkItem,
    *,
    report_type: str,
    route_status: Literal["rejected", "needs_split"],
    route_reason: str,
    rejections: list[dict[str, str]],
) -> RoutingResult:
    """Return a routing result that annotates one work item for feedback generation."""

    work_item.payload["routing"] = {
        "destination": "rejection_feedback_agent",
        "route_status": route_status,
        "route_reason": route_reason,
    }
    work_item.payload["rejection_feedback"] = {
        "report_type": report_type,
        "branch": _string_or_none(_report_field(work_item, "branch")),
        "report_date": _string_or_none(_report_field(work_item, "report_date")),
        "channel": "whatsapp",
        "dry_run": True,
        "rejections": rejections,
        "source_record_path": _source_record_path(work_item),
        "source_message_hash": _string_or_none(work_item.payload.get("message_hash")),
    }
    return RoutingResult(
        work_item=work_item,
        destination="rejection_feedback_agent",
        route_status=route_status,
        route_reason=route_reason,
    )


def _report_field(work_item: WorkItem, field_name: str) -> Any:
    """Return a report field from the best available validation payload."""

    validation_payload = _validation_payload_from_work_item(work_item)
    if validation_payload is None:
        return None
    return validation_payload.get(field_name)


def _source_record_path(work_item: WorkItem) -> str | None:
    """Return the best available source record path when present."""

    raw_record = work_item.payload.get("raw_record")
    if isinstance(raw_record, Mapping):
        for field_name in ("text_path", "raw_txt_path", "path"):
            value = raw_record.get(field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()

    replay = work_item.payload.get("replay")
    if isinstance(replay, Mapping):
        value = replay.get("original_path")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _string_or_none(value: Any) -> str | None:
    """Return a stripped string value or None."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
