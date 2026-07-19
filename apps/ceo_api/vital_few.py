"""Read-only API route for the active Phase 1 CEO KPI contract."""

from __future__ import annotations

from collections.abc import Mapping
from http import HTTPStatus
from typing import Any

from analytics.ceo_vital_few import build_phase1_dashboard

ROUTE = "/api/ceo/vital-few"
SERVICE_NAME = "ceo_vital_few_api"


def route_request(
    path: str,
    params: Mapping[str, list[str]],
    *,
    root: str | None = None,
) -> tuple[int, dict[str, Any]] | None:
    """Return the factual KPI presentation contract for one reporting date."""

    if path != ROUTE:
        return None
    report_date = _query_value(params, "date")
    if report_date is None:
        return HTTPStatus.BAD_REQUEST, {
            "ok": False,
            "service": SERVICE_NAME,
            "error": "missing_filters",
            "message": "`date` query parameter is required.",
        }
    try:
        payload = build_phase1_dashboard(
            report_date,
            branch=_query_value(params, "branch"),
            root=root,
        )
    except ValueError as error:
        return HTTPStatus.BAD_REQUEST, {
            "ok": False,
            "service": SERVICE_NAME,
            "error": "invalid_filters",
            "message": str(error),
        }
    return HTTPStatus.OK, {
        "ok": True,
        "service": SERVICE_NAME,
        "product": "phase1_vital_few",
        "payload": payload,
    }


def _query_value(params: Mapping[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    if not values:
        return None
    value = values[0].strip()
    return value or None


__all__ = ["ROUTE", "route_request"]
