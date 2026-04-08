"""Route handlers for the Phase 4 read-only analytics API."""

from __future__ import annotations

from collections.abc import Mapping
from http import HTTPStatus
from typing import Any

from packages.common.analytics_loader import (
    build_catalog,
    load_branch_analytics,
    load_branch_comparison,
    list_available_branches,
    list_available_dates,
)

SERVICE_NAME = "phase4_dashboard_api"


def route_request(path: str, params: Mapping[str, list[str]], *, root: str | None = None) -> tuple[int, dict[str, Any]] | None:
    """Dispatch one analytics API route and return `(status_code, payload)`."""

    if path == "/api/analytics/branches":
        return HTTPStatus.OK, {
            "ok": True,
            "service": SERVICE_NAME,
            "branches": list_available_branches(root=root),
        }
    if path == "/api/analytics/dates":
        branch = _query_value(params, "branch")
        return HTTPStatus.OK, {
            "ok": True,
            "service": SERVICE_NAME,
            "branch": branch,
            "dates": list_available_dates(branch=branch, root=root),
        }
    if path == "/api/analytics/staff":
        return _route_branch_product("staff", params=params, root=root)
    if path == "/api/analytics/branch_daily":
        return _route_branch_product("branch_daily", params=params, root=root)
    if path == "/api/analytics/section":
        return _route_branch_product("section", params=params, root=root)
    if path == "/api/analytics/branch_comparison":
        return _route_branch_comparison(params=params, root=root)
    if path == "/api/analytics/catalog":
        return HTTPStatus.OK, {
            "ok": True,
            "service": SERVICE_NAME,
            "catalog": build_catalog(root=root, branch=_query_value(params, "branch")),
        }
    return None


def _route_branch_product(
    product: str,
    *,
    params: Mapping[str, list[str]],
    root: str | None = None,
) -> tuple[int, dict[str, Any]]:
    branch = _query_value(params, "branch")
    report_date = _query_value(params, "date")
    if not branch or not report_date:
        return HTTPStatus.BAD_REQUEST, {
            "ok": False,
            "service": SERVICE_NAME,
            "error": "missing_filters",
            "message": "`branch` and `date` query parameters are required.",
            "product": product,
        }

    payload, not_found = load_branch_analytics(product, branch=branch, report_date=report_date, root=root)
    if payload is None:
        assert not_found is not None
        return HTTPStatus.NOT_FOUND, {
            "ok": False,
            "service": SERVICE_NAME,
            **not_found.as_dict(),
        }
    return HTTPStatus.OK, {
        "ok": True,
        "service": SERVICE_NAME,
        "product": product,
        "branch": payload.get("branch"),
        "report_date": payload.get("report_date"),
        "payload": payload,
    }


def _route_branch_comparison(
    *,
    params: Mapping[str, list[str]],
    root: str | None = None,
) -> tuple[int, dict[str, Any]]:
    report_date = _query_value(params, "date")
    if not report_date:
        return HTTPStatus.BAD_REQUEST, {
            "ok": False,
            "service": SERVICE_NAME,
            "error": "missing_filters",
            "message": "`date` query parameter is required.",
            "product": "branch_comparison",
        }

    payload, not_found = load_branch_comparison(report_date=report_date, root=root)
    if payload is None:
        assert not_found is not None
        return HTTPStatus.NOT_FOUND, {
            "ok": False,
            "service": SERVICE_NAME,
            **not_found.as_dict(),
        }
    return HTTPStatus.OK, {
        "ok": True,
        "service": SERVICE_NAME,
        "product": "branch_comparison",
        "report_date": payload.get("report_date"),
        "payload": payload,
    }


def _query_value(params: Mapping[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    if not values:
        return None
    value = values[0].strip()
    return value or None
