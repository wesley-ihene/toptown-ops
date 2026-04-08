"""Read-only Phase 5 CEO API routes."""

from __future__ import annotations

from collections.abc import Mapping
from http import HTTPStatus
from typing import Any

from analytics.phase5_executive import (
    build_ceo_branches,
    build_ceo_catalog,
    build_ceo_dashboard,
    build_ceo_overview,
    build_ceo_sections,
    build_ceo_staff,
)
from packages.common.executive_alerts import (
    build_executive_alert_branch,
    build_executive_alert_summary,
    load_executive_alert_branch,
    load_executive_alert_feed,
    load_executive_alert_summary,
)

SERVICE_NAME = "phase5_ceo_api"


def route_request(path: str, params: Mapping[str, list[str]], *, root: str | None = None) -> tuple[int, dict[str, Any]] | None:
    """Dispatch Phase 5 CEO API routes and keep lightweight aliases."""

    if path in {"/api/ceo/overview", "/api/executive/overview"}:
        report_date = _query_value(params, "date")
        if not report_date:
            return _bad_request("overview", needs_branch=False)
        payload, error = build_ceo_overview(report_date, root=root)
        return _result(payload, error, product="overview")

    if path in {"/api/ceo/branches", "/api/executive/branches"}:
        report_date = _query_value(params, "date")
        if not report_date:
            return _bad_request("branches", needs_branch=False)
        payload, error = build_ceo_branches(report_date, root=root)
        return _result(payload, error, product="branches")

    if path in {"/api/ceo/staff", "/api/executive/staff"}:
        report_date = _query_value(params, "date")
        if not report_date:
            return _bad_request("staff", needs_branch=False)
        payload, error = build_ceo_staff(report_date, root=root)
        return _result(payload, error, product="staff")

    if path in {"/api/ceo/sections", "/api/executive/sections", "/api/executive/section"}:
        report_date = _query_value(params, "date")
        if not report_date:
            return _bad_request("sections", needs_branch=False)
        payload, error = build_ceo_sections(report_date, root=root)
        return _result(payload, error, product="sections")

    if path in {"/api/ceo/alerts", "/api/executive/alerts"}:
        report_date = _query_value(params, "date")
        if not report_date:
            return _bad_request("alerts", needs_branch=False)
        payload, error = load_executive_alert_summary(report_date, root=root)
        if payload is None:
            payload, error = build_executive_alert_summary(report_date, root=root)
        return _result(payload, error, product="alerts")

    if path in {
        "/api/ceo/alerts/feed",
        "/api/executive/alerts/feed",
        "/api/ceo/alerts/whatsapp",
        "/api/executive/alerts/whatsapp",
        "/api/ceo/alerts_whatsapp",
        "/api/executive/alerts_whatsapp",
    }:
        report_date = _query_value(params, "date")
        if not report_date:
            return _bad_request("alerts_feed", needs_branch=False)
        payload, error = load_executive_alert_feed(report_date, root=root)
        if payload is None:
            summary_payload, error = build_executive_alert_summary(report_date, root=root)
            if summary_payload is None:
                return _result(None, error, product="alerts_feed")
            from packages.common.whatsapp_alert_formatter import format_executive_alert_summary_whatsapp

            payload = {
                "report_date": report_date,
                "format": "whatsapp",
                "artifact_path": None,
                "message": format_executive_alert_summary_whatsapp(summary_payload),
            }
        return _result(payload, error, product="alerts_feed")

    if path in {"/api/ceo/alerts/branch", "/api/executive/alerts/branch"}:
        report_date = _query_value(params, "date")
        branch = _query_value(params, "branch")
        if not report_date or not branch:
            return _bad_request("alerts_branch", needs_branch=True)
        payload, error = load_executive_alert_branch(report_date, branch, root=root)
        if payload is None:
            payload, error = build_executive_alert_branch(report_date, branch, root=root)
        return _result(payload, error, product="alerts_branch")

    if path in {"/api/ceo/catalog", "/api/executive/catalog"}:
        return HTTPStatus.OK, {
            "ok": True,
            "service": SERVICE_NAME,
            "product": "catalog",
            "payload": build_ceo_catalog(root=root),
        }

    if path == "/api/ceo/dashboard":
        report_date = _query_value(params, "date")
        if not report_date:
            return _bad_request("dashboard", needs_branch=False)
        payload, error = build_ceo_dashboard(_query_value(params, "branch"), report_date, root=root)
        return _result(payload, error, product="dashboard")

    if path in {"/api/executive/branch", "/api/executive/section"}:
        report_date = _query_value(params, "date")
        branch = _query_value(params, "branch")
        if not report_date or not branch:
            return _bad_request("dashboard", needs_branch=True)
        payload, error = build_ceo_dashboard(branch, report_date, root=root)
        return _result(payload, error, product="dashboard")

    return None


def _result(payload: dict[str, Any] | None, error: dict[str, Any] | None, *, product: str) -> tuple[int, dict[str, Any]]:
    if payload is None:
        assert error is not None
        return HTTPStatus.NOT_FOUND, {"ok": False, "service": SERVICE_NAME, **error}
    return HTTPStatus.OK, {"ok": True, "service": SERVICE_NAME, "product": product, "payload": payload}


def _bad_request(product: str, *, needs_branch: bool) -> tuple[int, dict[str, Any]]:
    message = "`date` query parameter is required." if not needs_branch else "`branch` and `date` query parameters are required."
    return HTTPStatus.BAD_REQUEST, {
        "ok": False,
        "service": SERVICE_NAME,
        "error": "missing_filters",
        "message": message,
        "product": product,
    }


def _query_value(params: Mapping[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    if not values:
        return None
    value = values[0].strip()
    return value or None
