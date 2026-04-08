"""Read-only Phase 4 dashboard and API server over analytics JSON outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from apps.ceo_api.routes import route_request as ceo_route_request
from apps.ceo_dashboard_ui.routes import render_ceo_dashboard_response
from apps.dashboard_api.routes import route_request as route_request
from apps.dashboard_ui.routes import render_dashboard_response, render_not_found_page
from analytics.phase5_executive import (
    build_ceo_dashboard,
)
from packages.common.analytics_loader import (
    build_catalog,
    canonical_branch_or_none,
    load_branch_analytics,
    load_branch_comparison,
)

SERVICE_NAME = "phase4_dashboard_api"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8010
HEALTH_ROUTE = "/health"
DASHBOARD_ROUTES = {"/", "/dashboard"}


@dataclass(slots=True)
class PortalHttpResponse:
    """Simple response envelope for testable request dispatch."""

    status_code: int
    body: bytes
    content_type: str = "application/json; charset=utf-8"


def dispatch_http_request(
    *,
    method: str,
    target: str,
    root: str | Path | None = None,
) -> PortalHttpResponse:
    """Dispatch one read-only dashboard or API request."""

    parsed = urlparse(target)
    path = parsed.path or "/"
    params = parse_qs(parsed.query, keep_blank_values=True)

    if method != "GET":
        return _json_response(
            HTTPStatus.METHOD_NOT_ALLOWED,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "method_not_allowed",
                "message": "Phase 4 portal is read-only and supports GET only.",
            },
        )

    if path == HEALTH_ROUTE:
        catalog = build_catalog(root=root)
        return _json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "service": SERVICE_NAME,
                "analytics_root": catalog["analytics_root"],
            },
        )

    result = ceo_route_request(path, params, root=str(root) if root is not None else None)
    if result is not None:
        status_code, payload = result
        return _json_response(status_code, payload)

    result = route_request(path, params, root=str(root) if root is not None else None)
    if result is not None:
        status_code, payload = result
        return _json_response(status_code, payload)

    if path == "/api/dashboard":
        return _dashboard_json_response(params=params, root=root)
    if path == "/api/ceo/dashboard":
        return _ceo_dashboard_json_response(params=params, root=root)
    if path in DASHBOARD_ROUTES:
        return _dashboard_response(path=path, params=params, root=root)
    if path in {"/ceo", "/ceo/dashboard"}:
        return _ceo_dashboard_response(path=path, params=params, root=root)

    if path.startswith("/api/"):
        return _json_response(
            HTTPStatus.NOT_FOUND,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "route_not_found",
                "message": f"Unsupported API route: {path}",
            },
        )
    return _html_response(HTTPStatus.NOT_FOUND, render_not_found_page(path=path, branch=None, report_date=None))


def serve(*, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, root: str | Path | None = None) -> ThreadingHTTPServer:
    """Create one live HTTP server for the Phase 4 portal."""

    class Phase4PortalHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            response = dispatch_http_request(method="GET", target=self.path, root=root)
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            self.end_headers()
            self.wfile.write(response.body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return ThreadingHTTPServer((host, port), Phase4PortalHandler)


def _dashboard_json_response(
    *,
    params: Mapping[str, list[str]],
    root: str | Path | None = None,
) -> PortalHttpResponse:
    selection = _resolve_selection(params=params, root=root)
    if selection is None:
        return _json_response(
            HTTPStatus.NOT_FOUND,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "analytics_not_found",
                "message": "No analytics output matched the requested filters.",
                "branch": canonical_branch_or_none(_query_value(params, "branch")),
                "report_date": _query_value(params, "date"),
            },
        )

    bundle = _load_bundle(branch=selection["branch"], report_date=selection["report_date"], root=root)
    if not any(bundle[key] is not None for key in ("staff", "branch_daily", "section", "branch_comparison")):
        return _json_response(
            HTTPStatus.NOT_FOUND,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "analytics_not_found",
                "message": "No analytics output matched the requested filters.",
                "branch": selection["branch"],
                "report_date": selection["report_date"],
            },
        )
    return _json_response(
        HTTPStatus.OK,
        {
            "ok": True,
            "service": SERVICE_NAME,
            "filters": selection,
            "catalog": build_catalog(root=root, branch=selection["branch"]),
            "datasets": bundle,
        },
    )


def _dashboard_response(
    *,
    path: str,
    params: Mapping[str, list[str]],
    root: str | Path | None = None,
) -> PortalHttpResponse:
    selection = _resolve_selection(params=params, root=root)
    if selection is None:
        return _html_response(
            HTTPStatus.NOT_FOUND,
            render_not_found_page(
                path=path,
                branch=canonical_branch_or_none(_query_value(params, "branch")),
                report_date=_query_value(params, "date"),
            ),
        )

    bundle = _load_bundle(branch=selection["branch"], report_date=selection["report_date"], root=root)
    if not any(bundle[key] is not None for key in ("staff", "branch_daily", "section", "branch_comparison")):
        return _html_response(
            HTTPStatus.NOT_FOUND,
            render_not_found_page(
                path=path,
                branch=selection["branch"],
                report_date=selection["report_date"],
            ),
        )

    warnings = []
    for key, label in (
        ("staff", "Staff Performance"),
        ("branch_daily", "Sales vs Staffing Efficiency"),
        ("section", "Section Productivity"),
        ("branch_comparison", "Branch Comparison"),
    ):
        if bundle.get(key) is None:
            warnings.append(f"{label} analytics are missing for the current selection.")

    html = render_dashboard_response(
        bundle=bundle,
        catalog=build_catalog(root=root, branch=selection["branch"]),
        selected_branch=selection["branch"],
        selected_date=selection["report_date"],
        warnings=warnings,
    )
    return _html_response(HTTPStatus.OK, html)


def _ceo_dashboard_json_response(
    *,
    params: Mapping[str, list[str]],
    root: str | Path | None = None,
) -> PortalHttpResponse:
    selection = _resolve_selection(params=params, root=root)
    if selection is None:
        return _json_response(
            HTTPStatus.NOT_FOUND,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "analytics_not_found",
                "message": "No analytics output matched the requested filters.",
                "branch": canonical_branch_or_none(_query_value(params, "branch")),
                "report_date": _query_value(params, "date"),
            },
        )

    dashboard, error = build_ceo_dashboard(selection["branch"], selection["report_date"], root=str(root) if root is not None else None)
    if dashboard is None:
        return _json_response(HTTPStatus.NOT_FOUND, {"ok": False, "service": SERVICE_NAME, **(error or {})})
    return _json_response(
        HTTPStatus.OK,
        {
            "ok": True,
            "service": SERVICE_NAME,
            "filters": selection,
            "dashboard": dashboard,
        },
    )


def _ceo_dashboard_response(
    *,
    path: str,
    params: Mapping[str, list[str]],
    root: str | Path | None = None,
) -> PortalHttpResponse:
    selection = _resolve_selection(params=params, root=root)
    if selection is None:
        return _html_response(
            HTTPStatus.NOT_FOUND,
            render_not_found_page(
                path=path,
                branch=canonical_branch_or_none(_query_value(params, "branch")),
                report_date=_query_value(params, "date"),
            ),
        )

    dashboard, error = build_ceo_dashboard(selection["branch"], selection["report_date"], root=str(root) if root is not None else None)
    if dashboard is None:
        return _html_response(
            HTTPStatus.NOT_FOUND,
            render_not_found_page(path=path, branch=selection["branch"], report_date=selection["report_date"]),
        )
    catalog = build_catalog(root=root, branch=selection["branch"])
    html = render_ceo_dashboard_response(
        dashboard=dashboard,
        selected_branch=selection["branch"],
        selected_date=selection["report_date"],
        available_branches=catalog["available_branches"],
        available_dates=catalog["available_dates"],
    )
    return _html_response(HTTPStatus.OK, html)


def _resolve_selection(
    *,
    params: Mapping[str, list[str]],
    root: str | Path | None = None,
) -> dict[str, str] | None:
    branch = canonical_branch_or_none(_query_value(params, "branch"))
    report_date = _query_value(params, "date")
    catalog = build_catalog(root=root, branch=branch)

    if branch is None:
        branches = catalog["available_branches"]
        branch = str(branches[0]["slug"]) if branches else None
    if branch is None:
        return None

    dates = catalog["available_dates"] if catalog["selected_branch"] == branch else build_catalog(root=root, branch=branch)["available_dates"]
    if report_date is None:
        report_date = dates[0] if dates else (catalog["available_comparison_dates"][0] if catalog["available_comparison_dates"] else None)
    if report_date is None:
        return None

    return {"branch": branch, "report_date": report_date}


def _load_bundle(
    *,
    branch: str,
    report_date: str,
    root: str | Path | None = None,
) -> dict[str, Any]:
    staff_payload, _ = load_branch_analytics("staff", branch=branch, report_date=report_date, root=root)
    branch_daily_payload, _ = load_branch_analytics("branch_daily", branch=branch, report_date=report_date, root=root)
    section_payload, _ = load_branch_analytics("section", branch=branch, report_date=report_date, root=root)
    comparison_payload, _ = load_branch_comparison(report_date=report_date, root=root)
    return {
        "branch": branch,
        "report_date": report_date,
        "staff": staff_payload,
        "branch_daily": branch_daily_payload,
        "section": section_payload,
        "branch_comparison": comparison_payload,
    }


def _query_value(params: Mapping[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    if not values:
        return None
    value = values[0].strip()
    return value or None


def _json_response(status_code: int, payload: Mapping[str, Any]) -> PortalHttpResponse:
    return PortalHttpResponse(
        status_code=status_code,
        body=json.dumps(dict(payload), indent=2, sort_keys=True).encode("utf-8"),
    )


def _html_response(status_code: int, body: str) -> PortalHttpResponse:
    return PortalHttpResponse(
        status_code=status_code,
        body=body.encode("utf-8"),
        content_type="text/html; charset=utf-8",
    )
