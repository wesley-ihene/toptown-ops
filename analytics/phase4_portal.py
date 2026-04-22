"""Read-only operator portal and deprecated executive compatibility server.

Wave 2 keeps operator routes active in TopTown Ops while isolating deprecated
CEO/executive surfaces behind compatibility routing only.
"""

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
from packages.feedback_store import build_action_feedback_state
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
OPERATOR_DASHBOARD_ROUTES = {"/", "/dashboard"}
DEPRECATED_EXECUTIVE_DASHBOARD_ROUTES = {"/ceo", "/ceo/dashboard"}
DEPRECATED_EXECUTIVE_API_PREFIXES = ("/api/ceo", "/api/executive")
COMPATIBILITY_QUERY_PARAM = "compat"


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
    compatibility_mode = _compatibility_mode_requested(params) or _compatibility_mode_requested_from_query(parsed.query)

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

    if _is_hidden_deprecated_surface(path) and not compatibility_mode:
        return _deprecated_surface_hidden_response(path=path)

    result = route_request(path, params, root=str(root) if root is not None else None)
    if result is not None:
        status_code, payload = result
        return _json_response(status_code, payload)

    result = ceo_route_request(path, params, root=str(root) if root is not None else None)
    if result is not None:
        status_code, payload = result
        return _json_response(status_code, payload)

    if path == "/api/dashboard":
        return _dashboard_json_response(params=params, root=root)
    if path == "/api/ceo/dashboard":
        return _ceo_dashboard_json_response(params=params, root=root)
    if path in OPERATOR_DASHBOARD_ROUTES:
        return _dashboard_response(path=path, params=params, root=root)
    if path in DEPRECATED_EXECUTIVE_DASHBOARD_ROUTES:
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
        "staff_daily": staff_payload,
        "branch_daily": branch_daily_payload,
        "section": section_payload,
        "section_daily": section_payload,
        "branch_comparison": comparison_payload,
        "operator_action_state": build_action_feedback_state(
            report_date,
            branch=branch,
            output_root=root,
        ),
    }


def _query_value(params: Mapping[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    if not values:
        return None
    value = values[0].strip()
    return value or None


def _compatibility_mode_requested(params: Mapping[str, list[str]]) -> bool:
    requested = _query_value(params, COMPATIBILITY_QUERY_PARAM)
    if requested is None:
        return False
    return requested.lower() in {"1", "true", "yes", "on"}


def _compatibility_mode_requested_from_query(query: str) -> bool:
    for token in query.split("&"):
        key, _, value = token.partition("=")
        if key != COMPATIBILITY_QUERY_PARAM:
            continue
        if value.lower() in {"1", "true", "yes", "on"}:
            return True
    return False


def _is_hidden_deprecated_surface(path: str) -> bool:
    if path in DEPRECATED_EXECUTIVE_DASHBOARD_ROUTES:
        return True
    if path.startswith(DEPRECATED_EXECUTIVE_API_PREFIXES):
        return True
    return False


def _deprecated_surface_hidden_response(*, path: str) -> PortalHttpResponse:
    payload = {
        "ok": False,
        "service": SERVICE_NAME,
        "error": "deprecated_surface_hidden",
        "message": (
            "Deprecated CEO/executive surfaces are hidden from normal TopTown Ops "
            f"exposure. Add `{COMPATIBILITY_QUERY_PARAM}=1` only for rollback or "
            "automation compatibility."
        ),
        "path": path,
        "compatibility_query": f"{COMPATIBILITY_QUERY_PARAM}=1",
        "operator_routes": ["/dashboard", "/api/analytics/*", "/api/dashboard"],
    }
    if path.startswith("/api/"):
        return _json_response(HTTPStatus.NOT_FOUND, payload)
    return _html_response(
        HTTPStatus.NOT_FOUND,
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Deprecated Surface Hidden</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: linear-gradient(155deg, #eff6ff, #f8fafc);
      color: #0f172a;
    }}
    article {{
      max-width: 720px;
      background: rgba(255, 255, 255, 0.96);
      border-radius: 20px;
      padding: 28px;
      box-shadow: 0 20px 48px rgba(15, 23, 42, 0.12);
      border: 1px solid rgba(15, 23, 42, 0.08);
    }}
    code {{ font-family: "IBM Plex Mono", monospace; }}
    a {{ color: #0f766e; font-weight: 700; }}
  </style>
</head>
<body>
  <article>
    <h1>Deprecated Surface Hidden</h1>
    <p>CEO/executive compatibility routes are no longer part of normal TopTown Ops product exposure.</p>
    <p><strong>Route:</strong> {path}</p>
    <p>Use <code>{COMPATIBILITY_QUERY_PARAM}=1</code> only for rollback or automation compatibility.</p>
    <p><a href="/dashboard">Back to dashboard</a></p>
  </article>
</body>
</html>""",
    )


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
