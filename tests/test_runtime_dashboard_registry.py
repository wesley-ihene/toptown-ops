"""Tests for metadata-only dashboard/API runtime ownership surfaces."""

from __future__ import annotations

import inspect
import json
from pathlib import Path

from analytics import phase4_portal
from packages.runtime_ownership import (
    DASHBOARD_SURFACES,
    DASHBOARD_SURFACE_REGISTRY,
    EXTERNAL_STRATEGIC_SURFACE,
    LIVE_RUNTIME,
    dashboard_surface,
)


def test_dashboard_surface_registry_matches_expected_official_surfaces() -> None:
    expected = {
        "taop_operational_dashboard": {
            "name": "TopTown Operational Dashboard",
            "route": "/dashboard",
            "route_prefix": None,
            "owner": ("analytics/phase4_portal.py",),
            "audience": ("operations",),
            "status": LIVE_RUNTIME,
            "purpose": (
                "branch/day operational analytics, sales, staffing, pending actions, "
                "operational KPIs"
            ),
            "observed_port": 8082,
            "intelligence_source": (),
            "owned_by_taop": True,
        },
        "taop_operational_api": {
            "name": "TopTown Operational API",
            "route": None,
            "route_prefix": "/api/",
            "owner": (
                "analytics/phase4_portal.py",
                "apps/dashboard_api/routes.py",
                "apps/taop_ops_api/routes.py",
            ),
            "audience": ("operations", "system"),
            "status": LIVE_RUNTIME,
            "purpose": "JSON analytics and operational action data for dashboards/tools",
            "observed_port": 8082,
            "intelligence_source": (),
            "owned_by_taop": True,
        },
        "ceo_briefing_dashboard": {
            "name": "CEO Briefing Dashboard",
            "route": "/ceo_briefing",
            "route_prefix": None,
            "owner": ("analytics/phase4_portal.py",),
            "audience": ("executive",),
            "status": LIVE_RUNTIME,
            "purpose": (
                "executive prioritization, branch ranking, intervention "
                "recommendations"
            ),
            "observed_port": None,
            "intelligence_source": (
                "apps/ceo_router/worker.py",
                "apps/executive_engine/worker.py",
            ),
            "owned_by_taop": True,
        },
        "ioi_colony_dashboard": {
            "name": "IOI Colony Dashboard",
            "route": "/",
            "route_prefix": None,
            "owner": (
                "external repo: ~/.openclaw/workspace/ioi-colony/scripts/colony_web.py",
            ),
            "audience": ("strategic intelligence",),
            "status": EXTERNAL_STRATEGIC_SURFACE,
            "purpose": (
                "opportunity intelligence, signal fusion, reinforcement/decay analysis"
            ),
            "observed_port": None,
            "intelligence_source": (),
            "owned_by_taop": False,
        },
    }

    actual = {
        service: {
            "name": surface.name,
            "route": surface.route,
            "route_prefix": surface.route_prefix,
            "owner": surface.owner,
            "audience": surface.audience,
            "status": surface.status,
            "purpose": surface.purpose,
            "observed_port": surface.observed_port,
            "intelligence_source": surface.intelligence_source,
            "owned_by_taop": surface.owned_by_taop,
        }
        for service, surface in DASHBOARD_SURFACE_REGISTRY.items()
    }

    assert actual == expected

    for surface in DASHBOARD_SURFACES:
        assert surface.service
        assert surface.route or surface.route_prefix
        assert surface.owner
        assert surface.audience
        assert surface.status
        assert surface.purpose


def test_no_duplicate_live_runtime_surface_exists_for_same_service_and_route() -> None:
    seen: set[tuple[str, str]] = set()

    for surface in DASHBOARD_SURFACES:
        if surface.status != LIVE_RUNTIME:
            continue
        signature = (surface.service, surface.routing_key)
        assert signature not in seen
        seen.add(signature)


def test_ioi_colony_surface_is_external_to_taop() -> None:
    surface = dashboard_surface("ioi_colony_dashboard")

    assert surface.status == EXTERNAL_STRATEGIC_SURFACE
    assert surface.owned_by_taop is False
    assert surface.owner == (
        "external repo: ~/.openclaw/workspace/ioi-colony/scripts/colony_web.py",
    )


def test_observed_port_is_metadata_only_not_routing_contract() -> None:
    dashboard_entry = dashboard_surface("taop_operational_dashboard")
    api_entry = dashboard_surface("taop_operational_api")

    dispatch_signature = inspect.signature(phase4_portal.dispatch_http_request)
    serve_signature = inspect.signature(phase4_portal.serve)

    assert dashboard_entry.observed_port == 8082
    assert api_entry.observed_port == 8082
    assert "port" not in dispatch_signature.parameters
    assert serve_signature.parameters["port"].default == phase4_portal.DEFAULT_PORT
    assert isinstance(phase4_portal.DEFAULT_PORT, int)
    assert phase4_portal.DEFAULT_PORT > 0


def test_dashboard_surface_registry_does_not_change_runtime_behavior(tmp_path: Path) -> None:
    _seed_operational_fixture(tmp_path)

    api_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/analytics/branches",
        root=tmp_path,
    )
    api_body = json.loads(api_response.body.decode("utf-8"))

    assert api_response.status_code == 200
    assert api_body["service"] == "phase4_dashboard_api"
    assert api_body["service"] != dashboard_surface("taop_operational_api").service

    dashboard_api_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/dashboard?branch=waigani&date=2026-04-07",
        root=tmp_path,
    )
    dashboard_api_body = json.loads(dashboard_api_response.body.decode("utf-8"))

    assert dashboard_api_response.status_code == 200
    assert dashboard_api_body["datasets"]["openclaw_runtime"]["openclaw"]["status"] == "disabled"

    dashboard_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/dashboard?branch=waigani&date=2026-04-07",
        root=tmp_path,
    )
    html = dashboard_response.body.decode("utf-8")

    assert dashboard_response.status_code == 200
    assert "TopTown Operational Dashboard" in html
    assert "OpenClaw Runtime" in html
    assert "Standby" in html
    assert dashboard_surface("taop_operational_dashboard").route == "/dashboard"


def _seed_operational_fixture(root: Path) -> None:
    _write_json(
        root / "analytics" / "branch_daily" / "waigani" / "2026-04-07.json",
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "gross_sales": 1200.0,
            "traffic": 12,
            "served": 9,
            "active_staff_count": 4,
            "sales_per_active_staff": 300.0,
            "items_per_active_staff": 5.0,
            "assists_per_active_staff": 2.0,
            "operational_flags": [],
            "warnings": [],
        },
    )
    _write_json(
        root / "analytics" / "branch_comparison" / "2026-04-07.json",
        {
            "report_date": "2026-04-07",
            "branch_scorecards": [
                {
                    "branch": "waigani",
                    "gross_sales": 1200.0,
                    "active_staff_count": 4,
                    "conversion_rate": 0.75,
                    "sales_per_active_staff": 300.0,
                    "staff_productivity_index": 18.0,
                    "operational_score": 90,
                    "warning_count": 0,
                    "flag_count": 0,
                }
            ],
        },
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
