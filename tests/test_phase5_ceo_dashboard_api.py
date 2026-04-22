"""Focused tests for the Phase 5 CEO dashboard and executive API."""

from __future__ import annotations

import json
from pathlib import Path

from analytics import phase4_portal
from analytics.phase5_executive import (
    build_ceo_alerts,
    build_ceo_branches,
    build_ceo_overview,
    build_ceo_sections,
    build_ceo_staff,
)
from packages.common.executive_alerts import (
    build_executive_alert_branch,
    build_executive_alert_summary,
    get_executive_alert_branch_path,
    get_executive_alert_summary_path,
    get_executive_alert_summary_whatsapp_path,
    write_executive_alert_artifacts,
)


def test_ceo_overview_aggregates_branch_totals_and_rankings(tmp_path: Path) -> None:
    _write_branch_daily(
        tmp_path,
        "waigani",
        "2026-04-07",
        gross_sales=1200.0,
        traffic=15,
        served=12,
        active_staff_count=4,
        sales_per_active_staff=300.0,
        conversion_rate=0.8,
        sources={"sales_income": True, "hr_performance": True},
        traceability={"sales_status": "accepted", "staff_status": "accepted"},
    )
    _write_staff_daily(
        tmp_path,
        "waigani",
        "2026-04-07",
        activity_rows=[
            {"staff_name": "Cara Demo", "activity_score": 19.0, "items_moved": 12, "assisting_count": 4, "duty_status": "on_duty", "section": "shoe_shop"},
            {"staff_name": "Dan Demo", "activity_score": 0.0, "items_moved": 0, "assisting_count": 0, "duty_status": "on_duty", "section": "mens_tshirt"},
        ],
    )
    _write_section_daily(
        tmp_path,
        "waigani",
        "2026-04-07",
        sections=[{"section": "shoe_shop", "productivity_index": 18.0, "staff_count": 2, "items_moved": 12, "assisting_count": 6}],
        unresolved_count=0,
    )

    _write_branch_daily(
        tmp_path,
        "lae_malaita",
        "2026-04-07",
        gross_sales=2745.0,
        traffic=30,
        served=9,
        active_staff_count=12,
        sales_per_active_staff=228.75,
        conversion_rate=0.3,
        sources={"sales_income": True, "hr_performance": True},
        traceability={"sales_status": "accepted_with_warning", "staff_status": "needs_review"},
    )
    _write_staff_daily(
        tmp_path,
        "lae_malaita",
        "2026-04-07",
        activity_rows=[
            {"staff_name": "Alice Demo", "activity_score": 22.0, "items_moved": 17, "assisting_count": 8, "duty_status": "on_duty", "section": "ladies_jeans"},
            {"staff_name": "Bob Demo", "activity_score": 0.0, "items_moved": 0, "assisting_count": 0, "duty_status": "on_duty", "section": None},
        ],
    )
    _write_section_daily(
        tmp_path,
        "lae_malaita",
        "2026-04-07",
        sections=[{"section": "ladies_jeans", "productivity_index": 9.0, "staff_count": 3, "items_moved": 8, "assisting_count": 3}],
        unresolved_count=2,
    )

    _write_branch_comparison(
        tmp_path,
        "2026-04-07",
        [
            {
                "branch": "waigani",
                "gross_sales": 1200.0,
                "active_staff_count": 4,
                "conversion_rate": 0.8,
                "sales_per_active_staff": 300.0,
                "staff_productivity_index": 18.0,
                "operational_score": 90,
                "warning_count": 1,
                "flag_count": 1,
            },
            {
                "branch": "lae_malaita",
                "gross_sales": 2745.0,
                "active_staff_count": 12,
                "conversion_rate": 0.3,
                "sales_per_active_staff": 228.75,
                "staff_productivity_index": 12.0,
                "operational_score": 60,
                "warning_count": 4,
                "flag_count": 4,
            },
        ],
    )

    payload, error = build_ceo_overview("2026-04-07", root=str(tmp_path))

    assert error is None
    assert payload is not None
    assert payload["total_gross_sales"] == 3945.0
    assert payload["total_active_staff"] == 16
    assert payload["total_traffic"] == 45
    assert payload["total_served"] == 21
    assert payload["branches_reporting_count"] == 2
    assert payload["top_branch_by_operational_score"]["branch"] == "waigani"
    assert payload["weakest_branch_by_operational_score"]["branch"] == "lae_malaita"
    assert payload["top_branch_by_sales"]["branch"] == "lae_malaita"
    assert payload["top_branch_by_conversion"]["branch"] == "waigani"
    assert payload["summary_warning_counts"]["critical_alert_count"] >= 0
    assert payload["summary_warning_counts"]["warning_alert_count"] >= 5


def test_ceo_branches_staff_and_sections_build_cross_branch_summaries(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    branches_payload, branches_error = build_ceo_branches("2026-04-07", root=str(tmp_path))
    staff_payload, staff_error = build_ceo_staff("2026-04-07", root=str(tmp_path))
    sections_payload, sections_error = build_ceo_sections("2026-04-07", root=str(tmp_path))

    assert branches_error is None
    assert staff_error is None
    assert sections_error is None
    assert branches_payload is not None
    assert staff_payload is not None
    assert sections_payload is not None
    assert branches_payload["branches"][0]["sales_per_active_staff"] is not None
    assert branches_payload["branches"][0]["data_completeness"]["branch_daily_present"] is True
    assert staff_payload["best_staff"]["staff_name"] == "Alice Demo"
    assert staff_payload["top_items_staff"][0]["staff_name"] == "Alice Demo"
    assert staff_payload["top_assisting_staff"][0]["staff_name"] == "Alice Demo"
    assert any(row["staff_name"] == "Bob Demo" for row in staff_payload["idle_on_duty_staff"])
    assert sections_payload["strongest_section"]["section"] == "shoe_shop"
    assert sections_payload["weakest_section"]["section"] == "ladies_jeans"
    assert sections_payload["unresolved_section_hotspots"][0]["branch"] == "lae_malaita"


def test_ceo_alerts_are_deterministic_and_traceable(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    payload, error = build_ceo_alerts("2026-04-07", root=str(tmp_path))

    assert error is None
    assert payload is not None
    codes = {(item["branch"], item["code"]) for item in payload["alerts"]}
    assert ("lae_malaita", "record_needs_review") in codes
    assert ("lae_malaita", "record_accepted_with_warning") in codes
    assert ("lae_malaita", "low_conversion_rate") in codes
    assert ("lae_malaita", "low_sales_per_active_staff") in codes
    assert ("lae_malaita", "idle_on_duty_staff") in codes
    assert ("lae_malaita", "unresolved_sections_present") in codes
    assert ("lae_malaita", "high_warning_density") in codes
    assert payload["count_by_severity"]["warning"] >= 6
    first_alert = payload["alerts"][0]
    assert first_alert["traceability"]["report_date"] == "2026-04-07"
    assert first_alert["traceability"]["source_records"]["branch_comparison"] == "analytics/branch_comparison/2026-04-07.json"


def test_ceo_alert_artifacts_write_json_and_whatsapp_outputs(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    payload, error = build_executive_alert_summary("2026-04-07", root=str(tmp_path))

    assert error is None
    assert payload is not None

    artifact_paths = write_executive_alert_artifacts("2026-04-07", output_root=tmp_path, overwrite=True)
    json_path = Path(artifact_paths["summary_path"])
    whatsapp_path = Path(artifact_paths["summary_whatsapp_path"])
    branch_path = Path(artifact_paths["branch_paths"]["lae_malaita"])

    assert json_path == get_executive_alert_summary_path("2026-04-07", output_root=tmp_path)
    assert whatsapp_path == get_executive_alert_summary_whatsapp_path("2026-04-07", output_root=tmp_path)
    assert branch_path == get_executive_alert_branch_path("2026-04-07", "lae_malaita", output_root=tmp_path)
    written_json = json.loads(json_path.read_text(encoding="utf-8"))
    assert written_json["report_date"] == "2026-04-07"
    assert written_json["counts_by_severity"]["warning"] >= 5
    written_branch = json.loads(branch_path.read_text(encoding="utf-8"))
    codes = {row["alert_code"] for row in written_branch["alerts"]}
    assert "low_conversion_rate" in codes
    assert "idle_on_duty_staff" in codes
    assert "unresolved_sections_present" in codes
    assert "weak_branch_operational_score" in codes
    assert "critical_branch_gap" in codes
    written_whatsapp = whatsapp_path.read_text(encoding="utf-8")
    assert "TOPTOWN EXECUTIVE ALERTS 2026-04-07" in written_whatsapp
    assert "[WARNING] Lae Malaita" in written_whatsapp


def test_ceo_api_handles_success_and_missing_data_cleanly(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    routes = [
        "/api/ceo/overview?date=2026-04-07&compat=1",
        "/api/ceo/branches?date=2026-04-07&compat=1",
        "/api/ceo/staff?date=2026-04-07&compat=1",
        "/api/ceo/sections?date=2026-04-07&compat=1",
        "/api/ceo/alerts?date=2026-04-07&compat=1",
        "/api/ceo/alerts/feed?date=2026-04-07&compat=1",
        "/api/ceo/alerts/branch?branch=lae_malaita&date=2026-04-07&compat=1",
        "/api/ceo/catalog?compat=1",
        "/api/ceo/dashboard?branch=lae_malaita&date=2026-04-07&compat=1",
    ]

    for target in routes:
        response = phase4_portal.dispatch_http_request(method="GET", target=target, root=tmp_path)
        body = json.loads(response.body.decode("utf-8"))
        assert response.status_code == 200
        assert body["ok"] is True
        assert body["deprecated"] is True
        assert body["operator_routes"] == ["/dashboard", "/api/analytics/*", "/api/dashboard"]

    missing = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/ceo/overview?date=2026-04-08&compat=1",
        root=tmp_path,
    )
    missing_body = json.loads(missing.body.decode("utf-8"))

    assert missing.status_code == 404
    assert missing_body["error"] == "analytics_not_found"
    assert missing_body["expected_path"] == "analytics/branch_comparison/2026-04-08.json"


def test_ceo_alert_routes_prefer_file_backed_artifacts_when_present(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)
    payload, error = build_executive_alert_summary("2026-04-07", root=str(tmp_path))

    assert error is None
    assert payload is not None
    write_executive_alert_artifacts("2026-04-07", output_root=tmp_path, overwrite=True)

    alerts_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/ceo/alerts?date=2026-04-07&compat=1",
        root=tmp_path,
    )
    alerts_body = json.loads(alerts_response.body.decode("utf-8"))
    assert alerts_response.status_code == 200
    assert alerts_body["payload"]["report_date"] == "2026-04-07"
    assert alerts_body["payload"]["alerts"][0]["report_date"] == "2026-04-07"

    feed_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/ceo/alerts/feed?date=2026-04-07&compat=1",
        root=tmp_path,
    )
    feed_body = json.loads(feed_response.body.decode("utf-8"))
    assert feed_response.status_code == 200
    assert feed_body["product"] == "alerts_feed"
    assert feed_body["payload"]["format"] == "whatsapp"
    assert feed_body["payload"]["artifact_path"] == "alerts/executive/2026-04-07/summary.whatsapp.txt"
    assert "TOPTOWN EXECUTIVE ALERTS 2026-04-07" in feed_body["payload"]["message"]

    branch_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/ceo/alerts/branch?branch=lae_malaita&date=2026-04-07&compat=1",
        root=tmp_path,
    )
    branch_body = json.loads(branch_response.body.decode("utf-8"))
    assert branch_response.status_code == 200
    assert branch_body["product"] == "alerts_branch"
    assert branch_body["payload"]["branch"] == "lae_malaita"
    assert any(row["alert_code"] == "critical_branch_gap" for row in branch_body["payload"]["alerts"])


def test_executive_alert_summary_and_branch_builders_cover_required_alert_classes(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    summary_payload, summary_error = build_executive_alert_summary("2026-04-07", root=str(tmp_path))
    branch_payload, branch_error = build_executive_alert_branch("2026-04-07", "lae_malaita", root=str(tmp_path))

    assert summary_error is None
    assert branch_error is None
    assert summary_payload is not None
    assert branch_payload is not None
    codes = {row["alert_code"] for row in branch_payload["alerts"]}
    assert "low_conversion_rate" in codes
    assert "idle_on_duty_staff" in codes
    assert "record_needs_review" in codes
    assert "record_accepted_with_warning" in codes
    assert "low_sales_per_active_staff" in codes
    assert "unresolved_sections_present" in codes
    assert "weak_branch_operational_score" in codes
    assert "critical_branch_gap" in codes
    assert any(row["dedupe_key"].startswith("2026-04-07|lae_malaita|") for row in branch_payload["alerts"])


def test_executive_alert_builders_emit_missing_input_alerts(tmp_path: Path) -> None:
    _write_branch_daily(
        tmp_path,
        "waigani",
        "2026-04-07",
        gross_sales=None,
        traffic=0,
        served=0,
        active_staff_count=None,
        sales_per_active_staff=None,
        conversion_rate=None,
        sources={"sales_income": False, "hr_performance": False},
        traceability={"sales_status": "needs_review", "staff_status": "needs_review"},
    )
    _write_branch_comparison(
        tmp_path,
        "2026-04-07",
        [
            {
                "branch": "waigani",
                "gross_sales": None,
                "active_staff_count": None,
                "conversion_rate": None,
                "sales_per_active_staff": None,
                "staff_productivity_index": None,
                "operational_score": 40,
                "warning_count": 5,
                "flag_count": 5,
            },
        ],
    )

    branch_payload, error = build_executive_alert_branch("2026-04-07", "waigani", root=str(tmp_path))

    assert error is None
    assert branch_payload is not None
    codes = {row["alert_code"] for row in branch_payload["alerts"]}
    assert "branch_missing_sales_input" in codes
    assert "branch_missing_staff_input" in codes
    assert "record_needs_review" in codes


def test_ceo_routes_are_hidden_from_normal_product_exposure(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    for target in (
        "/api/ceo/overview?date=2026-04-07",
        "/api/executive/overview?date=2026-04-07",
    ):
        response = phase4_portal.dispatch_http_request(method="GET", target=target, root=tmp_path)
        body = json.loads(response.body.decode("utf-8"))

        assert response.status_code == 404
        assert body["ok"] is False
        assert body["service"] == "phase4_dashboard_api"
        assert body["error"] == "deprecated_surface_hidden"
        assert body["compatibility_query"] == "compat=1"


def test_ceo_routes_remain_available_in_explicit_compatibility_mode(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    for target in (
        "/api/ceo/overview?date=2026-04-07&compat=1",
        "/api/executive/overview?date=2026-04-07&compat=1",
    ):
        response = phase4_portal.dispatch_http_request(method="GET", target=target, root=tmp_path)
        body = json.loads(response.body.decode("utf-8"))

        assert response.status_code == 200
        assert body["ok"] is True
        assert body["service"] == "phase5_ceo_api"
        assert body["deprecated"] is True


def test_ceo_dashboard_is_hidden_without_compatibility_mode(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/ceo?branch=lae_malaita&date=2026-04-07",
        root=tmp_path,
    )
    html = response.body.decode("utf-8")

    assert response.status_code == 404
    assert "Deprecated Surface Hidden" in html
    assert "compat=1" in html


def test_ceo_dashboard_renders_executive_control_sections_in_compatibility_mode(tmp_path: Path) -> None:
    _seed_ceo_fixture(tmp_path)

    response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/ceo?branch=lae_malaita&date=2026-04-07&compat=1",
        root=tmp_path,
    )
    html = response.body.decode("utf-8")

    assert response.status_code == 200
    assert "TopTown Deprecated Executive Compatibility View" in html
    assert "Deprecation Notice" in html
    assert "Compatibility Summary Cards" in html
    assert "Branch Compatibility View" in html
    assert "Staff Compatibility View" in html
    assert "Section Compatibility View" in html
    assert "Deprecated Alerts Panel" in html
    assert "operator dashboard at" in html
    assert "/api/ceo/overview?date=2026-04-07" not in html
    assert "lae_malaita" in html
    assert 'name="compat" value="1"' in html


def _seed_ceo_fixture(root: Path) -> None:
    _write_branch_daily(
        root,
        "waigani",
        "2026-04-07",
        gross_sales=1200.0,
        traffic=15,
        served=12,
        active_staff_count=4,
        sales_per_active_staff=300.0,
        conversion_rate=0.8,
        sources={"sales_income": True, "hr_performance": True},
        traceability={"sales_status": "accepted", "staff_status": "accepted"},
    )
    _write_staff_daily(
        root,
        "waigani",
        "2026-04-07",
        activity_rows=[
            {"staff_name": "Cara Demo", "activity_score": 19.0, "items_moved": 12, "assisting_count": 4, "duty_status": "on_duty", "section": "shoe_shop"},
            {"staff_name": "Dan Demo", "activity_score": 0.0, "items_moved": 0, "assisting_count": 0, "duty_status": "on_duty", "section": "mens_tshirt"},
        ],
    )
    _write_section_daily(
        root,
        "waigani",
        "2026-04-07",
        sections=[{"section": "shoe_shop", "productivity_index": 18.0, "staff_count": 2, "items_moved": 12, "assisting_count": 6}],
        unresolved_count=0,
    )

    _write_branch_daily(
        root,
        "lae_malaita",
        "2026-04-07",
        gross_sales=2745.0,
        traffic=30,
        served=9,
        active_staff_count=12,
        sales_per_active_staff=228.75,
        conversion_rate=0.3,
        sources={"sales_income": True, "hr_performance": True},
        traceability={"sales_status": "accepted_with_warning", "staff_status": "needs_review"},
    )
    _write_staff_daily(
        root,
        "lae_malaita",
        "2026-04-07",
        activity_rows=[
            {"staff_name": "Alice Demo", "activity_score": 22.0, "items_moved": 17, "assisting_count": 8, "duty_status": "on_duty", "section": "ladies_jeans"},
            {"staff_name": "Bob Demo", "activity_score": 0.0, "items_moved": 0, "assisting_count": 0, "duty_status": "on_duty", "section": None},
        ],
    )
    _write_section_daily(
        root,
        "lae_malaita",
        "2026-04-07",
        sections=[{"section": "ladies_jeans", "productivity_index": 9.0, "staff_count": 3, "items_moved": 8, "assisting_count": 3}],
        unresolved_count=2,
    )

    _write_branch_comparison(
        root,
        "2026-04-07",
        [
            {
                "branch": "waigani",
                "gross_sales": 1200.0,
                "active_staff_count": 4,
                "conversion_rate": 0.8,
                "sales_per_active_staff": 300.0,
                "staff_productivity_index": 18.0,
                "operational_score": 90,
                "warning_count": 1,
                "flag_count": 1,
            },
            {
                "branch": "lae_malaita",
                "gross_sales": 2745.0,
                "active_staff_count": 12,
                "conversion_rate": 0.3,
                "sales_per_active_staff": 228.75,
                "staff_productivity_index": 12.0,
                "operational_score": 60,
                "warning_count": 4,
                "flag_count": 4,
            },
        ],
    )


def _write_branch_daily(
    root: Path,
    branch: str,
    report_date: str,
    *,
    gross_sales: float | None,
    traffic: int,
    served: int,
    active_staff_count: int | None,
    sales_per_active_staff: float | None,
    conversion_rate: float | None,
    sources: dict[str, bool],
    traceability: dict[str, str],
) -> None:
    _write_json(
        root / "analytics" / "branch_daily" / branch / f"{report_date}.json",
        {
            "branch": branch,
            "report_date": report_date,
            "gross_sales": gross_sales,
            "traffic": traffic,
            "served": served,
            "labor_hours": 8.0,
            "active_staff_count": active_staff_count,
            "sales_per_active_staff": sales_per_active_staff,
            "items_per_active_staff": 5.0,
            "assists_per_active_staff": 2.0,
            "conversion_rate": conversion_rate,
            "operational_flags": [],
            "warnings": [],
            "sources": sources,
            "traceability": traceability,
            "source_records": {
                "sales_income": f"records/structured/sales_income/{branch}/{report_date}.json",
                "hr_performance": f"records/structured/hr_performance/{branch}/{report_date}.json",
            },
        },
    )


def _write_staff_daily(root: Path, branch: str, report_date: str, *, activity_rows: list[dict[str, object]]) -> None:
    top_items = sorted(activity_rows, key=lambda row: (float(row.get("items_moved") or 0), str(row.get("staff_name") or "")), reverse=True)
    top_assists = sorted(activity_rows, key=lambda row: (float(row.get("assisting_count") or 0), str(row.get("staff_name") or "")), reverse=True)
    lowest = sorted(activity_rows, key=lambda row: (float(row.get("activity_score") or 0), str(row.get("staff_name") or "")))
    _write_json(
        root / "analytics" / "staff_daily" / branch / f"{report_date}.json",
        {
            "branch": branch,
            "report_date": report_date,
            "summary_counts": {
                "total_staff_count": len(activity_rows),
                "active_staff_count": sum(1 for row in activity_rows if row.get("duty_status") == "on_duty"),
                "total_items_moved": sum(int(row.get("items_moved") or 0) for row in activity_rows),
                "total_assisting_count": sum(int(row.get("assisting_count") or 0) for row in activity_rows),
            },
            "top_items_moved": top_items[:5],
            "top_assisting": top_assists[:5],
            "top_activity_score": activity_rows,
            "lowest_productivity": lowest[:5],
            "role_summaries": [{"role": "Floor", "staff_count": len(activity_rows), "avg_activity_score": 8.0}],
            "duty_status_summaries": [{"duty_status": "on_duty", "staff_count": sum(1 for row in activity_rows if row.get("duty_status") == "on_duty"), "total_items_moved": sum(int(row.get("items_moved") or 0) for row in activity_rows)}],
            "source_records": {
                "hr_performance": f"records/structured/hr_performance/{branch}/{report_date}.json",
            },
        },
    )


def _write_section_daily(
    root: Path,
    branch: str,
    report_date: str,
    *,
    sections: list[dict[str, object]],
    unresolved_count: int,
) -> None:
    _write_json(
        root / "analytics" / "section_daily" / branch / f"{report_date}.json",
        {
            "branch": branch,
            "report_date": report_date,
            "sections": sections,
            "unresolved_section_tracking": {
                "count": unresolved_count,
                "examples": ["Unknown Rack"] if unresolved_count else [],
            },
            "source_records": {
                "hr_performance": f"records/structured/hr_performance/{branch}/{report_date}.json",
            },
        },
    )


def _write_branch_comparison(root: Path, report_date: str, rows: list[dict[str, object]]) -> None:
    ranked_sales = sorted(rows, key=lambda row: float(row.get("gross_sales") or 0), reverse=True)
    ranked_conversion = sorted(rows, key=lambda row: float(row.get("conversion_rate") or 0), reverse=True)
    ranked_productivity = sorted(rows, key=lambda row: float(row.get("staff_productivity_index") or 0), reverse=True)
    ranked_ops = sorted(rows, key=lambda row: float(row.get("operational_score") or 0), reverse=True)
    _write_json(
        root / "analytics" / "branch_comparison" / f"{report_date}.json",
        {
            "report_date": report_date,
            "branch_scorecards": rows,
            "ranked_branches_by_sales": _rank(ranked_sales, "gross_sales"),
            "ranked_branches_by_conversion": _rank(ranked_conversion, "conversion_rate"),
            "ranked_branches_by_staff_productivity": _rank(ranked_productivity, "staff_productivity_index"),
            "ranked_branches_by_operational_score": _rank(ranked_ops, "operational_score"),
            "warnings": [],
        },
    )


def _rank(rows: list[dict[str, object]], metric: str) -> list[dict[str, object]]:
    return [
        {
            "rank": index,
            "branch": row["branch"],
            metric: row[metric],
        }
        for index, row in enumerate(rows, start=1)
    ]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
