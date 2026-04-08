"""Focused tests for the Phase 4 analytics loader, API, and dashboard."""

from __future__ import annotations

import json
from pathlib import Path

from analytics import phase4_portal
from packages.common import analytics_loader


def test_analytics_loader_lists_branches_dates_and_loads_payloads(tmp_path: Path) -> None:
    _write_branch_daily(tmp_path, "waigani", "2026-04-07", gross_sales=1200.0)
    _write_staff_daily(tmp_path, "waigani", "2026-04-07")
    _write_section_daily(tmp_path, "lae_malaita", "2026-04-06")
    _write_branch_comparison(tmp_path, "2026-04-07", ["waigani", "lae_malaita"])

    branches = analytics_loader.list_available_branches(root=tmp_path)
    dates = analytics_loader.list_available_dates(branch="waigani", root=tmp_path)
    comparison_dates = analytics_loader.list_available_comparison_dates(root=tmp_path)
    payload, not_found = analytics_loader.load_branch_analytics(
        "branch_daily",
        branch="waigani",
        report_date="2026-04-07",
        root=tmp_path,
    )

    assert [entry["slug"] for entry in branches] == ["lae_malaita", "waigani"]
    assert dates == ["2026-04-07"]
    assert comparison_dates == ["2026-04-07"]
    assert payload is not None
    assert payload["gross_sales"] == 1200.0
    assert not_found is None


def test_api_routes_return_success_payloads_and_filters(tmp_path: Path) -> None:
    _write_branch_daily(tmp_path, "waigani", "2026-04-07", gross_sales=1200.0)
    _write_staff_daily(tmp_path, "waigani", "2026-04-07")
    _write_section_daily(tmp_path, "waigani", "2026-04-07")
    _write_branch_comparison(tmp_path, "2026-04-07", ["waigani", "lae_malaita"])

    routes = [
        ("/api/analytics/staff?branch=waigani&date=2026-04-07", "staff"),
        ("/api/analytics/branch_daily?branch=waigani&date=2026-04-07", "branch_daily"),
        ("/api/analytics/section?branch=waigani&date=2026-04-07", "section"),
        ("/api/analytics/branch_comparison?date=2026-04-07", "branch_comparison"),
    ]

    for target, product in routes:
        response = phase4_portal.dispatch_http_request(method="GET", target=target, root=tmp_path)
        body = json.loads(response.body.decode("utf-8"))
        assert response.status_code == 200
        assert body["ok"] is True
        assert body["product"] == product

    branches_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/analytics/branches",
        root=tmp_path,
    )
    branches_body = json.loads(branches_response.body.decode("utf-8"))
    assert branches_response.status_code == 200
    assert [entry["slug"] for entry in branches_body["branches"]] == ["waigani"]

    dates_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/analytics/dates?branch=waigani",
        root=tmp_path,
    )
    dates_body = json.loads(dates_response.body.decode("utf-8"))
    assert dates_response.status_code == 200
    assert dates_body["dates"] == ["2026-04-07"]


def test_api_404_responses_are_clean_and_traceable(tmp_path: Path) -> None:
    _write_branch_comparison(tmp_path, "2026-04-07", ["waigani"])

    response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/analytics/staff?branch=waigani&date=2026-04-07",
        root=tmp_path,
    )
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 404
    assert body["error"] == "analytics_not_found"
    assert body["branch"] == "waigani"
    assert body["report_date"] == "2026-04-07"
    assert body["expected_path"] == "analytics/staff_daily/waigani/2026-04-07.json"


def test_dashboard_renders_required_operational_views(tmp_path: Path) -> None:
    _write_branch_daily(tmp_path, "lae_malaita", "2026-04-07", gross_sales=2745.0)
    _write_staff_daily(tmp_path, "lae_malaita", "2026-04-07")
    _write_section_daily(tmp_path, "lae_malaita", "2026-04-07")
    _write_branch_comparison(tmp_path, "2026-04-07", ["waigani", "lae_malaita"])

    response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/dashboard?branch=lae_malaita&date=2026-04-07",
        root=tmp_path,
    )
    html = response.body.decode("utf-8")

    assert response.status_code == 200
    assert "Executive Overview" in html
    assert "Staff Performance View" in html
    assert "Sales vs Staffing Efficiency View" in html
    assert "Section Productivity View" in html
    assert "Branch Comparison View" in html
    assert "Top Branch by Ops Score" in html
    assert "Weakest Branch by Ops Score" in html
    assert "Top Items Moved" in html
    assert "Operational Flags" in html
    assert "Unresolved Sections Indicator" in html


def test_dashboard_branch_and_date_filters_handle_partial_and_missing_data(tmp_path: Path) -> None:
    _write_branch_daily(tmp_path, "waigani", "2026-04-07", gross_sales=1200.0)
    _write_branch_comparison(tmp_path, "2026-04-07", ["waigani"])

    partial_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/dashboard?branch=waigani&date=2026-04-07",
        root=tmp_path,
    )
    partial_html = partial_response.body.decode("utf-8")

    assert partial_response.status_code == 200
    assert "Staff Performance analytics are missing for the current selection." in partial_html
    assert "Section Productivity analytics are missing for the current selection." in partial_html

    missing_response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/dashboard?branch=bena_road&date=2026-04-09",
        root=tmp_path,
    )
    missing_html = missing_response.body.decode("utf-8")

    assert missing_response.status_code == 404
    assert "Analytics Not Found" in missing_html
    assert "branch=bena_road, date=2026-04-09" in missing_html


def _write_branch_daily(root: Path, branch: str, report_date: str, *, gross_sales: float) -> None:
    _write_json(
        root / "analytics" / "branch_daily" / branch / f"{report_date}.json",
        {
            "branch": branch,
            "report_date": report_date,
            "gross_sales": gross_sales,
            "traffic": 12,
            "served": 9,
            "labor_hours": 4.0,
            "active_staff_count": 4,
            "sales_per_active_staff": 300.0,
            "items_per_active_staff": 5.0,
            "assists_per_active_staff": 2.0,
            "operational_flags": [{"code": "flag_a", "severity": "warning", "message": "Review branch."}],
            "warnings": [],
        },
    )


def _write_staff_daily(root: Path, branch: str, report_date: str) -> None:
    _write_json(
        root / "analytics" / "staff_daily" / branch / f"{report_date}.json",
        {
            "branch": branch,
            "report_date": report_date,
            "summary_counts": {
                "total_staff_count": 10,
                "active_staff_count": 6,
                "total_items_moved": 42,
                "total_assisting_count": 15,
            },
            "top_items_moved": [{"staff_name": "Alice Demo", "items_moved": 15, "section": "mens_tshirt"}],
            "top_assisting": [{"staff_name": "Bob Demo", "assisting_count": 9, "section": "ladies_jeans"}],
            "top_activity_score": [{"staff_name": "Cara Demo", "activity_score": 19.0, "section": "shoe_shop"}],
            "lowest_productivity": [{"staff_name": "Dan Demo", "activity_score": 0.0, "duty_status": "on_duty"}],
            "role_summaries": [{"role": "Cashier", "staff_count": 2, "avg_activity_score": 10.0}],
            "duty_status_summaries": [{"duty_status": "on_duty", "staff_count": 6, "total_items_moved": 42}],
        },
    )


def _write_section_daily(root: Path, branch: str, report_date: str) -> None:
    _write_json(
        root / "analytics" / "section_daily" / branch / f"{report_date}.json",
        {
            "branch": branch,
            "report_date": report_date,
            "sections": [
                {"section": "mens_tshirt", "productivity_index": 12.4, "staff_count": 2, "items_moved": 9},
            ],
            "unresolved_section_tracking": {
                "count": 1,
                "examples": ["Unknown Rack"],
            },
        },
    )


def _write_branch_comparison(root: Path, report_date: str, branches: list[str]) -> None:
    scorecards = []
    ranked_sales = []
    ranked_conversion = []
    ranked_productivity = []
    ranked_ops = []
    ordered = list(branches)
    for index, branch in enumerate(ordered, start=1):
        sales = 2745.0 if branch == "lae_malaita" else 1200.0
        ops = 90 if branch == "waigani" else 60
        conversion = 0.75 if branch == "waigani" else 0.0
        productivity = 26.67 if branch == "lae_malaita" else 11.0
        scorecards.append(
            {
                "branch": branch,
                "gross_sales": sales,
                "operational_score": ops,
                "conversion_rate": conversion,
                "staff_productivity_index": productivity,
            }
        )
        ranked_sales.append({"rank": index, "branch": branch, "gross_sales": sales})
        ranked_conversion.append({"rank": index, "branch": branch, "conversion_rate": conversion})
        ranked_productivity.append({"rank": index, "branch": branch, "staff_productivity_index": productivity})
        ranked_ops.append({"rank": index, "branch": branch, "operational_score": ops})

    _write_json(
        root / "analytics" / "branch_comparison" / f"{report_date}.json",
        {
            "report_date": report_date,
            "branch_scorecards": scorecards,
            "ranked_branches_by_sales": ranked_sales,
            "ranked_branches_by_conversion": ranked_conversion,
            "ranked_branches_by_staff_productivity": ranked_productivity,
            "ranked_branches_by_operational_score": ranked_ops,
        },
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
