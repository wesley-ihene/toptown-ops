"""Phase 1 vital-few CEO dashboard contract tests."""

from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path

from analytics.ceo_vital_few import build_phase1_dashboard
from analytics.phase4_portal import dispatch_http_request


def test_phase1_contract_has_trends_baselines_thresholds_and_completeness(tmp_path: Path) -> None:
    _seed_history(tmp_path)

    payload = build_phase1_dashboard("2026-03-04", branch="waigani", root=tmp_path)
    waigani = next(scope for scope in payload["branches"] if scope["scope_id"] == "waigani")
    by_id = {kpi["id"]: kpi for kpi in waigani["kpis"]}

    assert payload["contract_version"] == "ceo-kpi.v1"
    assert payload["presentation_only"] is True
    assert payload["writes_back"] is False
    assert len(by_id) == 7
    assert by_id["sales_per_labor_hour"]["value"] == 70.0
    assert by_id["sales_per_labor_hour"]["target"] == {
        "type": "trailing_8_week_median",
        "value": 100.0,
        "sample_count": 56,
        "expected_sample_count": 56,
    }
    assert by_id["sales_per_labor_hour"]["threshold"]["state"] == "breach"
    assert by_id["conversion_rate"]["threshold"]["state"] == "healthy"
    assert by_id["attendance_rate"]["value"] == 0.8
    assert by_id["attendance_rate"]["threshold"]["boundary"] == 0.9
    assert by_id["stock_release_to_sales"]["cadence"] == "weekly"
    assert by_id["stock_release_to_sales"]["threshold"]["state"] == "breach"
    assert by_id["staff_productivity"]["data_completeness"]["status"] == "complete"
    assert by_id["attendance_rate"]["source"]["records"]
    assert waigani["exception_count"] == 6
    assert payload["top_priority"]["advisory_only"] is True
    assert payload["gated_domains"][0]["status"] == "blocked"
    assert "revenue is not profit" in payload["gated_domains"][0]["reason"].lower()


def test_active_vital_few_routes_are_read_only_and_exception_first(tmp_path: Path) -> None:
    _seed_history(tmp_path)

    api = dispatch_http_request(
        method="GET",
        target="/api/ceo/vital-few?date=2026-03-04&branch=waigani",
        root=tmp_path,
    )
    api_body = json.loads(api.body)
    assert api.status_code == 200
    assert api_body["service"] == "ceo_vital_few_api"
    assert api_body["payload"]["selected_scope"] == "waigani"
    assert "deprecated" not in api_body

    default_page = dispatch_http_request(
        method="GET",
        target="/ceo/vital-few?date=2026-03-04&branch=waigani",
        root=tmp_path,
    )
    default_html = default_page.body.decode("utf-8")
    assert default_page.status_code == 200
    assert "Today’s single priority" in default_html
    assert default_html.count('class="kpi ') == 6
    assert "<h3>Conversion rate</h3>" not in default_html
    assert "Target / baseline" in default_html
    assert "Threshold" in default_html
    assert "Source" in default_html
    assert "Cadence" in default_html
    assert "Revenue is not profit" in default_html

    full_page = dispatch_http_request(
        method="GET",
        target="/ceo/vital-few?date=2026-03-04&branch=waigani&show=all",
        root=tmp_path,
    )
    full_html = full_page.body.decode("utf-8")
    assert full_page.status_code == 200
    assert full_html.count('class="kpi ') == 7
    assert "<h3>Conversion rate</h3>" in full_html

    rejected_write = dispatch_http_request(
        method="POST",
        target="/api/ceo/vital-few?date=2026-03-04",
        root=tmp_path,
    )
    assert rejected_write.status_code == 405


def test_existing_deprecated_ceo_routes_remain_hidden(tmp_path: Path) -> None:
    _seed_history(tmp_path)

    response = dispatch_http_request(
        method="GET",
        target="/api/ceo/overview?date=2026-03-04",
        root=tmp_path,
    )
    assert response.status_code == 404
    assert json.loads(response.body)["error"] == "deprecated_surface_hidden"


def _seed_history(root: Path) -> None:
    selected = date(2026, 3, 4)
    for offset in range(62, -1, -1):
        report_date = selected - timedelta(days=offset)
        is_current = report_date == selected
        sales = 700.0 if is_current else 1000.0
        labor_hours = 10.0
        traffic = 100
        served = 80
        active_staff = 10
        moved = 40 if is_current else 80
        assists = 10 if is_current else 20
        released = 7000.0 if is_current else 50.0
        attendance = (["present"] * 8 + ["absent"] * 2) if is_current else (["present"] * 19 + ["leave"])
        iso_date = report_date.isoformat()

        _write_json(
            root / "analytics" / "branch_daily" / "waigani" / f"{iso_date}.json",
            {
                "branch": "waigani",
                "report_date": iso_date,
                "gross_sales": sales,
                "labor_hours": labor_hours,
                "traffic": traffic,
                "served": served,
                "active_staff_count": active_staff,
                "total_items_moved": moved,
                "total_assisting_count": assists,
                "warnings": [],
                "source_records": {
                    "sales_income": f"records/structured/sales_income/waigani/{iso_date}.json",
                    "hr_performance": f"records/structured/hr_performance/waigani/{iso_date}.json",
                },
            },
        )
        _write_json(
            root / "records" / "structured" / "hr_attendance" / "waigani" / f"{iso_date}.json",
            {
                "branch": "waigani",
                "report_date": iso_date,
                "status": "accepted",
                "items": [
                    {"staff_name": f"Staff {index}", "status": status}
                    for index, status in enumerate(attendance, start=1)
                ],
                "warnings": [],
            },
        )
        _write_json(
            root / "records" / "structured" / "pricing_stock_release" / "waigani" / f"{iso_date}.json",
            {
                "branch_slug": "waigani",
                "report_date": iso_date,
                "status": "accepted",
                "metrics": {"total_amount": released},
                "warnings": [],
            },
        )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
