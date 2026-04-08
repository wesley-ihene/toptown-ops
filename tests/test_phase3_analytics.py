"""Tests for Phase 3 analytics builders and outputs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from apps.branch_daily_analytics_agent.worker import process_work_item as process_branch_daily_work_item
from apps.branch_comparison_agent.worker import process_work_item as process_branch_comparison_work_item
from apps.section_productivity_agent.worker import process_work_item as process_section_productivity_work_item
from apps.staff_leaderboard_agent.worker import process_work_item as process_staff_leaderboard_work_item
from analytics.phase3 import (
    build_branch_comparison,
    build_branch_daily_analytics,
    build_section_productivity,
    build_staff_leaderboard,
    get_branch_comparison_path,
    get_branch_daily_analytics_path,
    get_section_productivity_path,
    get_staff_leaderboard_path,
    write_branch_comparison_json,
)
from packages.signal_contracts.work_item import WorkItem
from scripts.build_branch_daily_analytics import main as build_branch_daily_main
from scripts.build_branch_comparison import main as build_branch_comparison_main
from scripts.build_section_productivity import main as build_section_productivity_main
from scripts.build_staff_leaderboard import main as build_staff_leaderboard_main


def test_branch_daily_analytics_computes_sales_and_staff_efficiency(tmp_path: Path) -> None:
    _write_sales_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        gross_sales=1200.0,
        traffic=12,
        served=9,
        sales_per_labor_hour=300.0,
        status="needs_review",
        warnings=[{"code": "cash_variance_present", "severity": "warning", "message": "Variance present."}],
    )
    _write_staff_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        items=[
            _staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2, role="Cashier"),
            _staff_item("Beth Demo", "off_duty", "ladies_tshirt", "Ladies Tshirt", items=0, assists=0),
            _staff_item("Chris Demo", "sick", None, "Unknown Rail", items=0, assists=0),
        ],
        total_items_moved=10,
        total_assisting_count=2,
        unresolved_section_count=1,
        status="accepted_with_warning",
    )

    payload = build_branch_daily_analytics("waigani", "2026-04-07", root=tmp_path)

    assert payload["gross_sales"] == 1200.0
    assert payload["traffic"] == 12
    assert payload["served"] == 9
    assert payload["labor_hours"] == 4.0
    assert payload["active_staff_count"] == 1
    assert payload["off_count"] == 1
    assert payload["sick_count"] == 1
    assert payload["total_items_moved"] == 10
    assert payload["total_assisting_count"] == 2
    assert payload["sales_per_active_staff"] == 1200.0
    assert payload["items_per_active_staff"] == 10.0
    assert payload["assists_per_active_staff"] == 2.0
    assert payload["sales_per_labor_hour"] == 300.0
    assert payload["conversion_rate"] == 0.75
    assert {flag["code"] for flag in payload["operational_flags"]} >= {
        "sales_status_review",
        "staff_status_review",
        "unresolved_sections_present",
    }


def test_staff_leaderboard_ranks_items_assists_and_low_productivity(tmp_path: Path) -> None:
    _write_staff_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        items=[
            _staff_item("Cara Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=20, assists=1, role="Cashier"),
            _staff_item("Alice Demo", "on_duty", "ladies_tshirt", "Ladies Tshirt", items=10, assists=5, role="Cashier"),
            _staff_item("Bob Demo", "on_duty", "mens_shorts", "Men's Shorts", items=5, assists=0),
            _staff_item("Off Demo", "off_duty", "mens_shorts", "Men's Shorts", items=0, assists=0),
        ],
        total_items_moved=35,
        total_assisting_count=6,
    )

    payload = build_staff_leaderboard("waigani", "2026-04-07", root=tmp_path)

    assert payload["summary_counts"] == {
        "total_staff_count": 4,
        "active_staff_count": 3,
        "off_count": 1,
        "sick_count": 0,
        "unknown_duty_status_count": 0,
        "total_items_moved": 35,
        "total_assisting_count": 6,
        "unresolved_section_count": 0,
    }
    assert payload["top_items_moved"][0]["staff_name"] == "Cara Demo"
    assert payload["top_items_moved"][1]["staff_name"] == "Alice Demo"
    assert payload["top_assisting"][0]["staff_name"] == "Alice Demo"
    assert payload["top_activity_score"][0]["staff_name"] == "Cara Demo"
    assert payload["lowest_productivity"][0]["staff_name"] == "Bob Demo"
    assert payload["role_summaries"][0]["role"] == "Cashier"
    assert payload["role_summaries"][0]["staff_count"] == 2
    assert payload["duty_status_summaries"][0]["duty_status"] == "off_duty"
    assert payload["duty_status_summaries"][1]["duty_status"] == "on_duty"


def test_section_productivity_groups_sections_and_tracks_unresolved(tmp_path: Path) -> None:
    _write_staff_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        items=[
            _staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2),
            _staff_item("Beth Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=4, assists=4),
            _staff_item("Chris Demo", "on_duty", None, "Unknown Rail", items=1, assists=0),
        ],
        total_items_moved=15,
        total_assisting_count=6,
        unresolved_section_count=1,
        unresolved_examples=["Unknown Rail"],
    )

    payload = build_section_productivity("waigani", "2026-04-07", root=tmp_path)

    assert payload["sections"][0]["section"] == "mens_tshirt"
    assert payload["sections"][0]["staff_count"] == 2
    assert payload["sections"][0]["items_moved"] == 14
    assert payload["sections"][0]["assisting_count"] == 6
    assert payload["sections"][0]["avg_activity_score"] == 8.5
    assert payload["sections"][0]["productivity_index"] == 10.8
    assert payload["unresolved_section_tracking"]["count"] == 1
    assert payload["unresolved_section_tracking"]["examples"] == ["Unknown Rail"]


def test_branch_comparison_ranks_branches_and_writes_output(tmp_path: Path) -> None:
    _write_sales_record(tmp_path, "waigani", "2026-04-07", gross_sales=1200.0, traffic=12, served=9, sales_per_labor_hour=300.0)
    _write_staff_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        items=[
            _staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2),
            _staff_item("Beth Demo", "on_duty", "ladies_tshirt", "Ladies Tshirt", items=8, assists=1),
        ],
        total_items_moved=18,
        total_assisting_count=3,
    )
    _write_sales_record(tmp_path, "bena_road", "2026-04-07", gross_sales=800.0, traffic=20, served=8, sales_per_labor_hour=200.0)
    _write_staff_record(
        tmp_path,
        "bena_road",
        "2026-04-07",
        items=[
            _staff_item("Cara Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=3, assists=1),
            _staff_item("Dan Demo", "off_duty", "ladies_tshirt", "Ladies Tshirt", items=0, assists=0),
        ],
        total_items_moved=3,
        total_assisting_count=1,
        unresolved_section_count=1,
        status="accepted_with_warning",
    )

    payload = build_branch_comparison("2026-04-07", root=tmp_path)

    assert payload["ranked_branches_by_sales"][0]["branch"] == "waigani"
    assert payload["ranked_branches_by_staff_productivity"][0]["branch"] == "waigani"
    assert payload["ranked_branches_by_conversion"][0]["branch"] == "waigani"
    assert payload["ranked_branches_by_operational_score"][0]["branch"] == "waigani"

    path = write_branch_comparison_json(payload, output_root=tmp_path)
    assert path == get_branch_comparison_path("2026-04-07", output_root=tmp_path)
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["branch_scorecards"][0]["branch"] == "bena_road"
    assert written["branch_scorecards"][1]["branch"] == "waigani"


def test_branch_daily_analytics_tolerates_missing_staff_record(tmp_path: Path) -> None:
    _write_sales_record(tmp_path, "waigani", "2026-04-07", gross_sales=500.0, traffic=10, served=4, sales_per_labor_hour=250.0)

    payload = build_branch_daily_analytics("waigani", "2026-04-07", root=tmp_path)

    assert payload["active_staff_count"] is None
    assert payload["total_items_moved"] is None
    assert any(warning["code"] == "missing_staff_record" for warning in payload["warnings"])


def test_branch_comparison_warns_when_explicit_branch_is_missing_inputs(tmp_path: Path) -> None:
    _write_sales_record(tmp_path, "waigani", "2026-04-07", gross_sales=500.0, traffic=10, served=4, sales_per_labor_hour=250.0)

    payload = build_branch_comparison("2026-04-07", root=tmp_path, branches=["waigani", "bena_road"])

    assert payload["ranked_branches_by_sales"][0]["branch"] == "waigani"
    assert payload["ranked_branches_by_sales"][1]["branch"] == "bena_road"
    assert any(
        warning["code"] == "branch_missing_inputs" and "bena_road" in warning["message"]
        for warning in payload["warnings"]
    )


def test_branch_comparison_worker_requires_explicit_overwrite_flag(tmp_path: Path) -> None:
    _write_sales_record(tmp_path, "waigani", "2026-04-07", gross_sales=500.0, traffic=10, served=4, sales_per_labor_hour=250.0)

    first = process_branch_comparison_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"report_date": "2026-04-07", "root": str(tmp_path)},
        )
    )
    assert first.payload["status"] == "written"
    assert Path(first.payload["output_path"]) == get_branch_comparison_path("2026-04-07", output_root=tmp_path)

    with pytest.raises(FileExistsError):
        process_branch_comparison_work_item(
            WorkItem(
                kind="analytics_request",
                payload={"report_date": "2026-04-07", "root": str(tmp_path)},
            )
        )

    second = process_branch_comparison_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"report_date": "2026-04-07", "root": str(tmp_path), "overwrite": True},
        )
    )
    assert second.payload["status"] == "written"


def test_branch_comparison_cli_requires_explicit_overwrite_flag(tmp_path: Path) -> None:
    _write_sales_record(tmp_path, "waigani", "2026-04-07", gross_sales=500.0, traffic=10, served=4, sales_per_labor_hour=250.0)

    assert build_branch_comparison_main(["--date", "2026-04-07", "--root", str(tmp_path)]) == 0

    with pytest.raises(FileExistsError):
        build_branch_comparison_main(["--date", "2026-04-07", "--root", str(tmp_path)])

    assert build_branch_comparison_main(["--date", "2026-04-07", "--root", str(tmp_path), "--overwrite"]) == 0


def test_branch_daily_worker_and_cli_require_explicit_overwrite_flag(tmp_path: Path) -> None:
    _write_sales_record(tmp_path, "waigani", "2026-04-07", gross_sales=500.0, traffic=10, served=4, sales_per_labor_hour=250.0)
    _write_staff_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        items=[_staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2)],
        total_items_moved=10,
        total_assisting_count=2,
    )

    first = process_branch_daily_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path)},
        )
    )
    assert first.payload["status"] == "written"
    assert Path(first.payload["output_path"]) == get_branch_daily_analytics_path("waigani", "2026-04-07", output_root=tmp_path)

    with pytest.raises(FileExistsError):
        process_branch_daily_work_item(
            WorkItem(
                kind="analytics_request",
                payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path)},
            )
        )

    second = process_branch_daily_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path), "overwrite": True},
        )
    )
    assert second.payload["status"] == "written"

    cli_root = tmp_path / "cli_branch_daily"
    _write_sales_record(cli_root, "waigani", "2026-04-07", gross_sales=500.0, traffic=10, served=4, sales_per_labor_hour=250.0)
    _write_staff_record(
        cli_root,
        "waigani",
        "2026-04-07",
        items=[_staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2)],
        total_items_moved=10,
        total_assisting_count=2,
    )
    assert build_branch_daily_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root)]) == 0
    with pytest.raises(FileExistsError):
        build_branch_daily_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root)])
    assert build_branch_daily_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root), "--overwrite"]) == 0


def test_staff_leaderboard_worker_and_cli_require_explicit_overwrite_flag(tmp_path: Path) -> None:
    _write_staff_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        items=[_staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2)],
        total_items_moved=10,
        total_assisting_count=2,
    )

    first = process_staff_leaderboard_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path)},
        )
    )
    assert first.payload["status"] == "written"
    assert Path(first.payload["output_path"]) == get_staff_leaderboard_path("waigani", "2026-04-07", output_root=tmp_path)

    with pytest.raises(FileExistsError):
        process_staff_leaderboard_work_item(
            WorkItem(
                kind="analytics_request",
                payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path)},
            )
        )

    second = process_staff_leaderboard_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path), "overwrite": True},
        )
    )
    assert second.payload["status"] == "written"

    cli_root = tmp_path / "cli_staff_daily"
    _write_staff_record(
        cli_root,
        "waigani",
        "2026-04-07",
        items=[_staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2)],
        total_items_moved=10,
        total_assisting_count=2,
    )
    assert build_staff_leaderboard_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root)]) == 0
    with pytest.raises(FileExistsError):
        build_staff_leaderboard_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root)])
    assert build_staff_leaderboard_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root), "--overwrite"]) == 0


def test_section_productivity_worker_and_cli_require_explicit_overwrite_flag(tmp_path: Path) -> None:
    _write_staff_record(
        tmp_path,
        "waigani",
        "2026-04-07",
        items=[_staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2)],
        total_items_moved=10,
        total_assisting_count=2,
    )

    first = process_section_productivity_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path)},
        )
    )
    assert first.payload["status"] == "written"
    assert Path(first.payload["output_path"]) == get_section_productivity_path("waigani", "2026-04-07", output_root=tmp_path)

    with pytest.raises(FileExistsError):
        process_section_productivity_work_item(
            WorkItem(
                kind="analytics_request",
                payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path)},
            )
        )

    second = process_section_productivity_work_item(
        WorkItem(
            kind="analytics_request",
            payload={"branch": "waigani", "report_date": "2026-04-07", "root": str(tmp_path), "overwrite": True},
        )
    )
    assert second.payload["status"] == "written"

    cli_root = tmp_path / "cli_section_daily"
    _write_staff_record(
        cli_root,
        "waigani",
        "2026-04-07",
        items=[_staff_item("Alice Demo", "on_duty", "mens_tshirt", "Men's Tshirt", items=10, assists=2)],
        total_items_moved=10,
        total_assisting_count=2,
    )
    assert build_section_productivity_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root)]) == 0
    with pytest.raises(FileExistsError):
        build_section_productivity_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root)])
    assert build_section_productivity_main(["--branch", "waigani", "--date", "2026-04-07", "--root", str(cli_root), "--overwrite"]) == 0


def _write_sales_record(
    root: Path,
    branch: str,
    report_date: str,
    *,
    gross_sales: float,
    traffic: int,
    served: int,
    sales_per_labor_hour: float,
    status: str = "ready",
    warnings: list[dict[str, str]] | None = None,
) -> None:
    path = root / "records" / "structured" / "sales_income" / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "branch": branch,
                "report_date": report_date,
                "signal_type": "sales_income",
                "status": status,
                "warnings": warnings or [],
                "metrics": {
                    "gross_sales": gross_sales,
                    "traffic": traffic,
                    "served": served,
                    "conversion_rate": round(served / traffic, 4),
                    "sales_per_labor_hour": sales_per_labor_hour,
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_staff_record(
    root: Path,
    branch: str,
    report_date: str,
    *,
    items: list[dict[str, object]],
    total_items_moved: int,
    total_assisting_count: int,
    unresolved_section_count: int = 0,
    unresolved_examples: list[str] | None = None,
    status: str = "accepted",
) -> None:
    path = root / "records" / "structured" / "hr_performance" / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "branch": branch,
                "report_date": report_date,
                "signal_type": "hr",
                "signal_subtype": "staff_performance",
                "status": status,
                "warnings": [],
                "items": items,
                "metrics": {
                    "total_items_moved": total_items_moved,
                    "total_assisting_count": total_assisting_count,
                    "unresolved_section_count": unresolved_section_count,
                },
                "diagnostics": {
                    "section_resolution_stats": {
                        "resolved_count": len([item for item in items if item.get("section")]),
                        "unresolved_count": unresolved_section_count,
                        "unresolved_examples": unresolved_examples or [],
                    }
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _staff_item(
    staff_name: str,
    duty_status: str,
    section: str | None,
    raw_section: str | None,
    *,
    items: int,
    assists: int,
    role: str | None = None,
) -> dict[str, object]:
    return {
        "staff_name": staff_name,
        "duty_status": duty_status,
        "section": section,
        "raw_section": raw_section,
        "items_moved": items,
        "assisting_count": assists,
        "activity_score": round(items + (assists * 0.5), 2),
        "role": role,
    }
