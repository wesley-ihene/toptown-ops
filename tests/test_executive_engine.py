"""Tests for deterministic executive assistant insights."""

from __future__ import annotations

import json
from pathlib import Path

from apps.executive_engine.worker import execute_executive_query
from apps.proposals_store.store import write_optimization_proposal
from packages.learning_store import write_action_effectiveness, write_review_summary, write_threshold_recommendations


def test_execute_executive_overview_aggregates_analytics_learning_and_proposals(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)
    _seed_analytics(tmp_path)
    _seed_learning(tmp_path)
    _seed_proposals(tmp_path)

    result = execute_executive_query(
        {
            "query_type": "executive_overview",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "completed"
    assert result["report_date"] == "2026-04-23"
    assert result["insight"]["top_branch"] == "waigani"
    assert result["insight"]["weakest_branch"] == "lae_malaita"
    assert result["insight"]["critical_alert_count"] >= 1
    assert result["insight"]["pending_proposals_count"] == 1
    assert result["insight"]["top_recurring_review_cause"] == "conflicting_record_same_scope"


def test_execute_executive_proposals_summarizes_pending_and_approved_not_applied(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)
    _seed_analytics(tmp_path)
    _seed_learning(tmp_path)
    _seed_proposals(tmp_path)

    result = execute_executive_query(
        {
            "query_type": "executive_proposals",
            "sender_phone": "67570000000",
            "report_date": "2026-04-23",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "completed"
    assert result["insight"]["pending_proposals_count"] == 1
    assert result["insight"]["approved_not_applied_count"] == 1
    assert result["insight"]["leading_pending_branch"] == "waigani"
    assert result["insight"]["critical_alert_count"] >= 1


def test_execute_executive_query_requires_authorized_sender(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)
    _seed_analytics(tmp_path)

    result = execute_executive_query(
        {
            "query_type": "executive_alerts",
            "sender_phone": "67579999999",
            "report_date": "2026-04-23",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "unauthorized"
    assert result["reason"] == "supervisor_not_found"


def test_execute_executive_query_is_replay_safe(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)

    result = execute_executive_query(
        {
            "query_type": "executive_learning",
            "sender_phone": "67570000000",
            "is_replay": True,
        },
        output_root=tmp_path,
    )

    assert result["status"] == "failed"
    assert result["reason"] == "replay_ignored"


def _patch_supervisors(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config" / "supervisors.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        '{"supervisors":[{"name":"Alice","sender_phone":"67570000000","branches":["waigani","lae_malaita"]}]}',
        encoding="utf-8",
    )
    import apps.supervisor_auth.worker as supervisor_auth_worker

    monkeypatch.setattr(supervisor_auth_worker, "_CONFIG_PATH", config_path)
    supervisor_auth_worker._load_supervisors_cached.cache_clear()


def _seed_analytics(root: Path) -> None:
    _write_branch_daily(
        root,
        "waigani",
        "2026-04-23",
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
        "2026-04-23",
        activity_rows=[
            {"staff_name": "Cara Demo", "activity_score": 19.0, "items_moved": 12, "assisting_count": 4, "duty_status": "on_duty", "section": "shoe_shop"},
            {"staff_name": "Dan Demo", "activity_score": 0.0, "items_moved": 0, "assisting_count": 0, "duty_status": "on_duty", "section": "mens_tshirt"},
        ],
    )
    _write_section_daily(
        root,
        "waigani",
        "2026-04-23",
        sections=[{"section": "shoe_shop", "productivity_index": 18.0, "staff_count": 2, "items_moved": 12, "assisting_count": 6}],
        unresolved_count=0,
    )

    _write_branch_daily(
        root,
        "lae_malaita",
        "2026-04-23",
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
        "2026-04-23",
        activity_rows=[
            {"staff_name": "Alice Demo", "activity_score": 22.0, "items_moved": 17, "assisting_count": 8, "duty_status": "on_duty", "section": "ladies_jeans"},
            {"staff_name": "Bob Demo", "activity_score": 0.0, "items_moved": 0, "assisting_count": 0, "duty_status": "on_duty", "section": None},
        ],
    )
    _write_section_daily(
        root,
        "lae_malaita",
        "2026-04-23",
        sections=[{"section": "ladies_jeans", "productivity_index": 9.0, "staff_count": 3, "items_moved": 8, "assisting_count": 3}],
        unresolved_count=2,
    )

    _write_branch_comparison(
        root,
        "2026-04-23",
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


def _seed_learning(root: Path) -> None:
    write_review_summary(
        "2026-04-23",
        {
            "artifact_type": "review_summary",
            "report_date": "2026-04-23",
            "recurring_review_causes": [
                {
                    "reason": "conflicting_record_same_scope",
                    "count": 2,
                    "report_types": ["sales"],
                    "branches": ["waigani"],
                    "dates": ["2026-04-23"],
                }
            ],
        },
        output_root=root,
    )
    write_action_effectiveness(
        "2026-04-23",
        {
            "artifact_type": "action_effectiveness",
            "report_date": "2026-04-23",
            "noisy_rules_candidates": [{"rule_code": "low_conversion_rate"}],
        },
        output_root=root,
    )
    write_threshold_recommendations(
        "2026-04-23",
        {
            "artifact_type": "threshold_recommendations",
            "report_date": "2026-04-23",
            "recommendations": [{"recommendation_id": "sales__auto_accept_min__decrease"}],
        },
        output_root=root,
    )


def _seed_proposals(root: Path) -> None:
    write_optimization_proposal(
        {
            "proposal_id": "proposal-pending",
            "generated_date": "2026-04-23",
            "proposal_type": "branch_optimization_action",
            "status": "pending_review",
            "approval_status": "pending",
            "apply_status": "not_applied",
            "branch": "waigani",
            "report_type": "sales",
            "summary": "Pending branch coaching proposal.",
            "proposed_action": {
                "action_id": "proposal-pending-action",
                "action_type": "report_quality_coaching",
                "rule_code": "report_quality_coaching",
                "branch": "waigani",
                "report_date": "2026-04-23",
                "signal_type": "optimization_proposal",
                "severity": "warning",
                "priority": "high",
                "assigned_to": "branch_supervisor",
                "requires_ack": True,
                "status": "pending",
                "expires_at": "2026-04-24T23:59:59Z",
                "dedupe_key": "waigani:2026-04-23:report_quality_coaching:conflicting_record_same_scope",
                "scope_key": "conflicting_record_same_scope",
                "summary": "Review branch report quality controls.",
                "evidence": {"review_count": 3},
                "source_paths": [],
            },
            "evidence": {"review_count": 3},
            "source_paths": [],
        },
        output_root=root,
    )
    write_optimization_proposal(
        {
            "proposal_id": "proposal-approved",
            "generated_date": "2026-04-23",
            "proposal_type": "branch_optimization_action",
            "status": "approved",
            "approval_status": "approved",
            "apply_status": "not_applied",
            "branch": "lae_malaita",
            "report_type": "sales",
            "summary": "Approved pending apply proposal.",
            "proposed_action": {
                "action_id": "proposal-approved-action",
                "action_type": "report_quality_coaching",
                "rule_code": "report_quality_coaching",
                "branch": "lae_malaita",
                "report_date": "2026-04-23",
                "signal_type": "optimization_proposal",
                "severity": "warning",
                "priority": "high",
                "assigned_to": "branch_supervisor",
                "requires_ack": True,
                "status": "pending",
                "expires_at": "2026-04-24T23:59:59Z",
                "dedupe_key": "lae_malaita:2026-04-23:report_quality_coaching:conflicting_record_same_scope",
                "scope_key": "conflicting_record_same_scope",
                "summary": "Review branch report quality controls.",
                "evidence": {"review_count": 2},
                "source_paths": [],
            },
            "evidence": {"review_count": 2},
            "source_paths": [],
        },
        output_root=root,
    )


def _write_branch_daily(
    root: Path,
    branch: str,
    report_date: str,
    *,
    gross_sales: float,
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
