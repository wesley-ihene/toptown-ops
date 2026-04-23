"""Tests for the pipeline health API route."""

from __future__ import annotations

import json
from pathlib import Path

from analytics import phase4_portal


def test_pipeline_health_api_returns_daily_observability_artifact(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "records" / "observability" / "daily" / "2026_04_07" / "pipeline_health.json",
        {
            "report_date": "2026-04-07",
            "status": "warning",
            "summary": {"intake_volume": 3, "reject_count": 1},
        },
    )

    response = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/analytics/pipeline_health?date=2026-04-07",
        root=tmp_path,
    )
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["artifact"] == "pipeline_health"
    assert body["payload"]["status"] == "warning"


def test_pipeline_health_api_requires_date_and_handles_missing_artifact(tmp_path: Path) -> None:
    missing_date = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/analytics/pipeline_health",
        root=tmp_path,
    )
    missing_date_body = json.loads(missing_date.body.decode("utf-8"))
    assert missing_date.status_code == 400
    assert missing_date_body["error"] == "missing_filters"

    missing_payload = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/analytics/pipeline_health?date=2026-04-07",
        root=tmp_path,
    )
    missing_payload_body = json.loads(missing_payload.body.decode("utf-8"))
    assert missing_payload.status_code == 404
    assert missing_payload_body["error"] == "observability_not_found"


def test_action_and_feedback_summary_endpoints_are_read_only_and_file_backed(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate" / "action-1.json",
        {
            "action_id": "action-1",
            "action_type": "low_conversion_rate",
            "rule_code": "low_conversion_rate",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "signal_type": "sales_income",
            "priority": "high",
            "severity": "warning",
            "assigned_to": "branch_supervisor",
            "requires_ack": True,
            "status": "pending",
            "expires_at": "2026-04-08T23:59:59Z",
        },
    )
    _write_json(
        tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales_income" / "action_1.json",
        {
            "linked_action_id": "action-1",
            "linked_action_path": "records/actions/2026-04-07/waigani/low_conversion_rate/action-1.json",
        },
    )
    _write_json(
        tmp_path / "records" / "feedback" / "2026-04-07" / "waigani" / "action_1.json",
        {
            "feedback_id": "action_1_001",
            "action_id": "action-1",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "status": "acknowledged",
            "acknowledged_by": "Ops One",
            "acknowledged_at": "2026-04-07T10:00:00Z",
            "resolution_note": None,
            "evidence_paths": [],
            "source_action_path": "records/actions/2026-04-07/waigani/low_conversion_rate/action-1.json",
            "linked_review_queue_path": "records/review/2026_04_07/waigani/sales_income/action_1.json",
            "version": "v1",
            "history": [
                {
                    "feedback_id": "action_1_001",
                    "status": "acknowledged",
                    "acknowledged_by": "Ops One",
                    "acknowledged_at": "2026-04-07T10:00:00Z",
                    "resolution_note": None,
                    "evidence_paths": [],
                    "source_action_path": "records/actions/2026-04-07/waigani/low_conversion_rate/action-1.json",
                    "linked_review_queue_path": "records/review/2026_04_07/waigani/sales_income/action_1.json",
                }
            ],
        },
    )

    pending = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/actions/pending?date=2026-04-07&branch=waigani",
        root=tmp_path,
    )
    pending_body = json.loads(pending.body.decode("utf-8"))
    assert pending.status_code == 200
    assert pending_body["product"] == "actions_pending"
    assert pending_body["payload"]["summary"]["pending_actions"] == 1
    assert pending_body["payload"]["pending_actions"][0]["linked_review_queue_path"] is not None

    action_summary = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/actions/summary?date=2026-04-07&branch=waigani",
        root=tmp_path,
    )
    action_summary_body = json.loads(action_summary.body.decode("utf-8"))
    assert action_summary.status_code == 200
    assert action_summary_body["product"] == "actions_summary"
    assert action_summary_body["payload"]["summary"]["actions_acknowledged"] == 1
    assert action_summary_body["payload"]["summary"]["review_linked_actions"] == 1

    feedback_summary = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/feedback/summary?date=2026-04-07&branch=waigani",
        root=tmp_path,
    )
    feedback_summary_body = json.loads(feedback_summary.body.decode("utf-8"))
    assert feedback_summary.status_code == 200
    assert feedback_summary_body["product"] == "feedback_summary"
    assert feedback_summary_body["payload"]["summary"]["feedback_records"] == 1
    assert feedback_summary_body["payload"]["feedback_items"][0]["status"] == "acknowledged"


def test_learning_endpoints_return_latest_artifacts_with_stable_schema(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "records" / "learning" / "review_summary" / "2026-04-06.json",
        {
            "date": "2026-04-06",
            "generated_at": "2026-04-06T08:00:00Z",
            "artifact_type": "review_summary",
            "report_date": "2026-04-06",
            "total_review_items": 2,
        },
    )
    _write_json(
        tmp_path / "records" / "learning" / "review_summary" / "2026-04-07.json",
        {
            "date": "2026-04-07",
            "generated_at": "2026-04-07T08:00:00Z",
            "artifact_type": "review_summary",
            "report_date": "2026-04-07",
            "total_review_items": 4,
        },
    )
    _write_json(
        tmp_path / "records" / "learning" / "action_effectiveness" / "2026-04-07.json",
        {
            "date": "2026-04-07",
            "generated_at": "2026-04-07T08:05:00Z",
            "artifact_type": "action_effectiveness",
            "report_date": "2026-04-07",
            "total_actions": 3,
        },
    )
    _write_json(
        tmp_path / "records" / "learning" / "threshold_recommendations" / "2026-04-07.json",
        {
            "date": "2026-04-07",
            "generated_at": "2026-04-07T08:10:00Z",
            "artifact_type": "threshold_recommendations",
            "report_date": "2026-04-07",
            "recommendation_count": 1,
        },
    )
    _write_json(
        tmp_path / "records" / "learning" / "format_drift" / "2026-04-07.json",
        {
            "date": "2026-04-07",
            "generated_at": "2026-04-07T08:15:00Z",
            "artifact_type": "format_drift",
            "report_date": "2026-04-07",
            "total_raw_records": 5,
        },
    )

    review = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/learning/review",
        root=tmp_path,
    )
    review_body = json.loads(review.body.decode("utf-8"))
    assert review.status_code == 200
    assert review_body == {
        "status": "ok",
        "service": "phase4_dashboard_api",
        "artifact_date": "2026-04-07",
        "generated_at": "2026-04-07T08:00:00Z",
        "data": {
            "artifact_type": "review_summary",
            "report_date": "2026-04-07",
            "total_review_items": 4,
        },
    }

    actions = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/learning/actions",
        root=tmp_path,
    )
    actions_body = json.loads(actions.body.decode("utf-8"))
    assert actions.status_code == 200
    assert actions_body["status"] == "ok"
    assert actions_body["artifact_date"] == "2026-04-07"
    assert actions_body["generated_at"] == "2026-04-07T08:05:00Z"
    assert actions_body["data"]["artifact_type"] == "action_effectiveness"
    assert actions_body["data"]["total_actions"] == 3

    recommendations = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/learning/recommendations",
        root=tmp_path,
    )
    recommendations_body = json.loads(recommendations.body.decode("utf-8"))
    assert recommendations.status_code == 200
    assert recommendations_body["data"]["artifact_type"] == "threshold_recommendations"
    assert recommendations_body["data"]["recommendation_count"] == 1

    format_drift = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/learning/format-drift",
        root=tmp_path,
    )
    format_drift_body = json.loads(format_drift.body.decode("utf-8"))
    assert format_drift.status_code == 200
    assert format_drift_body["data"]["artifact_type"] == "format_drift"
    assert format_drift_body["data"]["total_raw_records"] == 5

    summary = phase4_portal.dispatch_http_request(
        method="GET",
        target="/api/learning/summary",
        root=tmp_path,
    )
    summary_body = json.loads(summary.body.decode("utf-8"))
    assert summary.status_code == 200
    assert summary_body == {
        "status": "ok",
        "service": "phase4_dashboard_api",
        "artifact_date": "2026-04-07",
        "generated_at": "2026-04-07T08:15:00Z",
        "data": {
            "review": {
                "artifact_type": "review_summary",
                "report_date": "2026-04-07",
                "total_review_items": 4,
            },
            "actions": {
                "artifact_type": "action_effectiveness",
                "report_date": "2026-04-07",
                "total_actions": 3,
            },
            "recommendations": {
                "artifact_type": "threshold_recommendations",
                "report_date": "2026-04-07",
                "recommendation_count": 1,
            },
            "format_drift": {
                "artifact_type": "format_drift",
                "report_date": "2026-04-07",
                "total_raw_records": 5,
            },
        },
    }


def test_learning_endpoints_handle_no_data_without_write_side_effects(tmp_path: Path) -> None:
    before_paths = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))

    endpoints = [
        "/api/learning/summary",
        "/api/learning/review",
        "/api/learning/actions",
        "/api/learning/recommendations",
        "/api/learning/format-drift",
    ]
    for endpoint in endpoints:
        response = phase4_portal.dispatch_http_request(
            method="GET",
            target=endpoint,
            root=tmp_path,
        )
        body = json.loads(response.body.decode("utf-8"))
        assert response.status_code == 200
        assert body == {
            "status": "ok",
            "service": "phase4_dashboard_api",
            "artifact_date": None,
            "generated_at": None,
            "data": {} if endpoint != "/api/learning/summary" else {
                "review": {},
                "actions": {},
                "recommendations": {},
                "format_drift": {},
            },
        }

    after_paths = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    assert after_paths == before_paths


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
