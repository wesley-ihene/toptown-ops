"""Tests for deterministic cross-branch analytics queries."""

from __future__ import annotations

import json
import os
from pathlib import Path

from apps.analytics_query_engine import worker as query_worker
from apps.cross_branch_router.worker import handle_cross_branch_query
from scripts import whatsapp_webhook_bridge as bridge


def test_execute_cross_branch_query_returns_ranked_sales_summary(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)
    _write_branch_comparison(
        tmp_path,
        "2026-04-23",
        [
            {
                "branch": "waigani",
                "gross_sales": 800.0,
                "conversion_rate": 0.5,
                "staff_productivity_index": 10.0,
                "operational_score": 70,
            },
            {
                "branch": "bena_road",
                "gross_sales": 1200.0,
                "conversion_rate": 0.4,
                "staff_productivity_index": 9.0,
                "operational_score": 65,
            },
        ],
    )

    result = query_worker.execute_cross_branch_query(
        {
            "command_name": "cross_branch_query",
            "query_type": "branch_sales_rank",
            "branch": "waigani",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "completed"
    assert result["report_date"] == "2026-04-23"
    assert result["rank"] == 2
    assert result["total_branches"] == 2
    assert result["branch_value"] == 800.0
    assert result["top_branch"] == "bena_road"
    assert result["top_value"] == 1200.0


def test_handle_cross_branch_query_formats_response_text(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)
    _write_branch_comparison(
        tmp_path,
        "2026-04-23",
        [
            {
                "branch": "waigani",
                "gross_sales": 1000.0,
                "conversion_rate": 0.75,
                "staff_productivity_index": 18.0,
                "operational_score": 90,
            },
            {
                "branch": "lae_malaita",
                "gross_sales": 900.0,
                "conversion_rate": 0.55,
                "staff_productivity_index": 12.0,
                "operational_score": 70,
            },
        ],
    )

    response = handle_cross_branch_query(
        {
            "command_name": "cross_branch_query",
            "query_type": "branch_conversion_rank",
            "branch": "waigani",
            "sender_phone": "67570000000",
            "channel": "whatsapp",
            "should_reply": True,
        },
        output_root=tmp_path,
    )

    assert response["response_type"] == "command_reply"
    assert response["response_text"] == (
        "Waigani ranks 1 of 2 branches by conversion on 2026-04-23. "
        "Waigani: 75.00%. Top branch: Waigani (75.00%)."
    )


def test_cross_branch_query_fails_safely_when_analytics_are_missing(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)

    result = query_worker.execute_cross_branch_query(
        {
            "command_name": "cross_branch_query",
            "query_type": "branch_productivity_rank",
            "branch": "waigani",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "failed"
    assert result["reason"] == "analytics_not_found"


def test_cross_branch_query_denies_unauthorized_branch_access(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)
    _write_branch_comparison(
        tmp_path,
        "2026-04-23",
        [
            {
                "branch": "lae_malaita",
                "gross_sales": 900.0,
                "conversion_rate": 0.55,
                "staff_productivity_index": 12.0,
                "operational_score": 70,
            }
        ],
    )

    result = query_worker.execute_cross_branch_query(
        {
            "command_name": "cross_branch_query",
            "query_type": "branch_operational_rank",
            "branch": "lae_malaita",
            "sender_phone": "67570000000",
        },
        output_root=tmp_path,
    )

    assert result["status"] == "unauthorized"
    assert result["reason"] == "branch_not_allowed"


def test_cross_branch_query_is_replay_safe(tmp_path: Path, monkeypatch) -> None:
    _patch_supervisors(monkeypatch, tmp_path)

    result = query_worker.execute_cross_branch_query(
        {
            "command_name": "cross_branch_query",
            "query_type": "branch_sales_rank",
            "branch": "waigani",
            "sender_phone": "67570000000",
            "is_replay": True,
        },
        output_root=tmp_path,
    )

    assert result["status"] == "failed"
    assert result["reason"] == "replay_ignored"


def test_bridge_bypasses_pipeline_for_cross_branch_query(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    _write_branch_comparison(
        tmp_path,
        "2026-04-23",
        [
            {
                "branch": "waigani",
                "gross_sales": 1000.0,
                "conversion_rate": 0.75,
                "staff_productivity_index": 18.0,
                "operational_score": 90,
            },
            {
                "branch": "lae_malaita",
                "gross_sales": 900.0,
                "conversion_rate": 0.55,
                "staff_productivity_index": 12.0,
                "operational_score": 70,
            },
        ],
    )

    def should_not_run(*args, **kwargs):
        raise AssertionError("report pipeline should not run for cross-branch queries")

    monkeypatch.setattr(bridge, "validate_inbound_text", should_not_run)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", should_not_run)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(
            _meta_payload(message_id="wamid.cross.query", text="rank sales for waigani")
        ).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = json.loads(Path(body["conversation_response"]["json_path"]).read_text(encoding="utf-8"))

    assert response.status_code == 200
    assert body["command"] is True
    assert body["command_name"] == "cross_branch_query"
    assert artifact_payload["response_type"] == "command_reply"
    if _taop_feedback_enabled():
        assert artifact_payload["response_text"] == (
            "Waigani ranks 1 of 2 branches by sales on 2026-04-23. "
            "Waigani: K1,000.00. Top branch: Waigani (K1,000.00)."
        )
    else:
        assert artifact_payload["response_text"] == _TAOP_DISABLED_RESPONSE_TEXT


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    import packages.record_store.paths as record_paths

    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(bridge, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", raising=False)
    monkeypatch.delenv("TOPTOWN_ENABLE_REPLAY_RESPONSES", raising=False)


def _patch_supervisors(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config" / "supervisors.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        '{"supervisors":[{"name":"Alice","sender_phone":"67570000000","branches":["waigani"]}]}',
        encoding="utf-8",
    )
    import apps.supervisor_auth.worker as supervisor_auth_worker

    monkeypatch.setattr(supervisor_auth_worker, "_CONFIG_PATH", config_path)
    supervisor_auth_worker._load_supervisors_cached.cache_clear()


def _write_branch_comparison(root: Path, report_date: str, rows: list[dict[str, object]]) -> None:
    ranked_sales = sorted(rows, key=lambda row: float(row.get("gross_sales") or 0), reverse=True)
    ranked_conversion = sorted(rows, key=lambda row: float(row.get("conversion_rate") or 0), reverse=True)
    ranked_productivity = sorted(rows, key=lambda row: float(row.get("staff_productivity_index") or 0), reverse=True)
    ranked_ops = sorted(rows, key=lambda row: float(row.get("operational_score") or 0), reverse=True)
    path = root / "analytics" / "branch_comparison" / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "report_date": report_date,
                "branch_scorecards": rows,
                "ranked_branches_by_sales": _rank(ranked_sales, "gross_sales"),
                "ranked_branches_by_conversion": _rank(ranked_conversion, "conversion_rate"),
                "ranked_branches_by_staff_productivity": _rank(ranked_productivity, "staff_productivity_index"),
                "ranked_branches_by_operational_score": _rank(ranked_ops, "operational_score"),
                "warnings": [],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
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


def _meta_payload(*, message_id: str, text: str) -> dict[str, object]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {
                                "display_phone_number": "15551230000",
                                "phone_number_id": "pnid-1",
                            },
                            "contacts": [
                                {
                                    "profile": {"name": "Alice"},
                                    "wa_id": "67570000000",
                                }
                            ],
                            "messages": [
                                {
                                    "from": "67570000000",
                                    "id": message_id,
                                    "timestamp": "1775563200",
                                    "text": {"body": text},
                                    "type": "text",
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


_TAOP_DISABLED_RESPONSE_TEXT = "[TAOP DISABLED - AGENT OUTPUT ONLY]"


def _taop_feedback_enabled() -> bool:
    return os.getenv("TAOP_FEEDBACK_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
