"""Focused tests for the live WhatsApp webhook bridge."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path

from packages.observability import load_daily_artifact
import packages.record_store.paths as record_paths
from packages.signal_contracts.agent_result import AgentResult
from scripts import whatsapp_webhook_bridge as bridge


def test_live_webhook_writes_raw_then_invokes_orchestrator(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    captured_work_items = []

    def fake_process_work_item(work_item):
        captured_work_items.append(work_item)
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload()).encode("utf-8"),
        headers={"X-Hub-Signature-256": "sha256=test"},
    )

    body = json.loads(response.body.decode("utf-8"))
    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")

    assert response.status_code == 200
    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert len(captured_work_items) == 1
    assert body["ok"] is True
    assert body["raw_written"] is True
    assert body["replay"] is False
    assert body["orchestrator_status"] == "accepted"
    assert body["agent"] == "sales_income_agent"
    assert body["route"] == "sales"
    assert body["outputs"] == [
        str(tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"),
        body["conversation_response"]["json_path"],
    ]

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["message_id"] == "wamid.live-1"
    assert raw_meta["processing_status"] == "received"
    assert raw_meta["human_tolerance"]["original_text_hash"]
    assert raw_meta["human_tolerance"]["normalized_text_hash"]

    work_item = captured_work_items[0]
    assert work_item.kind == "raw_message"
    assert "classification" not in work_item.payload
    assert work_item.payload["ingress_policy"] == {"reject_mixed_reports": False}
    assert work_item.payload["replay"] == {"is_replay": False}
    assert work_item.payload["raw_record"]["raw_written"] is True
    assert work_item.payload["raw_message"]["normalized_text"] == "DAY-END SALES REPORT\nBranch: Waigani"
    assert work_item.payload["human_tolerance"]["original_text_hash"]
    assert work_item.payload["ingress_envelope"]["payload"]["text"] == "DAY-END SALES REPORT\nBranch: Waigani"


def test_validator_runs_before_orchestrator_and_records_observability(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    call_order: list[str] = []

    def fake_validate_inbound_text(text, *, payload_kind="text", metadata=None):
        del metadata
        call_order.append("validator")
        return {
            "status": "accepted",
            "cleaned_text": text,
            "reasons": [],
            "warnings": [],
            "detected_risks": [],
            "suggested_report_family": "sales",
            "validator_version": "v1",
        }

    def fake_process_work_item(work_item):
        call_order.append("orchestrator")
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge, "validate_inbound_text", fake_validate_inbound_text)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.validate-order-1")).encode("utf-8"),
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    body = json.loads(response.body.decode("utf-8"))
    observability = load_daily_artifact("pre_ingestion_validation", "2026-04-07", output_root=tmp_path)

    assert response.status_code == 200
    assert body["ok"] is True
    assert call_order == ["validator", "orchestrator"]
    assert len(raw_meta_paths) == 1
    assert _read_json(raw_meta_paths[0])["pre_ingestion_validation"]["status"] == "accepted"
    assert observability is not None
    assert observability["summary"]["accepted"] == 1
    assert observability["events"][0]["message_id"] == "wamid.validate-order-1"


def test_validator_reject_still_writes_raw_txt_and_meta(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    orchestrator_calls = 0

    def fake_validate_inbound_text(text, *, payload_kind="text", metadata=None):
        del text, payload_kind, metadata
        return {
            "status": "rejected",
            "cleaned_text": "",
            "reasons": [{"code": "empty_input", "message": "empty"}],
            "warnings": [],
            "detected_risks": [],
            "suggested_report_family": None,
            "validator_version": "v1",
        }

    def fake_process_work_item(work_item):
        nonlocal orchestrator_calls
        orchestrator_calls += 1
        return AgentResult(agent_name="orchestrator_agent", payload={"status": "accepted"})

    monkeypatch.setattr(bridge, "validate_inbound_text", fake_validate_inbound_text)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.reject-1", text="   \r\n\t")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["raw_written"] is True
    assert body["orchestrator_status"] == "skipped"
    assert body["validator_status"] == "rejected"
    assert body["validator_reason_summary"] == ["empty_input"]
    assert orchestrator_calls == 0
    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert _read_json(raw_meta_paths[0])["pre_ingestion_validation"]["status"] == "rejected"


def test_cleaned_text_is_forwarded_when_validator_cleans_input(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    captured_work_items = []
    original_text = "  DAY-END SALES REPORT\r\n\r\nBranch: Waigani  "
    normalized_input = "DAY-END SALES REPORT\n\nBranch: Waigani"
    cleaned_text = "DAY-END SALES REPORT\n\nBranch: Waigani"

    def fake_validate_inbound_text(text, *, payload_kind="text", metadata=None):
        del payload_kind, metadata
        assert text == normalized_input
        return {
            "status": "cleaned",
            "cleaned_text": cleaned_text,
            "reasons": [{"code": "trimmed_whitespace", "message": "trimmed"}],
            "warnings": [],
            "detected_risks": [],
            "suggested_report_family": "sales",
            "validator_version": "v1",
        }

    def fake_process_work_item(work_item):
        captured_work_items.append(work_item)
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge, "validate_inbound_text", fake_validate_inbound_text)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.cleaned-1", text=original_text)).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")

    assert response.status_code == 200
    assert body["ok"] is True
    assert len(captured_work_items) == 1
    assert captured_work_items[0].payload["raw_message"]["text"] == original_text
    assert captured_work_items[0].payload["cleaned_text"] == cleaned_text
    assert captured_work_items[0].payload["ingress_envelope"]["payload"]["text"] == cleaned_text
    assert captured_work_items[0].payload["pre_ingestion_validation"]["status"] == "cleaned"
    assert _read_json(raw_meta_paths[0])["pre_ingestion_validation"]["status"] == "cleaned"


def test_replay_webhook_skips_raw_write_and_invokes_orchestrator(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    captured_work_items = []

    def fake_process_work_item(work_item):
        captured_work_items.append(work_item)
        return AgentResult(
            agent_name="orchestrator_agent",
            payload={
                "status": "needs_review",
                "routing": {"classification": "unknown"},
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    replay_payload = {
        "payload": {
            "text": "Replay message body",
            "message_id": "wamid.replay-1",
            "sender_name": "Replay User",
            "sender_phone": "67570000001",
            "group_name": "Waigani",
        },
        "metadata": {
            "received_at": "2026-04-07T12:00:00Z",
            "chat_id": "67570000001",
        },
        "replay": {
            "is_replay": True,
            "source": "raw",
            "original_path": "records/raw/whatsapp/unknown/original.txt",
            "replayed_at": "2026-04-07T12:30:00Z",
        },
    }

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(replay_payload).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt") == []
    assert len(captured_work_items) == 1
    assert body["ok"] is True
    assert body["raw_written"] is False
    assert body["replay"] is True
    assert body["orchestrator_status"] == "needs_review"
    assert body["raw_txt_path"] == "records/raw/whatsapp/unknown/original.txt"

    work_item = captured_work_items[0]
    assert work_item.payload["replay"] == replay_payload["replay"]
    assert work_item.payload["raw_record"]["raw_written"] is False


def test_duplicate_live_webhook_does_not_duplicate_raw_write_or_dispatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    dispatch_count = 0

    def fake_process_work_item(work_item):
        nonlocal dispatch_count
        dispatch_count += 1
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)
    request_body = json.dumps(_meta_payload()).encode("utf-8")

    first = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_body)
    second = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_body)

    second_body = json.loads(second.body.decode("utf-8"))

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(_paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")) == 1
    assert dispatch_count == 1
    assert second_body["ok"] is True
    assert second_body["duplicate"] is True
    assert second_body["raw_written"] is False
    assert second_body["orchestrator_status"] == "skipped"


def test_same_raw_sha256_with_different_message_ids_is_rejected_within_24_hours(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    specialist_calls = 0
    repeated_text = "DAY-END SALES REPORT\nBranch: Waigani\nDate: 2026-04-07\nGross Sales: 1200"
    request_one = json.dumps(_meta_payload(message_id="wamid.live-1", text=repeated_text)).encode("utf-8")
    request_two = json.dumps(_meta_payload(message_id="wamid.live-2", text=repeated_text)).encode("utf-8")

    def fake_dispatch_to_specialist(work_item, *, target_agent):
        nonlocal specialist_calls
        specialist_calls += 1
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "accepted",
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "_dispatch_to_specialist", fake_dispatch_to_specialist)

    first = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_one)
    second = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_two)

    first_body = json.loads(first.body.decode("utf-8"))
    second_body = json.loads(second.body.decode("utf-8"))
    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first_body["ok"] is True
    assert second_body["ok"] is True
    assert first_body["raw_written"] is True
    assert second_body["raw_written"] is True
    assert first_body["duplicate"] is False
    assert second_body["duplicate"] is True
    assert second_body["duplicate_reason"] == "duplicate_message"
    assert second_body["orchestrator_status"] == "duplicate"
    assert specialist_calls == 1
    assert len(raw_text_paths) == 2
    assert second_body["raw_txt_path"].endswith(".txt")
    assert "__" in Path(second_body["raw_txt_path"]).stem


def test_operational_query_returns_attendance_status_without_validator_or_orchestrator(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    _write_structured_record(tmp_path, "hr_attendance", "waigani", "2026-04-25")
    _write_structured_record(tmp_path, "hr_attendance", "lae_5th_street", "2026-04-25")
    _write_structured_record(tmp_path, "hr_attendance", "lae_malaita", "2026-04-25")

    def should_not_run(*args, **kwargs):
        raise AssertionError("report pipeline should not run for operational query")

    monkeypatch.setattr(bridge, "validate_inbound_text", should_not_run)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", should_not_run)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(
            _meta_payload(
                message_id="wamid.query.attendance-1",
                text="confirm you receive all four STAFFS ATTENDANCE report for the date 25/04/26?",
            )
        ).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    observability = load_daily_artifact("conversation_replies", _today_utc(), output_root=tmp_path)

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["query"] is True
    assert body["orchestrator_status"] == "skipped"
    assert body["conversation_response"]["response_type"] == "operational_query_status"
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))
    assert "STATUS: ⚠️ PARTIAL / NEEDS CHECK" in artifact_payload["response_text"]
    assert "❌ Bena Road" in artifact_payload["response_text"]
    assert observability is not None
    assert observability["events"][0]["response_type"] == "operational_query_status"
    assert observability["events"][0]["report_type"] == "attendance_status"
    assert observability["events"][0]["date"] == "2026-04-25"
    assert observability["events"][0]["outcome"] == "success"


def test_webhooks_whatsapp_alias_matches_webhook_post_behavior(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    alias_response = bridge.dispatch_http_request(
        method="POST",
        target="/webhooks/whatsapp",
        body=json.dumps(_meta_payload(message_id="wamid.alias-1", text="DAY-END SALES REPORT\nBranch: Waigani\nAlias")).encode("utf-8"),
    )
    primary_response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.primary-1", text="DAY-END SALES REPORT\nBranch: Waigani\nPrimary")).encode("utf-8"),
    )

    alias_body = json.loads(alias_response.body.decode("utf-8"))
    primary_body = json.loads(primary_response.body.decode("utf-8"))

    assert alias_response.status_code == 200
    assert primary_response.status_code == 200
    assert alias_body["ok"] is True
    assert primary_body["ok"] is True
    assert alias_body["agent"] == "sales_income_agent"
    assert primary_body["agent"] == "sales_income_agent"
    assert len(_paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")) == 2


def test_orchestrator_exception_preserves_raw_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        raise RuntimeError("downstream exploded")

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.fail-1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")

    assert response.status_code == 200
    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert body["ok"] is False
    assert body["raw_written"] is True
    assert body["orchestrator_status"] == "failed"
    assert body["error_stage"] == "orchestrator"
    assert raw_text_paths[0].read_text(encoding="utf-8") == "DAY-END SALES REPORT\nBranch: Waigani"


def test_live_webhook_fans_out_mixed_report_when_split_is_safe(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(
            _meta_payload(
                message_id="wamid.mixed-1",
                text="\n".join(
                    [
                        "Branch: Waigani Branch",
                        "Date: 07/04/2026",
                        "",
                        "DAY-END SALES REPORT",
                        "Gross Sales: 1200",
                        "Cash Sales: 600",
                        "",
                        "SUPERVISOR CONTROL REPORT",
                        "Floor Check: Passed",
                    ]
                ),
            )
        ).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_text_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.txt")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["agent"] == "orchestrator_agent"
    assert body["orchestrator_status"] in {"accepted", "accepted_with_warning"}
    assert body["route"] == "mixed"
    assert body["conversation_response"]["response_type"] == "accepted_ack"
    assert len(body["outputs"]) == 3
    assert body["conversation_response"]["json_path"] in body["outputs"]
    assert len(raw_meta_paths) == 1
    assert len(rejected_text_paths) == 0
    assert len(rejected_meta_paths) == 0
    assert (tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json").exists()
    assert (tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json").exists()

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "mixed"
    assert raw_meta["routing_target"] == "fan_out"
    assert raw_meta["processing_status"] == "processed"


def test_live_webhook_mixed_sales_and_supervisor_warning_uses_success_ack(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(
            _meta_payload(
                message_id="wamid.mixed-warning-1",
                text="\n".join(
                    [
                        "Branch: Bena Road Branch",
                        "Date: 28/04/2026",
                        "",
                        "DAY-END SALES REPORT",
                        "Gross Sales: 1200",
                        "Cash Sales: 600",
                        "Eftpos Sales: 600",
                        "Traffic: 12",
                        "Served: 9",
                        "",
                        "Supervisor Control Summary",
                        "Notes: Skeleton team only",
                    ]
                ),
            )
        ).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["agent"] == "orchestrator_agent"
    assert body["orchestrator_status"] == "accepted_with_warning"
    assert body["route"] == "mixed"
    assert body["conversation_response"]["response_type"] == "accepted_ack"
    assert (tmp_path / "records" / "structured" / "sales_income" / "bena_road" / "2026-04-28.json").exists()
    assert (tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "bena_road.json").exists()
    assert not (tmp_path / "records" / "structured" / "supervisor_control" / "bena_road" / "2026-04-28.json").exists()
    assert "TAOP REVIEW REQUIRED" not in artifact_payload["response_text"]
    assert "Please resend" not in artifact_payload["response_text"]
    assert "One split report still needs review" not in artifact_payload["response_text"]


def test_live_webhook_mixed_sales_totals_mismatch_surfaces_sales_blocker_details(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(
            _meta_payload(
                message_id="wamid.mixed-review-1",
                text="\n".join(
                    [
                        "Branch: Bena Road Branch",
                        "Date: 28/04/2026",
                        "",
                        "DAY-END SALES REPORT",
                        "Total Sales: 2575",
                        "Total Cash: 2205",
                        "Total Card: 805",
                        "Till Total: 2640",
                        "Deposit Total: 0",
                        "Traffic: 20",
                        "Served: 18",
                        "",
                        "Supervisor Control Summary",
                        "Floor Check: Passed",
                        "Cashier Reconciled: Yes",
                    ]
                ),
            )
        ).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["agent"] == "orchestrator_agent"
    assert body["orchestrator_status"] == "needs_review"
    assert body["conversation_response"]["response_type"] == "review_ack"
    assert not (tmp_path / "records" / "structured" / "sales_income" / "bena_road" / "2026-04-28.json").exists()
    assert (tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "bena_road.json").exists()
    assert "Report: Day-End Sales Report" in artifact_payload["response_text"]
    assert "Sales totals do not match till/payment totals." in artifact_payload["response_text"]
    assert "Declared Total Cash: K2,205.00" in artifact_payload["response_text"]
    assert "Calculated Till Cash: K2,640.00" in artifact_payload["response_text"]
    assert "Declared Total Sales: K2,575.00" in artifact_payload["response_text"]
    assert "Expected Total Sales: K3,010.00" in artifact_payload["response_text"]
    assert "Correct the TOTALS section and resend the Day-End Sales Report." in artifact_payload["response_text"]
    assert "One split report still needs review" not in artifact_payload["response_text"]
    assert "Supervisor Control Report format" not in artifact_payload["response_text"]


def test_live_webhook_mixed_review_keeps_generic_fallback_when_child_detail_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        del work_item
        return AgentResult(
            agent_name="orchestrator_agent",
            payload={
                "status": "needs_review",
                "governance": {"status": "needs_review", "reasons": ["mixed_child_requires_review"]},
                "routing": {"review_reason": "mixed_child_requires_review"},
                "classification": {"report_type": "mixed"},
                "fanout": {
                    "children": [
                        {
                            "report_type": "sales_income",
                            "report_family": "sales_income",
                            "status": "rejected",
                            "blocks_transactional_processing": True,
                        },
                        {
                            "report_type": "supervisor_control",
                            "report_family": "supervisor_control",
                            "report_family_label": "intelligence",
                            "status": "accepted",
                            "blocks_transactional_processing": False,
                        },
                    ]
                },
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.mixed-review-fallback-1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert body["conversation_response"]["response_type"] == "review_ack"
    assert "TAOP split the message into multiple reports." in artifact_payload["response_text"]
    assert "One split report still needs review before final processing." in artifact_payload["response_text"]


def test_health_endpoint_returns_bridge_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    response = bridge.dispatch_http_request(method="GET", target="/health")
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body == {
        "ok": True,
        "orchestrator_enabled": True,
        "raw_root": str(tmp_path / "records" / "raw" / "whatsapp"),
        "service": "whatsapp_webhook_bridge",
        "workspace_root": str(tmp_path),
    }


def test_status_only_payload_is_logged_and_returns_no_supported_messages(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    caplog.set_level(logging.INFO, logger=bridge.__name__)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhooks/whatsapp",
        body=json.dumps(_status_payload()).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["reason"] == "no_supported_messages"
    assert "bridge request received" in caplog.text
    assert "whatsapp payload summary" in caplog.text
    assert "statuses=1" in caplog.text
    assert "reason=no_supported_messages" in caplog.text


def _meta_payload(*, message_id: str = "wamid.live-1", text: str = "DAY-END SALES REPORT\nBranch: Waigani") -> dict[str, object]:
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


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _status_payload() -> dict[str, object]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-status-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "statuses": [
                                {
                                    "id": "wamid.status-1",
                                    "status": "delivered",
                                    "timestamp": "1775563200",
                                }
                            ]
                        },
                    }
                ],
            }
        ],
    }


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "DUPLICATES_DIR", records_dir / "duplicates" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(bridge, "REPO_ROOT", tmp_path)


def _paths(directory: Path, pattern: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob(pattern))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_structured_record(root: Path, signal_type: str, branch: str, report_date: str) -> None:
    if signal_type == "supervisor_control":
        path = root / "records" / "intelligence" / signal_type / report_date / f"{branch}.json"
    else:
        path = root / "records" / "structured" / signal_type / branch / f"{report_date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"branch": branch, "report_date": report_date}, indent=2) + "\n", encoding="utf-8")
