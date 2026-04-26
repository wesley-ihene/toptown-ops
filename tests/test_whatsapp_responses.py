"""Tests for Phase C1 WhatsApp response integration."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from packages.observability import load_daily_artifact
import packages.record_store.paths as record_paths
from packages.response_store import load_response_artifact
from packages.record_store.automation import generate_whatsapp_conversation_reply
from packages.signal_contracts.agent_result import AgentResult
from scripts import whatsapp_webhook_bridge as bridge


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def test_webhook_generates_accepted_response_artifact(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "governance": {"status": "accepted", "reasons": []},
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.accepted-phase-c1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact = body["conversation_response"]
    artifact_payload = _read_json(Path(artifact["json_path"]))
    observability = load_daily_artifact("conversation_replies", _today_utc(), output_root=tmp_path)

    assert response.status_code == 200
    assert artifact["response_type"] == "accepted_ack"
    assert artifact["dispatch_status"] == "generated"
    assert body["outputs"][-1] == artifact["json_path"]
    assert artifact_payload["source_message_id"] == "wamid.accepted-phase-c1"
    assert observability is not None
    assert observability["summary"]["conversation_replies_generated"] == 1


def test_webhook_generates_review_response_artifact(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "needs_review",
                "governance": {
                    "status": "needs_review",
                    "reasons": ["confidence_between_review_and_accept_thresholds"],
                },
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
            metadata={"review_queue_path": "records/review/2026_04_07/waigani/sales_income/review-1.json"},
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.review-phase-c1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert body["conversation_response"]["response_type"] == "review_ack"
    assert artifact_payload["dispatch_status"] == "generated"


def test_webhook_generates_rejected_response_artifact(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="orchestrator_agent",
            payload={
                "status": "rejected",
                "governance": {"status": "rejected", "reasons": ["validation_failed"]},
                "classification": {"report_type": "sales"},
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.rejected-phase-c1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["conversation_response"]["response_type"] == "rejected_fix_request"
    assert body["outputs"] == [body["conversation_response"]["json_path"]]


def test_webhook_generates_duplicate_response_artifact(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    request_body = json.dumps(_meta_payload(message_id="wamid.duplicate-phase-c1")).encode("utf-8")

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "governance": {"status": "accepted", "reasons": []},
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    first = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_body)
    second = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_body)

    second_body = json.loads(second.body.decode("utf-8"))

    assert first.status_code == 200
    assert second.status_code == 200
    assert second_body["duplicate"] is True
    assert second_body["conversation_response"]["response_type"] == "duplicate_notice"
    assert second_body["conversation_response"]["dispatch_status"] == "generated"


def test_webhook_generates_unknown_response_artifact(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="orchestrator_agent",
            payload={
                "status": "needs_review",
                "governance": {"status": "needs_review", "reasons": ["unknown_report_type"]},
                "routing": {"classification": "unknown"},
                "classification": {"report_type": "unknown"},
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.unknown-phase-c1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["conversation_response"]["response_type"] == "unknown_message_guidance"
    assert body["outputs"] == [body["conversation_response"]["json_path"]]


def test_replay_suppresses_outbound_response_by_default(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="orchestrator_agent",
            payload={
                "status": "needs_review",
                "governance": {"status": "needs_review", "reasons": ["confidence_missing"]},
                "routing": {"classification": "unknown"},
            },
        )

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    replay_payload = {
        "payload": {
            "text": "Replay message body",
            "message_id": "wamid.replay-phase-c1",
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
    observability = load_daily_artifact("conversation_replies", _today_utc(), output_root=tmp_path)

    assert response.status_code == 200
    assert body["conversation_response"]["dispatch_status"] == "suppressed"
    assert observability is not None
    assert observability["summary"]["conversation_replies_suppressed"] == 1


def test_response_generation_failure_does_not_break_main_flow(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "accepted",
                "governance": {"status": "accepted", "reasons": []},
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
            },
        )

    def explode(**kwargs):
        raise RuntimeError("response layer exploded")

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)
    monkeypatch.setattr(bridge, "generate_whatsapp_conversation_reply", explode)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.failure-phase-c1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["orchestrator_status"] == "accepted"
    assert body["conversation_response"] is None


def test_duplicate_response_protection_prevents_repeated_send_for_same_source_message(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", "live")
    dispatch_calls: list[str] = []

    def dispatcher(payload):
        dispatch_calls.append(str(payload["response_type"]))
        return {"dispatch_status": "sent"}

    outcome = AgentResult(
        agent_name="sales_income_agent",
        payload={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
        },
    )

    first = generate_whatsapp_conversation_reply(
        outcome=outcome,
        source_message_id="wamid.idempotent-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
        dispatcher=dispatcher,
    )
    second = generate_whatsapp_conversation_reply(
        outcome=outcome,
        source_message_id="wamid.idempotent-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
        dispatcher=dispatcher,
    )

    assert first is not None
    assert second is not None
    assert first["dispatch_status"] == "sent"
    assert second["dispatch_status"] == "duplicate"
    assert dispatch_calls == ["accepted_ack"]
    observability = load_daily_artifact("conversation_replies", _today_utc(), output_root=tmp_path)
    assert observability is not None
    assert observability["summary"]["conversation_replies_generated"] == 1
    assert observability["summary"]["conversation_replies_sent"] == 1


def test_live_mode_writes_response_artifact_before_dispatch_and_updates_after_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", "live")

    def dispatcher(payload):
        artifact = load_response_artifact(str(payload["response_id"]), output_root=tmp_path)
        assert artifact is not None
        assert artifact["payload"]["dispatch_status"] == "generated"
        assert artifact["payload"]["provider_message_id"] is None
        assert artifact["payload"]["dispatch_error"] is None
        return {
            "dispatch_status": "sent",
            "provider_message_id": "wamid.provider-1",
            "http_status": 200,
        }

    result = generate_whatsapp_conversation_reply(
        outcome=_accepted_outcome(),
        source_message_id="wamid.live-success-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
        dispatcher=dispatcher,
    )

    assert result is not None
    artifact = _read_json(Path(result["json_path"]))
    assert artifact["dispatch_status"] == "sent"
    assert artifact["provider_message_id"] == "wamid.provider-1"
    assert artifact["dispatch_error"] is None
    assert artifact["http_status"] == 200


def test_live_mode_failed_dispatch_updates_response_artifact_without_breaking_flow(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", "live")

    def dispatcher(payload):
        artifact = load_response_artifact(str(payload["response_id"]), output_root=tmp_path)
        assert artifact is not None
        assert artifact["payload"]["dispatch_status"] == "generated"
        raise RuntimeError("network exploded")

    result = generate_whatsapp_conversation_reply(
        outcome=_accepted_outcome(),
        source_message_id="wamid.live-failed-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
        dispatcher=dispatcher,
    )

    assert result is not None
    assert result["dispatch_status"] == "failed"
    artifact = _read_json(Path(result["json_path"]))
    assert artifact["dispatch_status"] == "failed"
    assert artifact["provider_message_id"] is None
    assert artifact["dispatch_error"] == "network exploded"
    assert artifact["http_status"] is None


def test_write_only_mode_generates_artifact_without_live_send(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", "write_only")
    dispatch_calls = 0

    def dispatcher(payload):
        nonlocal dispatch_calls
        dispatch_calls += 1
        return {"dispatch_status": "sent"}

    outcome = AgentResult(
        agent_name="sales_income_agent",
        payload={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
        },
    )

    result = generate_whatsapp_conversation_reply(
        outcome=outcome,
        source_message_id="wamid.write-only-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
        dispatcher=dispatcher,
    )

    assert result is not None
    assert result["dispatch_status"] == "generated"
    assert dispatch_calls == 0
    observability = load_daily_artifact("conversation_replies", _today_utc(), output_root=tmp_path)
    assert observability is not None
    assert observability["summary"]["conversation_replies_generated"] == 1
    assert observability["summary"]["conversation_replies_sent"] == 0


def _meta_payload(*, message_id: str) -> dict[str, object]:
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
                                    "text": {"body": "DAY-END SALES REPORT\nBranch: Waigani"},
                                    "type": "text",
                                }
                            ],
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
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(bridge, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", raising=False)
    monkeypatch.delenv("TOPTOWN_ENABLE_REPLAY_RESPONSES", raising=False)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _accepted_outcome() -> AgentResult:
    return AgentResult(
        agent_name="sales_income_agent",
        payload={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
        },
    )
