"""Focused tests for the live WhatsApp webhook bridge."""

from __future__ import annotations

import json
from pathlib import Path

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
                "status": "ready",
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
    assert body["orchestrator_status"] == "ready"
    assert body["agent"] == "sales_income_agent"
    assert body["route"] == "sales"
    assert body["outputs"] == [
        str(tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json")
    ]

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["message_id"] == "wamid.live-1"
    assert raw_meta["processing_status"] == "received"

    work_item = captured_work_items[0]
    assert work_item.kind == "raw_message"
    assert "classification" not in work_item.payload
    assert work_item.payload["replay"] == {"is_replay": False}
    assert work_item.payload["raw_record"]["raw_written"] is True
    assert work_item.payload["ingress_envelope"]["payload"]["text"] == "DAY-END SALES REPORT\nBranch: Waigani"


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
                "status": "ready",
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


def test_webhooks_whatsapp_alias_matches_webhook_post_behavior(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="sales_income_agent",
            payload={
                "status": "ready",
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


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(bridge, "REPO_ROOT", tmp_path)


def _paths(directory: Path, pattern: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob(pattern))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
