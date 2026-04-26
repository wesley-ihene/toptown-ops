"""Tests for review acknowledgement dispatch through the live bridge."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.outbound_reply_agent import worker as outbound_reply_worker
from packages.signal_contracts.agent_result import AgentResult
from scripts import whatsapp_webhook_bridge as bridge


def test_live_bridge_dispatches_review_ack_without_bypassing_governance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "live")
    outcome_payload = {
        "status": "needs_review",
        "governance": {
            "status": "needs_review",
            "reasons": ["confidence_between_review_and_accept_thresholds"],
        },
        "signal_type": "sales_income",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "export_allowed": False,
    }

    def fake_process_work_item(work_item):
        del work_item
        return AgentResult(
            agent_name="orchestrator_agent",
            payload=outcome_payload,
            metadata={"review_queue_path": "records/review/2026_04_07/waigani/sales_income/review-1.json"},
        )

    def fake_send_whatsapp_text(**kwargs):
        assert kwargs["to"] == "67570000000"
        assert kwargs["response_type"] == "review_ack"
        assert kwargs["source_message_id"] == "wamid.review-dispatch-1"
        return {
            "dispatch_status": "sent",
            "provider_message_id": "wamid.review-provider-1",
            "http_status": 200,
            "error": None,
        }

    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)
    monkeypatch.setattr(outbound_reply_worker, "send_whatsapp_text", fake_send_whatsapp_text)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.review-dispatch-1")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = json.loads(Path(body["conversation_response"]["json_path"]).read_text(encoding="utf-8"))

    assert response.status_code == 200
    assert body["orchestrator_status"] == "needs_review"
    assert body["conversation_response"]["response_type"] == "review_ack"
    assert body["conversation_response"]["dispatch_status"] == "sent"
    assert artifact_payload["dispatch_status"] == "sent"
    assert artifact_payload["provider_message_id"] == "wamid.review-provider-1"
    assert artifact_payload["dispatched_at"] is not None
    assert not any("/structured/" in output for output in body["outputs"])
    assert outcome_payload["export_allowed"] is False


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
