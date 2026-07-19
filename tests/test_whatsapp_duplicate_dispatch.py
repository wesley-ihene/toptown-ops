"""Tests for duplicate WhatsApp notice dispatch."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.outbound_reply_agent import worker as outbound_reply_worker
from packages.signal_contracts.agent_result import AgentResult
from scripts import whatsapp_webhook_bridge as bridge


def test_duplicate_report_uses_current_decision_without_exporting_duplicate_structured_output(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "live")
    specialist_calls = 0
    sent_response_types: list[str] = []
    repeated_text = "\n".join(
        [
            "DAY-END SALES REPORT",
            "Branch: Waigani",
            "Date: 07/04/2026",
            "Z Reading: 1200",
            "Cash Sales: 600",
            "Card Sales: 600",
            "Total Sales: 1200",
            "Traffic: 12",
            "Served: 9",
            "Staff On Duty: 4",
            "Cash Variance: 0",
            "Over Short Reason: Nil",
            "Supervisor Confirmed: YES",
        ]
    )
    request_one = json.dumps(_meta_payload(message_id="wamid.duplicate-send-1", text=repeated_text)).encode("utf-8")
    request_two = json.dumps(_meta_payload(message_id="wamid.duplicate-send-2", text=repeated_text)).encode("utf-8")

    def fake_dispatch_to_specialist(work_item, *, target_agent):
        nonlocal specialist_calls
        specialist_calls += 1
        return AgentResult(agent_name=target_agent, payload=_sales_payload())

    def fake_send_whatsapp_text(**kwargs):
        sent_response_types.append(str(kwargs["response_type"]))
        return {
            "dispatch_status": "sent",
            "provider_message_id": f"wamid.provider.{kwargs['response_type']}",
            "http_status": 200,
            "error": None,
        }

    monkeypatch.setattr(bridge.orchestrator_worker, "_dispatch_to_specialist", fake_dispatch_to_specialist)
    monkeypatch.setattr(outbound_reply_worker, "send_whatsapp_text", fake_send_whatsapp_text)
    caplog.set_level(logging.INFO, logger=bridge.__name__)
    caplog.set_level(logging.INFO, logger=outbound_reply_worker.__name__)

    first = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_one)
    second = bridge.dispatch_http_request(method="POST", target="/webhook", body=request_two)

    first_body = json.loads(first.body.decode("utf-8"))
    second_body = json.loads(second.body.decode("utf-8"))
    second_artifact = _read_json(Path(second_body["conversation_response"]["json_path"]))
    duplicate_raw_meta = _read_json(Path(second_body["raw_meta_path"]))

    assert first.status_code == 200
    assert second.status_code == 200
    assert specialist_calls == 2
    assert first_body["conversation_response"] is not None
    assert second_body["duplicate"] is True
    assert second_body["duplicate_reason"] == "duplicate_message"
    assert second_body["orchestrator_status"] == "accepted"
    assert second_body["conversation_response"]["response_type"] == "accepted_ack"
    assert second_body["conversation_response"]["dispatch_status"] == "sent"
    assert second_artifact["response_type"] == "accepted_ack"
    assert second_artifact["governance_status"] == "accepted"
    assert second_artifact["dispatch_status"] == "sent"
    assert second_artifact["http_status"] == 200
    assert second_artifact["provider_message_id"] == "wamid.provider.accepted_ack"
    assert second_artifact["dispatch_error"] is None
    assert not any("/structured/" in output for output in second_body["outputs"])
    assert duplicate_raw_meta["processing_status"] == "processed"
    assert duplicate_raw_meta["duplicate_handling"]["duplicate"] is True
    assert sent_response_types[-1] == "accepted_ack"
    assert sent_response_types.count("accepted_ack") == 2
    assert "outbound_send_attempted" in caplog.text
    assert "outbound_send_success" in caplog.text


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "DUPLICATES_DIR", records_dir / "duplicates" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(bridge, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(bridge, "verify_meta_signature", lambda body, signature: True)
    monkeypatch.delenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", raising=False)
    monkeypatch.delenv("TOPTOWN_ENABLE_REPLAY_RESPONSES", raising=False)


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


def _sales_payload() -> dict[str, object]:
    return {
        "status": "accepted",
        "signal_type": "sales_income",
        "source_agent": "sales_income_agent",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "confidence": 0.95,
        "metrics": {
            "gross_sales": 1200.0,
            "cash_sales": 600.0,
            "eftpos_sales": 600.0,
            "mobile_money_sales": 0.0,
            "traffic": 12,
            "served": 9,
        },
        "items": [],
        "warnings": [],
    }


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
