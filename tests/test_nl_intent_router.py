"""Tests for controlled natural-language intent routing."""

from __future__ import annotations

import json
from pathlib import Path

from packages.observability import load_daily_artifact
import packages.record_store.paths as record_paths
from packages.signal_contracts.agent_result import AgentResult
from apps.nl_intent_router import worker
from scripts import whatsapp_webhook_bridge as bridge


def test_normalize_text_applies_basic_tok_pisin_and_whitespace_rules() -> None:
    assert worker.normalize_text("  WHY em reject  ") == "why rejected"
    assert worker.normalize_text(" format bilong attendance ") == "format attendance"


def test_what_is_wrong_with_my_report_maps_to_why_rejected(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"why_rejected","confidence":0.93}')

    result = worker.classify_intent("what is wrong with my report", is_replay=False)

    assert result == {"intent": "why_rejected", "confidence": 0.93, "valid": True}


def test_did_my_report_go_through_maps_to_status(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"status","confidence":0.92}')

    result = worker.classify_intent("did my report go through", is_replay=False)

    assert result == {"intent": "status", "confidence": 0.92, "valid": True}


def test_show_attendance_format_maps_to_format_attendance(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"format_attendance","confidence":0.94}')

    result = worker.classify_intent("show attendance format", is_replay=False)

    assert result == {"intent": "format_attendance", "confidence": 0.94, "valid": True}


def test_help_me_maps_to_help(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"help","confidence":0.91}')

    result = worker.classify_intent("help me", is_replay=False)

    assert result == {"intent": "help", "confidence": 0.91, "valid": True}


def test_random_text_is_invalid(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"why_rejected","confidence":0.99}')

    result = worker.classify_intent("team lunch at 2pm", is_replay=False)

    assert result == {"intent": "why_rejected", "confidence": 0.99, "valid": False}


def test_low_confidence_is_invalid(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"status","confidence":0.50}')

    result = worker.classify_intent("did my report go through", is_replay=False)

    assert result == {"intent": "status", "confidence": 0.5, "valid": False}


def test_unsupported_intent_is_invalid(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"format_bale_summary","confidence":0.99}')

    result = worker.classify_intent("show bale format", is_replay=False)

    assert result == {"intent": "format_bale_summary", "confidence": 0.99, "valid": False}


def test_replay_skips_llm(monkeypatch) -> None:
    calls = 0

    def should_not_run(normalized_message: str) -> str | None:
        nonlocal calls
        calls += 1
        return '{"intent":"help","confidence":0.99}'

    monkeypatch.setattr(worker, "classify_user_intent_json", should_not_run)
    worker._CLASSIFICATION_CACHE.clear()

    result = worker.classify_intent("help me", is_replay=True)

    assert result == {"intent": None, "confidence": 0.0, "valid": False}
    assert calls == 0


def test_invalid_json_falls_back_safely(monkeypatch) -> None:
    _stub_llm(monkeypatch, "not-json")

    result = worker.classify_intent("help me", is_replay=False)

    assert result == {"intent": None, "confidence": 0.0, "valid": False}


def test_ambiguous_text_is_rejected(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"help","confidence":0.99}')

    result = worker.classify_intent("help with attendance format", is_replay=False)

    assert result == {"intent": None, "confidence": 0.0, "valid": False}


def test_short_text_is_rejected(monkeypatch) -> None:
    _stub_llm(monkeypatch, '{"intent":"help","confidence":0.99}')

    result = worker.classify_intent("help", is_replay=False)

    assert result == {"intent": None, "confidence": 0.0, "valid": False}


def test_bridge_maps_nl_help_to_existing_command_and_records_observability(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def should_not_run(*args, **kwargs):
        raise AssertionError("report pipeline should not run for NL command mapping")

    monkeypatch.setattr(bridge, "validate_inbound_text", should_not_run)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", should_not_run)
    _stub_llm(monkeypatch, '{"intent":"help","confidence":0.95}')

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.nl.help", text="help me")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact = load_daily_artifact("nl_intent", "2026-04-07", output_root=tmp_path)

    assert response.status_code == 200
    assert body["command"] is True
    assert body["command_name"] == "help"
    assert body["agent"] == "command_handler"
    assert artifact is not None
    assert artifact["summary"]["nl_intent_calls"] == 1
    assert artifact["summary"]["nl_intent_success"] == 1


def test_bridge_invalid_nl_falls_through_to_existing_unknown_guidance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    _stub_llm(monkeypatch, '{"intent":"status","confidence":0.99}')

    def fake_validate_inbound_text(text, *, payload_kind="text", metadata=None):
        del payload_kind, metadata
        return {
            "status": "accepted",
            "cleaned_text": text,
            "reasons": [],
            "warnings": [],
            "detected_risks": [],
            "suggested_report_family": None,
            "validator_version": "v1",
        }

    def fake_process_work_item(work_item):
        return AgentResult(
            agent_name="orchestrator_agent",
            payload={
                "status": "needs_review",
                "governance": {"status": "needs_review", "reasons": ["unknown_report_type"]},
                "classification": {"report_type": "unknown"},
                "routing": {"classification": "unknown"},
            },
        )

    monkeypatch.setattr(bridge, "validate_inbound_text", fake_validate_inbound_text)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", fake_process_work_item)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.nl.invalid", text="team lunch at 2pm")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact = load_daily_artifact("nl_intent", "2026-04-07", output_root=tmp_path)

    assert response.status_code == 200
    assert body.get("command") is not True
    assert body["conversation_response"]["response_type"] == "unknown_message_guidance"
    assert artifact is not None
    assert artifact["summary"]["nl_intent_calls"] == 1
    assert artifact["summary"]["nl_intent_mismatch"] == 1


def _stub_llm(monkeypatch, raw_response: str | None) -> None:
    def fake_classify(normalized_message: str) -> str | None:
        del normalized_message
        return raw_response

    monkeypatch.setattr(worker, "classify_user_intent_json", fake_classify)
    worker._CLASSIFICATION_CACHE.clear()


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
