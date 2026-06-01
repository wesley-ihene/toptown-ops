"""Tests for context-aware correction reply generation."""

from __future__ import annotations

import json
import os
from pathlib import Path

from packages.record_store.automation import generate_whatsapp_conversation_reply


def test_correction_success_flow_uses_context_aware_reply(tmp_path: Path, monkeypatch) -> None:
    _disable_llm_observability(monkeypatch)

    first = generate_whatsapp_conversation_reply(
        outcome={
            "status": "rejected",
            "governance": {"status": "rejected", "reasons": ["validation_failed"]},
            "classification": {"report_type": "sales"},
            "branch": "waigani",
            "report_date": "2026-04-07",
        },
        source_message_id="wamid.reject-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
    )
    second = generate_whatsapp_conversation_reply(
        outcome={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
        },
        source_message_id="wamid.accept-2",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
    )

    assert first is not None
    assert second is not None
    second_payload = _read_json(Path(second["json_path"]))
    assert second["response_type"] == "correction_accepted_ack"
    if _taop_feedback_enabled():
        assert second_payload["response_text"].startswith("✅ Corrected DAY-END SALES REPORT received")
    else:
        assert second_payload["response_text"] == _TAOP_DISABLED_RESPONSE_TEXT


def test_repeat_failure_flow_uses_repeat_fix_request(tmp_path: Path, monkeypatch) -> None:
    _disable_llm_observability(monkeypatch)

    generate_whatsapp_conversation_reply(
        outcome={
            "status": "rejected",
            "governance": {"status": "rejected", "reasons": ["validation_failed"]},
            "classification": {"report_type": "sales"},
            "branch": "waigani",
            "report_date": "2026-04-07",
        },
        source_message_id="wamid.reject-1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
    )
    reply = generate_whatsapp_conversation_reply(
        outcome={
            "status": "rejected",
            "governance": {"status": "rejected", "reasons": ["validation_failed"]},
            "classification": {"report_type": "sales"},
            "branch": "waigani",
            "report_date": "2026-04-07",
        },
        source_message_id="wamid.reject-2",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
    )

    assert reply is not None
    payload = _read_json(Path(reply["json_path"]))
    assert reply["response_type"] == "correction_repeat_fix_request"
    if _taop_feedback_enabled():
        assert "missing corrections applied" in payload["response_text"]
    else:
        assert payload["response_text"] == _TAOP_DISABLED_RESPONSE_TEXT


def test_context_isolation_and_no_false_positives(tmp_path: Path, monkeypatch) -> None:
    _disable_llm_observability(monkeypatch)

    generate_whatsapp_conversation_reply(
        outcome={
            "status": "rejected",
            "governance": {"status": "rejected", "reasons": ["validation_failed"]},
            "classification": {"report_type": "sales"},
            "branch": "waigani",
            "report_date": "2026-04-07",
        },
        source_message_id="wamid.reject-a1",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
    )

    other_sender = generate_whatsapp_conversation_reply(
        outcome={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
        },
        source_message_id="wamid.accept-b1",
        sender_phone="67570000001",
        replay=False,
        source_root=tmp_path,
    )
    other_report = generate_whatsapp_conversation_reply(
        outcome={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "hr_attendance",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "outputs": ["records/structured/hr_attendance/waigani/2026-04-07.json"],
        },
        source_message_id="wamid.accept-a2",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
    )

    assert other_sender is not None
    assert other_report is not None
    assert other_sender["response_type"] == "accepted_ack"
    assert other_report["response_type"] == "accepted_ack"


def test_replay_is_context_safe_and_does_not_mutate_live_context(tmp_path: Path, monkeypatch) -> None:
    _disable_llm_observability(monkeypatch)

    generate_whatsapp_conversation_reply(
        outcome={
            "status": "rejected",
            "governance": {"status": "rejected", "reasons": ["validation_failed"]},
            "classification": {"report_type": "sales"},
            "branch": "waigani",
            "report_date": "2026-04-07",
        },
        source_message_id="wamid.reject-live",
        sender_phone="67570000000",
        replay=False,
        source_root=tmp_path,
    )
    context_path = tmp_path / "records" / "context" / "whatsapp" / "67570000000.json"
    before = context_path.read_text(encoding="utf-8")

    replay_reply = generate_whatsapp_conversation_reply(
        outcome={
            "status": "accepted",
            "governance": {"status": "accepted", "reasons": []},
            "signal_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "outputs": ["records/structured/sales_income/waigani/2026-04-07.json"],
            "replay": {"is_replay": True, "source": "raw"},
        },
        source_message_id="wamid.replay-1",
        sender_phone="67570000000",
        replay=True,
        source_root=tmp_path,
    )

    assert replay_reply is not None
    assert replay_reply["response_type"] == "accepted_ack"
    assert replay_reply["dispatch_status"] == "suppressed"
    assert context_path.read_text(encoding="utf-8") == before


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _disable_llm_observability(monkeypatch) -> None:
    import apps.response_engine.worker as response_worker

    monkeypatch.setattr(response_worker, "record_conversation_llm_event", lambda **kwargs: None)
    response_worker._LLM_REWRITE_CACHE.clear()


_TAOP_DISABLED_RESPONSE_TEXT = "[TAOP DISABLED - AGENT OUTPUT ONLY]"


def _taop_feedback_enabled() -> bool:
    return os.getenv("TAOP_FEEDBACK_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
