"""Tests for outbound WhatsApp reply dispatch."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from apps.outbound_reply_agent import worker as outbound_reply_worker
from packages.response_store import load_response_artifact, write_response_artifacts
from packages.signal_contracts.work_item import WorkItem


@pytest.mark.parametrize(
    ("response_type", "governance_status"),
    [
        ("review_ack", "needs_review"),
        ("success_ack", "accepted"),
        ("error_ack", "rejected"),
    ],
)
def test_outbound_reply_agent_sends_supported_ack_types(
    tmp_path: Path,
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
    response_type: str,
    governance_status: str,
) -> None:
    monkeypatch.setattr(outbound_reply_worker, "REPO_ROOT", tmp_path)
    artifact = _seed_response_artifact(
        tmp_path,
        response_type=response_type,
        governance_status=governance_status,
        source_message_id=f"wamid.{response_type}.1",
    )
    sent_payload: dict[str, object] = {}

    def fake_send_whatsapp_text(**kwargs):
        sent_payload.update(kwargs)
        return {
            "dispatch_status": "sent",
            "provider_message_id": f"wamid.provider.{response_type}",
            "http_status": 200,
            "error": None,
        }

    monkeypatch.setattr(outbound_reply_worker, "send_whatsapp_text", fake_send_whatsapp_text)
    caplog.set_level(logging.INFO, logger=outbound_reply_worker.__name__)

    result = outbound_reply_worker.process_work_item(
        WorkItem(
            kind="outbound_reply",
            payload={
                "sender_phone": "67570000000",
                "response_text": f"{response_type} body",
                "response_type": response_type,
                "source_message_id": f"wamid.{response_type}.1",
                "response_id": artifact["response_id"],
            },
        )
    )

    persisted = load_response_artifact(artifact["response_id"], output_root=tmp_path)

    assert result.payload["status"] == "sent"
    assert result.payload["dispatch_status"] == "sent"
    assert sent_payload["response_type"] == response_type
    assert sent_payload["source_message_id"] == f"wamid.{response_type}.1"
    assert persisted is not None
    assert persisted["payload"]["dispatch_status"] == "sent"
    assert persisted["payload"]["provider_message_id"] == f"wamid.provider.{response_type}"
    assert persisted["payload"]["http_status"] == 200
    assert persisted["payload"]["dispatch_error"] is None
    assert persisted["payload"]["dispatched_at"] is not None
    assert "outbound_send_attempted" in caplog.text
    assert "outbound_send_success" in caplog.text


def test_outbound_reply_agent_marks_failed_dispatch(
    tmp_path: Path,
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(outbound_reply_worker, "REPO_ROOT", tmp_path)
    artifact = _seed_response_artifact(
        tmp_path,
        response_type="review_ack",
        governance_status="needs_review",
        source_message_id="wamid.failed-review.1",
    )

    def fake_send_whatsapp_text(**kwargs):
        del kwargs
        return {
            "dispatch_status": "failed",
            "provider_message_id": None,
            "http_status": 503,
            "error": "provider_unavailable",
        }

    monkeypatch.setattr(outbound_reply_worker, "send_whatsapp_text", fake_send_whatsapp_text)
    caplog.set_level(logging.INFO, logger=outbound_reply_worker.__name__)

    result = outbound_reply_worker.process_work_item(
        WorkItem(
            kind="outbound_reply",
            payload={
                "sender_phone": "67570000000",
                "response_text": "review body",
                "response_type": "review_ack",
                "source_message_id": "wamid.failed-review.1",
                "response_id": artifact["response_id"],
            },
        )
    )

    persisted = load_response_artifact(artifact["response_id"], output_root=tmp_path)

    assert result.payload["status"] == "failed"
    assert result.payload["dispatch_status"] == "failed"
    assert persisted is not None
    assert persisted["payload"]["dispatch_status"] == "failed"
    assert persisted["payload"]["provider_message_id"] is None
    assert persisted["payload"]["dispatch_error"] == "provider_unavailable"
    assert persisted["payload"]["http_status"] == 503
    assert persisted["payload"]["dispatched_at"] is not None
    assert "outbound_send_failed" in caplog.text


def test_outbound_reply_agent_stores_provider_message_id_on_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(outbound_reply_worker, "REPO_ROOT", tmp_path)
    artifact = _seed_response_artifact(
        tmp_path,
        response_type="success_ack",
        governance_status="accepted",
        source_message_id="wamid.success-provider.1",
    )

    def fake_send_whatsapp_text(**kwargs):
        del kwargs
        return {
            "dispatch_status": "sent",
            "provider_message_id": "wamid.provider.success-1",
            "http_status": 200,
            "error": None,
        }

    monkeypatch.setattr(outbound_reply_worker, "send_whatsapp_text", fake_send_whatsapp_text)

    result = outbound_reply_worker.process_work_item(
        WorkItem(
            kind="outbound_reply",
            payload={
                "sender_phone": "67570000000",
                "response_text": "success body",
                "response_type": "success_ack",
                "source_message_id": "wamid.success-provider.1",
                "response_id": artifact["response_id"],
            },
        )
    )

    persisted = load_response_artifact(artifact["response_id"], output_root=tmp_path)

    assert result.payload["dispatch_status"] == "sent"
    assert result.payload["provider_message_id"] == "wamid.provider.success-1"
    assert persisted is not None
    assert persisted["payload"]["provider_message_id"] == "wamid.provider.success-1"


def _seed_response_artifact(
    root: Path,
    *,
    response_type: str,
    governance_status: str,
    source_message_id: str,
) -> dict[str, object]:
    return write_response_artifacts(
        {
            "source_message_id": source_message_id,
            "sender_phone": "67570000000",
            "response_type": response_type,
            "governance_status": governance_status,
            "report_type": "sales_income",
            "branch": "waigani",
            "report_date": "2026-04-07",
            "reason": None,
            "generated_at": "2026-04-07T12:00:00Z",
            "response_text": f"{response_type} body",
            "dispatch_status": "generated",
            "provider_message_id": None,
            "dispatch_error": None,
            "http_status": None,
        },
        output_root=root,
    )
