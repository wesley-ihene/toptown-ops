"""Tests for controlled outbound WhatsApp sending."""

from __future__ import annotations

import json
from pathlib import Path
from urllib import error

from packages.response_store import write_response_artifacts
from packages.whatsapp_outbound import sender as outbound_sender


def test_outbound_off_mode_suppresses_without_http_call(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "off")
    http_calls = 0

    def fake_urlopen(*args, **kwargs):
        nonlocal http_calls
        http_calls += 1
        raise AssertionError("urlopen should not be called in off mode")

    monkeypatch.setattr(outbound_sender.request, "urlopen", fake_urlopen)

    result = outbound_sender.send_whatsapp_text(
        to="67570000000",
        body="Accepted.",
        source_message_id="wamid.outbound-off-1",
        response_id="response-off-1",
        response_type="accepted_ack",
        is_replay=False,
    )

    assert result["dispatch_status"] == "suppressed"
    assert http_calls == 0


def test_outbound_dry_run_mode_returns_dry_run_without_http_call(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "dry_run")
    http_calls = 0

    def fake_urlopen(*args, **kwargs):
        nonlocal http_calls
        http_calls += 1
        raise AssertionError("urlopen should not be called in dry_run mode")

    monkeypatch.setattr(outbound_sender.request, "urlopen", fake_urlopen)

    result = outbound_sender.send_whatsapp_text(
        to="67570000000",
        body="Accepted.",
        source_message_id="wamid.outbound-dry-run-1",
        response_id="response-dry-run-1",
        response_type="accepted_ack",
        is_replay=False,
    )

    assert result["dispatch_status"] == "dry_run"
    assert http_calls == 0


def test_outbound_live_mode_suppresses_replay(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "live")
    monkeypatch.setenv("WHATSAPP_OUTBOUND_ALLOWLIST", "67570000000")
    http_calls = 0

    def fake_urlopen(*args, **kwargs):
        nonlocal http_calls
        http_calls += 1
        raise AssertionError("urlopen should not be called for replay")

    monkeypatch.setattr(outbound_sender.request, "urlopen", fake_urlopen)

    result = outbound_sender.send_whatsapp_text(
        to="67570000000",
        body="Accepted.",
        source_message_id="wamid.outbound-replay-1",
        response_id="response-replay-1",
        response_type="accepted_ack",
        is_replay=True,
    )

    assert result["dispatch_status"] == "suppressed"
    assert http_calls == 0


def test_outbound_live_mode_suppresses_non_allowlisted_recipient(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "live")
    monkeypatch.setenv("WHATSAPP_OUTBOUND_ALLOWLIST", "67579999999")

    result = outbound_sender.send_whatsapp_text(
        to="67570000000",
        body="Accepted.",
        source_message_id="wamid.outbound-allowlist-1",
        response_id="response-allowlist-1",
        response_type="accepted_ack",
        is_replay=False,
    )

    assert result == {
        "dispatch_status": "suppressed",
        "provider_message_id": None,
        "error": "recipient_not_allowlisted",
        "http_status": None,
    }


def test_outbound_live_mode_sends_text_message(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "live")
    monkeypatch.setenv("WHATSAPP_GRAPH_API_VERSION", "v25.0")
    monkeypatch.setenv("WHATSAPP_PHONE_NUMBER_ID", "pnid-123")
    monkeypatch.setenv("WHATSAPP_ACCESS_TOKEN", "token-123")
    monkeypatch.setenv("WHATSAPP_OUTBOUND_ALLOWLIST", "67570000000")
    seen: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return json.dumps({"messages": [{"id": "wamid.outbound-provider-1"}]}).encode("utf-8")

        def getcode(self) -> int:
            return 200

    def fake_urlopen(req, timeout):
        seen["url"] = req.full_url
        seen["timeout"] = timeout
        seen["headers"] = dict(req.header_items())
        seen["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr(outbound_sender.request, "urlopen", fake_urlopen)

    result = outbound_sender.send_whatsapp_text(
        to="67570000000",
        body="Accepted.",
        source_message_id="wamid.outbound-live-1",
        response_id="response-live-1",
        response_type="accepted_ack",
        is_replay=False,
    )

    assert result == {
        "dispatch_status": "sent",
        "provider_message_id": "wamid.outbound-provider-1",
        "error": None,
        "http_status": 200,
    }
    assert seen["url"] == "https://graph.facebook.com/v25.0/pnid-123/messages"
    assert seen["timeout"] == 5
    assert seen["headers"]["Authorization"] == "Bearer token-123"
    assert seen["headers"]["Content-type"] == "application/json"
    assert seen["body"] == {
        "messaging_product": "whatsapp",
        "to": "67570000000",
        "type": "text",
        "text": {"preview_url": False, "body": "Accepted."},
    }


def test_outbound_live_mode_returns_failed_on_http_error(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "live")
    monkeypatch.setenv("WHATSAPP_PHONE_NUMBER_ID", "pnid-123")
    monkeypatch.setenv("WHATSAPP_ACCESS_TOKEN", "token-123")
    monkeypatch.setenv("WHATSAPP_OUTBOUND_ALLOWLIST", "67570000000")

    def fake_urlopen(req, timeout):
        raise error.HTTPError(
            req.full_url,
            401,
            "Unauthorized",
            hdrs=None,
            fp=_ErrorBody('{"error":{"message":"bad token"}}'),
        )

    monkeypatch.setattr(outbound_sender.request, "urlopen", fake_urlopen)

    result = outbound_sender.send_whatsapp_text(
        to="67570000000",
        body="Accepted.",
        source_message_id="wamid.outbound-http-error-1",
        response_id="response-http-error-1",
        response_type="accepted_ack",
        is_replay=False,
    )

    assert result["dispatch_status"] == "failed"
    assert result["http_status"] == 401
    assert "bad token" in str(result["error"])


def test_outbound_duplicate_detection_uses_sent_response_artifact(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    write_response_artifacts(
        {
            "source_message_id": "wamid.duplicate-1",
            "sender_phone": "67570000000",
            "response_type": "accepted_ack",
            "governance_status": "accepted",
            "report_type": "sales_income",
            "branch": "waigani",
            "reason": None,
            "generated_at": "2026-04-07T12:00:00Z",
            "response_text": "Accepted.",
            "dispatch_status": "sent",
            "provider_message_id": "wamid.provider-existing-1",
            "http_status": 200,
        },
        output_root=tmp_path,
    )

    result = outbound_sender.send_whatsapp_text(
        to="67570000000",
        body="Accepted.",
        source_message_id="wamid.duplicate-1",
        response_id="d128041f8680e48e58ff8860",
        response_type="accepted_ack",
        is_replay=False,
    )

    assert result == {
        "dispatch_status": "duplicate",
        "provider_message_id": "wamid.provider-existing-1",
        "error": None,
        "http_status": 200,
    }


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(outbound_sender, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("WHATSAPP_OUTBOUND_MODE", raising=False)
    monkeypatch.delenv("WHATSAPP_GRAPH_API_VERSION", raising=False)
    monkeypatch.delenv("WHATSAPP_PHONE_NUMBER_ID", raising=False)
    monkeypatch.delenv("WHATSAPP_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("WHATSAPP_OUTBOUND_ALLOWLIST", raising=False)


class _ErrorBody:
    def __init__(self, text: str) -> None:
        self._text = text

    def read(self) -> bytes:
        return self._text.encode("utf-8")

    def close(self) -> None:
        return None
