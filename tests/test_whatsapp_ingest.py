"""Tests for the WhatsApp ingest helper module and CLI."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from packages.whatsapp_ingest import ingest as ingest_module
from scripts import ingest_whatsapp_reports
from scripts.whatsapp_webhook_bridge import BridgeHttpResponse


def test_ingest_payload_dispatches_through_bridge(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_dispatch_http_request(*, method: str, target: str, body: bytes, headers):
        captured["method"] = method
        captured["target"] = target
        captured["body"] = json.loads(body.decode("utf-8"))
        captured["headers"] = dict(headers)
        return BridgeHttpResponse(
            status_code=200,
            body=json.dumps({"ok": True, "agent": "orchestrator_agent"}).encode("utf-8"),
        )

    monkeypatch.setattr(ingest_module, "dispatch_http_request", fake_dispatch_http_request)

    result = ingest_module.ingest_payload(
        {"payload": {"text": "DAY-END SALES REPORT"}},
        headers={"X-Test": "1"},
        route="/webhooks/whatsapp",
    )

    assert result == {
        "status_code": 200,
        "body": {
            "ok": True,
            "agent": "orchestrator_agent",
        },
    }
    assert captured == {
        "method": "POST",
        "target": "/webhooks/whatsapp",
        "body": {"payload": {"text": "DAY-END SALES REPORT"}},
        "headers": {"X-Test": "1"},
    }


def test_ingest_payload_file_requires_json_object(tmp_path: Path) -> None:
    payload_file = tmp_path / "payload.json"
    payload_file.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        ingest_module.ingest_payload_file(payload_file)


def test_ingest_cli_reads_stdin_and_returns_success(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_ingest_payload(payload, *, headers, route):
        captured["payload"] = payload
        captured["headers"] = dict(headers)
        captured["route"] = route
        return {
            "status_code": 200,
            "body": {"ok": True, "agent": "orchestrator_agent"},
        }

    monkeypatch.setattr(ingest_whatsapp_reports, "ingest_payload", fake_ingest_payload)
    monkeypatch.setattr(
        ingest_whatsapp_reports.sys,
        "stdin",
        io.StringIO(json.dumps({"payload": {"text": "DAY-END SALES REPORT"}})),
    )

    exit_code = ingest_whatsapp_reports.main(
        ["--route", "/webhooks/whatsapp", "--header", "X-Test:1"]
    )

    assert exit_code == 0
    assert captured == {
        "payload": {"payload": {"text": "DAY-END SALES REPORT"}},
        "headers": {"X-Test": "1"},
        "route": "/webhooks/whatsapp",
    }
    assert json.loads(capsys.readouterr().out) == {
        "status_code": 200,
        "body": {"ok": True, "agent": "orchestrator_agent"},
    }
