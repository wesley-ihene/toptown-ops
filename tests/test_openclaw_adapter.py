"""Tests for the minimal TAOP OpenClaw adapter layer."""

from __future__ import annotations

from packages.openclaw_adapter import (
    DEFAULT_OPENCLAW_GATEWAY_URL,
    check_gateway_health,
    generate_advisory,
    get_advisory_status,
    get_gateway_url,
    get_runtime_status,
    is_openclaw_enabled,
    send_advisory_prompt,
)


def test_gateway_health_reports_disabled_config(monkeypatch) -> None:
    monkeypatch.setenv("TAOP_OPENCLAW_ENABLED", "0")
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "ws://127.0.0.1:19999")

    payload = check_gateway_health()

    assert is_openclaw_enabled() is False
    assert payload == {
        "enabled": False,
        "gateway_url": "ws://127.0.0.1:19999",
        "status": "disabled",
        "reason": "TAOP_OPENCLAW_ENABLED is disabled",
    }


def test_gateway_health_reports_enabled_config(monkeypatch) -> None:
    monkeypatch.setenv("TAOP_OPENCLAW_ENABLED", "1")
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "ws://gateway.internal:18789")

    payload = check_gateway_health()

    assert is_openclaw_enabled() is True
    assert payload == {
        "enabled": True,
        "gateway_url": "ws://gateway.internal:18789",
        "status": "configured",
        "reason": "OpenClaw gateway configured; connectivity check not implemented",
    }


def test_runtime_status_reports_disabled_config(monkeypatch) -> None:
    monkeypatch.setenv("TAOP_OPENCLAW_ENABLED", "0")
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "ws://127.0.0.1:19999")

    payload = get_runtime_status()

    assert payload == {
        "openclaw": {
            "enabled": False,
            "gateway_url": "ws://127.0.0.1:19999",
            "status": "disabled",
            "reason": "TAOP_OPENCLAW_ENABLED is disabled",
        }
    }


def test_runtime_status_reports_enabled_config(monkeypatch) -> None:
    monkeypatch.setenv("TAOP_OPENCLAW_ENABLED", "1")
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "ws://gateway.internal:18789")

    payload = get_runtime_status()

    assert payload == {
        "openclaw": {
            "enabled": True,
            "gateway_url": "ws://gateway.internal:18789",
            "status": "configured",
            "reason": "OpenClaw gateway configured; connectivity check not implemented",
        }
    }


def test_get_gateway_url_returns_default_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("OPENCLAW_GATEWAY_URL", raising=False)

    assert get_gateway_url() == DEFAULT_OPENCLAW_GATEWAY_URL


def test_send_advisory_prompt_returns_disabled_status(monkeypatch) -> None:
    monkeypatch.setenv("TAOP_OPENCLAW_ENABLED", "0")
    monkeypatch.delenv("OPENCLAW_GATEWAY_URL", raising=False)

    payload = send_advisory_prompt("Check branch staffing risk.")

    assert payload == {
        "enabled": False,
        "gateway_url": DEFAULT_OPENCLAW_GATEWAY_URL,
        "status": "disabled",
        "reason": "TAOP OpenClaw integration is disabled",
        "prompt": "Check branch staffing risk.",
    }


def test_get_advisory_status_reports_sandbox_only_mode(monkeypatch) -> None:
    monkeypatch.setenv("TAOP_OPENCLAW_ENABLED", "0")
    monkeypatch.setenv("OPENCLAW_GATEWAY_URL", "ws://127.0.0.1:19999")

    payload = get_advisory_status()

    assert payload == {
        "enabled": False,
        "sandbox_only": True,
        "gateway_url": "ws://127.0.0.1:19999",
        "status": "sandbox_only",
        "reason": "OpenClaw advisory sandbox only; no gateway calls or external prompt delivery",
        "mock_output": True,
    }


def test_generate_advisory_returns_mock_sandbox_content(monkeypatch) -> None:
    monkeypatch.delenv("OPENCLAW_GATEWAY_URL", raising=False)

    payload = generate_advisory(
        "Branch Risk Check",
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
        },
    )

    assert payload["title"] == "Branch Risk Check"
    assert payload["context"] == "branch=waigani, report_date=2026-04-07"
    assert payload["status"]["status"] == "sandbox_only"
    assert payload["status"]["sandbox_only"] is True
    assert payload["status"]["gateway_url"] == DEFAULT_OPENCLAW_GATEWAY_URL
    assert payload["advisory"]["headline"] == "Sandbox advisory: Branch Risk Check"
    assert payload["advisory"]["summary"] == "Mock advisory only for Branch Risk Check. Human review remains required before any action."
    assert payload["advisory"]["signals"] == [
        "Sandbox-only advisory response generated locally.",
        "No OpenClaw gateway call was performed.",
        "No prompts were sent outside TAOP.",
    ]
    assert payload["advisory"]["recommended_action"] == "No automatic action. Escalate through normal governance if review is needed."
