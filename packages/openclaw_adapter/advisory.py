"""Sandbox-only advisory helpers for the TAOP OpenClaw adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .health import get_gateway_url
from .schemas import AdvisorySandboxPayload, AdvisorySandboxStatusPayload

_SANDBOX_REASON = "OpenClaw advisory sandbox only; no gateway calls or external prompt delivery"


def get_advisory_status() -> AdvisorySandboxStatusPayload:
    """Return metadata-only status for the local advisory sandbox."""

    return {
        "enabled": False,
        "sandbox_only": True,
        "gateway_url": get_gateway_url(),
        "status": "sandbox_only",
        "reason": _SANDBOX_REASON,
        "mock_output": True,
    }


def generate_advisory(title: str, context: Any) -> AdvisorySandboxPayload:
    """Return deterministic mock advisory content without contacting OpenClaw."""

    clean_title = title.strip() or "Untitled Advisory"
    context_text = _stringify_context(context)
    status = get_advisory_status()
    return {
        "title": clean_title,
        "context": context_text,
        "status": status,
        "advisory": {
            "headline": f"Sandbox advisory: {clean_title}",
            "summary": f"Mock advisory only for {clean_title}. Human review remains required before any action.",
            "signals": [
                "Sandbox-only advisory response generated locally.",
                "No OpenClaw gateway call was performed.",
                "No prompts were sent outside TAOP.",
            ],
            "operator_note": "Use this output as a non-binding draft only.",
            "recommended_action": "No automatic action. Escalate through normal governance if review is needed.",
            "context_excerpt": context_text,
        },
    }


def _stringify_context(context: Any) -> str:
    if isinstance(context, str):
        cleaned = context.strip()
        return cleaned or "No context provided."
    if isinstance(context, Mapping):
        parts = [f"{key}={value}" for key, value in context.items()]
        return ", ".join(parts) if parts else "No context provided."
    if context is None:
        return "No context provided."
    return str(context)
