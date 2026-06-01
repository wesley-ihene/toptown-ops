"""Placeholder client entrypoints for future OpenClaw integration."""

from __future__ import annotations

from .health import get_gateway_url, is_openclaw_enabled
from .schemas import AdvisoryPromptPayload


def send_advisory_prompt(prompt: str) -> AdvisoryPromptPayload:
    """Return placeholder adapter status without sending anything downstream."""

    gateway_url = get_gateway_url()
    if not is_openclaw_enabled():
        return {
            "enabled": False,
            "gateway_url": gateway_url,
            "status": "disabled",
            "reason": "TAOP OpenClaw integration is disabled",
            "prompt": prompt,
        }

    return {
        "enabled": True,
        "gateway_url": gateway_url,
        "status": "not_implemented",
        "reason": "OpenClaw advisory prompt sending is not implemented",
        "prompt": prompt,
    }
