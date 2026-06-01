"""Minimal health and configuration helpers for OpenClaw integration."""

from __future__ import annotations

import os

from .schemas import GatewayHealthPayload

OPENCLAW_ENABLED_ENV_VAR = "TAOP_OPENCLAW_ENABLED"
OPENCLAW_GATEWAY_URL_ENV_VAR = "OPENCLAW_GATEWAY_URL"
DEFAULT_OPENCLAW_GATEWAY_URL = "ws://127.0.0.1:18789"
_TRUE_VALUES = {"1", "true", "yes", "on"}


def is_openclaw_enabled() -> bool:
    """Return whether TAOP OpenClaw integration is explicitly enabled."""

    value = os.environ.get(OPENCLAW_ENABLED_ENV_VAR, "")
    return value.strip().lower() in _TRUE_VALUES


def get_gateway_url() -> str:
    """Return the configured OpenClaw gateway URL or the local default."""

    value = os.environ.get(OPENCLAW_GATEWAY_URL_ENV_VAR, "")
    cleaned = value.strip()
    if cleaned:
        return cleaned
    return DEFAULT_OPENCLAW_GATEWAY_URL


def check_gateway_health() -> GatewayHealthPayload:
    """Report config-only health without calling the gateway."""

    enabled = is_openclaw_enabled()
    gateway_url = get_gateway_url()
    if not enabled:
        return {
            "enabled": False,
            "gateway_url": gateway_url,
            "status": "disabled",
            "reason": f"{OPENCLAW_ENABLED_ENV_VAR} is disabled",
        }

    return {
        "enabled": True,
        "gateway_url": gateway_url,
        "status": "configured",
        "reason": "OpenClaw gateway configured; connectivity check not implemented",
    }
