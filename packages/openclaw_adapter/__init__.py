"""Minimal, config-only OpenClaw adapter for TAOP."""

from .client import send_advisory_prompt
from .health import (
    DEFAULT_OPENCLAW_GATEWAY_URL,
    OPENCLAW_ENABLED_ENV_VAR,
    OPENCLAW_GATEWAY_URL_ENV_VAR,
    check_gateway_health,
    get_gateway_url,
    is_openclaw_enabled,
)

__all__ = [
    "DEFAULT_OPENCLAW_GATEWAY_URL",
    "OPENCLAW_ENABLED_ENV_VAR",
    "OPENCLAW_GATEWAY_URL_ENV_VAR",
    "check_gateway_health",
    "get_gateway_url",
    "is_openclaw_enabled",
    "send_advisory_prompt",
]
