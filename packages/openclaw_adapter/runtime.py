"""Runtime visibility helpers for TAOP's OpenClaw adapter state."""

from __future__ import annotations

from .health import check_gateway_health
from .schemas import RuntimeStatusPayload


def get_runtime_status() -> RuntimeStatusPayload:
    """Return config-only runtime visibility for the OpenClaw adapter."""

    return {
        "openclaw": check_gateway_health(),
    }
