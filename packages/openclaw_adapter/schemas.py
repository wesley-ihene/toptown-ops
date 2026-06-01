"""Typed schemas for the minimal OpenClaw adapter layer."""

from __future__ import annotations

from typing import Literal, TypedDict


HealthStatus = Literal["disabled", "configured", "unavailable"]
AdvisoryStatus = Literal["disabled", "not_implemented"]


class GatewayHealthPayload(TypedDict):
    """Structured health payload for OpenClaw gateway configuration state."""

    enabled: bool
    gateway_url: str
    status: HealthStatus
    reason: str


class AdvisoryPromptPayload(TypedDict):
    """Structured placeholder response for advisory prompt requests."""

    enabled: bool
    gateway_url: str
    status: AdvisoryStatus
    reason: str
    prompt: str
