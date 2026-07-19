"""Typed schemas for the minimal OpenClaw adapter layer."""

from __future__ import annotations

from typing import Literal, TypedDict


HealthStatus = Literal["disabled", "configured", "unavailable"]
AdvisoryStatus = Literal["disabled", "not_implemented"]
AdvisorySandboxStatus = Literal["sandbox_only"]


class GatewayHealthPayload(TypedDict):
    """Structured health payload for OpenClaw gateway configuration state."""

    enabled: bool
    gateway_url: str
    status: HealthStatus
    reason: str


class RuntimeStatusPayload(TypedDict):
    """Structured TAOP runtime visibility payload for OpenClaw state."""

    openclaw: GatewayHealthPayload


class AdvisoryPromptPayload(TypedDict):
    """Structured placeholder response for advisory prompt requests."""

    enabled: bool
    gateway_url: str
    status: AdvisoryStatus
    reason: str
    prompt: str


class AdvisorySandboxStatusPayload(TypedDict):
    """Structured status payload for sandbox-only advisory visibility."""

    enabled: bool
    sandbox_only: bool
    gateway_url: str
    status: AdvisorySandboxStatus
    reason: str
    mock_output: bool


class AdvisorySandboxPayload(TypedDict):
    """Structured sandbox advisory payload with mock-only content."""

    title: str
    context: str
    status: AdvisorySandboxStatusPayload
    advisory: dict[str, object]
