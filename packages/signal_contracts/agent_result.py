"""Typed placeholder for specialist agent results."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AgentResult:
    """Minimal contract for specialist agent outputs."""

    agent_name: str
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
