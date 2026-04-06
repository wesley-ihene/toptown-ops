"""Typed placeholder for upstream work items."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class WorkItem:
    """Minimal contract for work emitted into the orchestration flow."""

    kind: str
    payload: dict[str, Any] = field(default_factory=dict)
