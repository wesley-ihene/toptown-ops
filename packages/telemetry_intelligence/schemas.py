"""Typed telemetry intelligence payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    """One compact operational event derived from runtime logs."""

    event_id: str
    event_type: str
    severity: str
    count: int
    sources: list[str]
    branches: list[str] = field(default_factory=list)
    items: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    pattern: str | None = None
    sample: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "severity": self.severity,
            "count": self.count,
            "sources": list(self.sources),
            "branches": list(self.branches),
            "items": list(self.items),
            "reason_codes": list(self.reason_codes),
        }
        if self.pattern is not None:
            payload["pattern"] = self.pattern
        if self.sample is not None:
            payload["sample"] = self.sample
        return payload


@dataclass(frozen=True, slots=True)
class VectorizeCandidate:
    """One compact lesson suitable for downstream vectorization."""

    candidate_id: str
    event_type: str
    priority: str
    lesson: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "event_type": self.event_type,
            "priority": self.priority,
            "lesson": self.lesson,
        }


@dataclass(frozen=True, slots=True)
class HighRiskTarget:
    """One compact branch/item risk rollup."""

    branch: str
    item: str | None
    occurrences: int
    event_ids: list[str]

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "branch": self.branch,
            "occurrences": self.occurrences,
            "event_ids": list(self.event_ids),
        }
        if self.item is not None:
            payload["item"] = self.item
        return payload

