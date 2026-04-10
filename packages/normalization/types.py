"""Typed contracts for shared report normalization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class AppliedRule:
    """One explicit normalization rule application."""

    name: str
    raw_value: str | None = None
    normalized_value: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe representation."""

        return {
            "name": self.name,
            "raw_value": self.raw_value,
            "normalized_value": self.normalized_value,
            "details": dict(self.details),
        }


@dataclass(slots=True)
class NormalizedValue:
    """One normalized scalar plus diagnostics."""

    raw_value: str | None
    normalized_value: str | None = None
    confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)
    hard_errors: list[str] = field(default_factory=list)
    applied_rules: list[AppliedRule] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        """Return whether a normalized value was recovered."""

        return self.normalized_value is not None and not self.hard_errors

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe representation."""

        return {
            "raw_value": self.raw_value,
            "normalized_value": self.normalized_value,
            "confidence": self.confidence,
            "warnings": list(self.warnings),
            "hard_errors": list(self.hard_errors),
            "applied_rules": [rule.to_payload() for rule in self.applied_rules],
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class NormalizationResult:
    """Report-level normalization output."""

    report_family: str | None = None
    report_type: str | None = None
    normalized_text: str | None = None
    normalized_fields: dict[str, Any] = field(default_factory=dict)
    branch: NormalizedValue | None = None
    report_date: NormalizedValue | None = None
    label_map: dict[str, NormalizedValue] = field(default_factory=dict)
    numeric_fields: dict[str, NormalizedValue] = field(default_factory=dict)
    money_fields: dict[str, NormalizedValue] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    hard_errors: list[str] = field(default_factory=list)
    provenance: list[AppliedRule] = field(default_factory=list)
    confidence_summary: dict[str, float] = field(default_factory=dict)
    raw_snapshots: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe representation."""

        return {
            "report_family": self.report_family,
            "report_type": self.report_type,
            "normalized_text": self.normalized_text,
            "normalized_fields": dict(self.normalized_fields),
            "branch": self.branch.to_payload() if self.branch is not None else None,
            "report_date": self.report_date.to_payload() if self.report_date is not None else None,
            "label_map": {key: value.to_payload() for key, value in self.label_map.items()},
            "numeric_fields": {key: value.to_payload() for key, value in self.numeric_fields.items()},
            "money_fields": {key: value.to_payload() for key, value in self.money_fields.items()},
            "warnings": list(self.warnings),
            "hard_errors": list(self.hard_errors),
            "provenance": [rule.to_payload() for rule in self.provenance],
            "confidence_summary": dict(self.confidence_summary),
            "raw_snapshots": dict(self.raw_snapshots),
        }
