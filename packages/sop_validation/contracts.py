"""Contracts for deterministic SOP validation results."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class Rejection:
    """One explicit rejection raised by a validator."""

    code: str
    message: str
    field: str | None = None

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-safe rejection payload."""

        payload = {
            "code": self.code,
            "message": self.message,
        }
        if self.field is not None:
            payload["field"] = self.field
        return payload


@dataclass(slots=True)
class ValidationResult:
    """Stable validation outcome for one normalized report payload."""

    report_type: str
    accepted: bool
    rejections: list[Rejection] = field(default_factory=list)
    normalized_payload: dict[str, object] = field(default_factory=dict)
    normalization: dict[str, object] = field(default_factory=dict)

    @property
    def status(self) -> str:
        """Return a stable string status for downstream routing."""

        return "accepted" if self.accepted else "rejected"

    @property
    def rejection_codes(self) -> list[str]:
        """Return rejection codes in insertion order."""

        return [rejection.code for rejection in self.rejections]

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-safe validation payload."""

        return {
            "report_type": self.report_type,
            "accepted": self.accepted,
            "status": self.status,
            "rejections": [rejection.to_payload() for rejection in self.rejections],
            "normalization": dict(self.normalization),
        }
