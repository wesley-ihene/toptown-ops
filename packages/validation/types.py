"""Typed helpers for validation and control metadata."""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True, frozen=True)
class ValidationRejection:
    """One machine-readable rejection entry."""

    reason_code: str
    reason_detail: str
    field: str | None = None
    extra: dict[str, Any] = dataclass_field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe rejection payload with compatibility aliases."""

        payload: dict[str, Any] = {
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            "code": self.reason_code,
            "message": self.reason_detail,
        }
        if self.field is not None:
            payload["field"] = self.field
        payload.update(self.extra)
        return payload


@dataclass(slots=True)
class ValidationMetadata:
    """Structured validation/control metadata kept outside business payloads."""

    stage: str
    status: str
    accepted: bool
    validated_at_utc: str = dataclass_field(default_factory=utc_now_iso)
    rejections: list[dict[str, Any]] = dataclass_field(default_factory=list)
    normalization: dict[str, Any] = dataclass_field(default_factory=dict)
    details: dict[str, Any] = dataclass_field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe metadata payload."""

        return {
            "stage": self.stage,
            "status": self.status,
            "accepted": self.accepted,
            "validated_at_utc": self.validated_at_utc,
            "reason_codes": [
                rejection["reason_code"]
                for rejection in self.rejections
                if isinstance(rejection, dict) and isinstance(rejection.get("reason_code"), str)
            ],
            "rejections": list(self.rejections),
            "normalization": dict(self.normalization),
            "details": dict(self.details),
        }
