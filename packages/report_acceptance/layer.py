"""Acceptance policy that consumes validation outcomes without re-validating."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from packages.report_policy import get_report_policy
from packages.sop_validation.contracts import ValidationResult

AcceptanceDecision = Literal["accept", "review", "reject"]


@dataclass(slots=True, frozen=True)
class AcceptanceResult:
    """Stable acceptance decision derived from validation and confidence."""

    report_type: str
    decision: AcceptanceDecision
    reason: str
    confidence: float | None
    thresholds: dict[str, float]

    @property
    def status(self) -> str:
        """Return the decision as a stable status string."""

        return self.decision

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-safe acceptance payload."""

        return {
            "report_type": self.report_type,
            "decision": self.decision,
            "status": self.status,
            "reason": self.reason,
            "confidence": self.confidence,
            "thresholds": dict(self.thresholds),
        }


def decide_acceptance(
    report_type: str,
    *,
    validation_result: ValidationResult,
    work_item_payload: Mapping[str, Any],
) -> AcceptanceResult:
    """Return accept/review/reject without duplicating validation rules."""

    confidence = _confidence_from_payload(work_item_payload)
    thresholds = get_report_policy(report_type).confidence_thresholds
    if not validation_result.accepted:
        return AcceptanceResult(
            report_type=report_type,
            decision="reject",
            reason="validation_failed",
            confidence=confidence,
            thresholds=thresholds.to_payload(),
        )
    if confidence is None:
        return AcceptanceResult(
            report_type=report_type,
            decision="review",
            reason="confidence_missing",
            confidence=confidence,
            thresholds=thresholds.to_payload(),
        )
    if confidence >= thresholds.auto_accept_min:
        return AcceptanceResult(
            report_type=report_type,
            decision="accept",
            reason="confidence_meets_auto_accept_threshold",
            confidence=confidence,
            thresholds=thresholds.to_payload(),
        )
    if confidence <= thresholds.reject_max:
        return AcceptanceResult(
            report_type=report_type,
            decision="reject",
            reason="confidence_below_reject_threshold",
            confidence=confidence,
            thresholds=thresholds.to_payload(),
        )
    return AcceptanceResult(
        report_type=report_type,
        decision="review",
        reason="confidence_between_review_and_accept_thresholds",
        confidence=confidence,
        thresholds=thresholds.to_payload(),
    )


def _confidence_from_payload(work_item_payload: Mapping[str, Any]) -> float | None:
    """Return the best available confidence input for acceptance policy."""

    candidates = [
        work_item_payload.get("confidence"),
        _mapping_value(work_item_payload.get("normalized_report"), "confidence"),
        _mapping_value(work_item_payload.get("raw_message"), "confidence"),
        _mapping_value(work_item_payload.get("classification"), "confidence"),
    ]
    for candidate in candidates:
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            return float(candidate)
    return None


def _mapping_value(value: object, field_name: str) -> Any:
    """Return a field from a mapping-like object when present."""

    if not isinstance(value, Mapping):
        return None
    return value.get(field_name)
