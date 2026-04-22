"""Pre-specialist policy decisions for upstream orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

from packages.report_policy import get_report_policy

PolicyAction = Literal["allow", "reject"]

_FINAL_PROCESSING_STATUSES: Final[set[str]] = {
    "processed",
    "rejected",
    "duplicate",
    "ready",
    "needs_review",
    "accepted_split",
    "accepted_with_warning",
    "invalid_input",
}


@dataclass(slots=True)
class PolicyDecision:
    """Compact upstream policy decision persisted into audit metadata."""

    action: PolicyAction
    reason: str
    report_family: str
    report_type: str | None
    target_agent: str | None
    fallback_eligible: bool
    hard_reject: bool
    duplicate: bool = False
    duplicate_basis: str | None = None

    def to_metadata(self) -> dict[str, object]:
        """Return the stable metadata representation for raw/rejected records."""

        return {
            "version": "v1",
            "stage": "pre_specialist",
            "action": self.action,
            "reason": self.reason,
            "report_family": self.report_family,
            "report_type": self.report_type,
            "target_agent": self.target_agent,
            "fallback_eligible": self.fallback_eligible,
            "hard_reject": self.hard_reject,
            "duplicate": self.duplicate,
            "duplicate_basis": self.duplicate_basis,
        }


def evaluate_mixed_report_policy(*, reject_mixed_reports: bool) -> PolicyDecision:
    """Return the policy decision for a mixed report before specialist fan-out."""

    if reject_mixed_reports:
        return reject_decision(
            reason="mixed_report_rejected",
            report_family="mixed",
            report_type=None,
            target_agent=None,
        )
    return allow_decision(
        reason="mixed_report_split_allowed",
        report_family="mixed",
        report_type=None,
        target_agent="fan_out",
    )


def evaluate_pre_specialist_policy(
    *,
    existing_metadata: Mapping[str, object],
    report_family: str,
    report_type: str | None,
    target_agent: str | None,
    route_status: str | None,
) -> PolicyDecision:
    """Return the policy decision for a routed single-report work item."""

    duplicate_basis = duplicate_basis_from_metadata(existing_metadata)
    if duplicate_basis is not None:
        return reject_decision(
            reason="duplicate_message",
            report_family=report_family,
            report_type=report_type,
            target_agent=target_agent,
            duplicate=True,
            duplicate_basis=duplicate_basis,
        )

    if report_family == "unknown" or report_type is None:
        return reject_decision(
            reason="unknown_report_type",
            report_family=report_family,
            report_type=report_type,
            target_agent=target_agent,
        )

    if target_agent is None or route_status != "routed":
        return reject_decision(
            reason="route_requires_review",
            report_family=report_family,
            report_type=report_type,
            target_agent=target_agent,
        )

    return allow_decision(
        reason="passed",
        report_family=report_family,
        report_type=report_type,
        target_agent=target_agent,
    )


def allow_decision(
    *,
    reason: str,
    report_family: str,
    report_type: str | None,
    target_agent: str | None,
) -> PolicyDecision:
    """Build an allow decision with stable fallback eligibility."""

    return PolicyDecision(
        action="allow",
        reason=reason,
        report_family=report_family,
        report_type=report_type,
        target_agent=target_agent,
        fallback_eligible=fallback_eligible_for_report_type(report_type),
        hard_reject=False,
    )


def reject_decision(
    *,
    reason: str,
    report_family: str,
    report_type: str | None,
    target_agent: str | None,
    duplicate: bool = False,
    duplicate_basis: str | None = None,
) -> PolicyDecision:
    """Build a hard reject decision with stable fallback eligibility."""

    return PolicyDecision(
        action="reject",
        reason=reason,
        report_family=report_family,
        report_type=report_type,
        target_agent=target_agent,
        fallback_eligible=fallback_eligible_for_report_type(report_type),
        hard_reject=True,
        duplicate=duplicate,
        duplicate_basis=duplicate_basis,
    )


def fallback_eligible_for_report_type(report_type: str | None) -> bool:
    """Return whether a report type is allowed to use specialist fallback parsing."""

    if report_type is None:
        return False
    return get_report_policy(report_type).fallback_enabled


def duplicate_basis_from_metadata(existing_metadata: Mapping[str, object]) -> str | None:
    """Return a stable duplicate basis when a raw record was already finalized."""

    policy_guard = existing_metadata.get("policy_guard")
    if isinstance(policy_guard, Mapping):
        action = _clean_text(policy_guard.get("action"))
        reason = _clean_text(policy_guard.get("reason"))
        if action in {"allow", "reject"}:
            return f"policy_guard:{reason or action}"

    processing_status = _clean_text(existing_metadata.get("processing_status"))
    if processing_status in _FINAL_PROCESSING_STATUSES:
        return f"processing_status:{processing_status}"
    return None


def _clean_text(value: object) -> str | None:
    """Return one stripped string when present."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
