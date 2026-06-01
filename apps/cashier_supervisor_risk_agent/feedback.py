"""Recommended-action helpers for cashier and supervisor risk entries."""

from __future__ import annotations

from collections.abc import Iterable


def recommended_action_for_entry(
    *,
    risk_type: str,
    severity: str,
    person_role: str,
    review_flags: Iterable[str] = (),
) -> str:
    """Return one concise recommended action for a risk entry."""

    flags = set(review_flags)
    if risk_type == "balanced_by_same_as_cashier":
        return "Supervisor review recommended."
    if risk_type == "z_reading_mismatch":
        return "Review the z-reading mismatch and confirm the declared reading against reconciliation."
    if "balanced_by_same_as_cashier" in flags:
        return "Review same-person balancing and confirm an independent supervisor check."
    if risk_type == "unexplained_variance":
        return "Review the source report and reconcile the unresolved variance before closeout."
    if risk_type == "repeated_adjustment":
        if person_role == "cashier":
            return "Review the cashier's recent adjustment pattern and confirm supporting evidence."
        if person_role == "supervisor":
            return "Review repeated supervised adjustments and confirm approvals were documented."
        if person_role == "balanced_by":
            return "Review repeated balancing activity and confirm segregation of duties."
    if risk_type == "high_adjustment":
        return "Review the large single-day adjustment and confirm supporting documents."
    if risk_type == "item_returns":
        return "Review return support and confirm the return type was documented."
    if risk_type in {"cash_over", "cash_down"}:
        return "Review cash variance support and confirm the variance reason was documented."
    if severity == "high":
        return "Escalate for manual review."
    if severity == "medium":
        return "Queue for supervisor review."
    return "Monitor for repeat occurrences."
