"""Escalation derivation helpers for supervisor control reports."""

from __future__ import annotations

from dataclasses import dataclass

from apps.supervisor_control_agent.exceptions import ExceptionSummary


@dataclass(slots=True)
class EscalationSummary:
    """Derived escalation state for supervisor control output."""

    escalated_count: int = 0
    escalation_required: bool = False


def derive_escalation(exceptions: ExceptionSummary) -> EscalationSummary:
    """Return escalation metrics from normalized exception items."""

    escalated_count = sum(1 for item in exceptions.items if item.status == "escalated")
    escalation_required = any(item.status in {"open", "escalated"} for item in exceptions.items)
    return EscalationSummary(
        escalated_count=escalated_count,
        escalation_required=escalation_required,
    )
