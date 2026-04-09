"""Control-status derivation helpers for supervisor control reports."""

from __future__ import annotations

from dataclasses import dataclass

from apps.supervisor_control_agent.exceptions import ExceptionSummary


@dataclass(slots=True)
class ControlsSummary:
    """Derived control metrics for supervisor control output."""

    confirmed_count: int = 0
    control_gap_count: int = 0


def derive_controls(exceptions: ExceptionSummary) -> ControlsSummary:
    """Return confirmation and control-gap metrics from normalized exceptions."""

    confirmed_count = sum(1 for item in exceptions.items if item.supervisor_confirmed == "YES")
    control_gap_count = sum(1 for item in exceptions.items if item.supervisor_confirmed != "YES")
    return ControlsSummary(
        confirmed_count=confirmed_count,
        control_gap_count=control_gap_count,
    )
