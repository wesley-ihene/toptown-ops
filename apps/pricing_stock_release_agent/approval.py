"""Approval interpretation helpers for bale summary items."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.pricing_stock_release_agent.parser import WarningEntry, make_warning
from apps.pricing_stock_release_agent.stock_flow import StockFlowInterpretation


@dataclass(slots=True)
class ApprovalInterpretation:
    """Approval backlog derived from stock-flow metrics."""

    bales_pending_approval: int = 0
    warnings: list[WarningEntry] = field(default_factory=list)


def interpret_approval(stock_flow: StockFlowInterpretation) -> ApprovalInterpretation:
    """Return backlog warnings when any bales remain pending approval."""

    warnings: list[WarningEntry] = []
    if stock_flow.bales_pending_approval > 0:
        warnings.append(
            make_warning(
                code="approval_backlog",
                severity="warning",
                message="One or more bales is still waiting for approval.",
            )
        )

    return ApprovalInterpretation(
        bales_pending_approval=stock_flow.bales_pending_approval,
        warnings=warnings,
    )
