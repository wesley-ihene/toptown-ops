"""Throughput interpretation helpers for bale summary items."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.pricing_stock_release_agent.parser import WarningEntry, make_warning
from apps.pricing_stock_release_agent.stock_flow import StockFlowInterpretation

LOW_RELEASE_RATIO_THRESHOLD = 0.8


@dataclass(slots=True)
class ThroughputInterpretation:
    """Throughput and readiness derived from stock-flow metrics."""

    release_ratio: float = 0.0
    readiness_status: str = "blocked"
    warnings: list[WarningEntry] = field(default_factory=list)


def interpret_throughput(stock_flow: StockFlowInterpretation) -> ThroughputInterpretation:
    """Return release ratio and a conservative readiness status."""

    if stock_flow.bales_processed <= 0:
        return ThroughputInterpretation(
            release_ratio=0.0,
            readiness_status="blocked",
            warnings=[
                make_warning(
                    code="missing_fields",
                    severity="error",
                    message="Processed bale count is unavailable.",
                )
            ],
        )

    release_ratio = stock_flow.bales_released / stock_flow.bales_processed
    warnings: list[WarningEntry] = []
    if release_ratio < LOW_RELEASE_RATIO_THRESHOLD:
        warnings.append(
            make_warning(
                code="low_release_ratio",
                severity="warning",
                message="Release ratio is below the safe review threshold.",
            )
        )

    readiness_status = "ready" if not warnings else "needs_review"
    return ThroughputInterpretation(
        release_ratio=round(release_ratio, 4),
        readiness_status=readiness_status,
        warnings=warnings,
    )
