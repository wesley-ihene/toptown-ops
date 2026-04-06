"""Stock release interpretation helpers for bale summary items."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.pricing_stock_release_agent.parser import ParsedBaleSummary, WarningEntry, make_warning


@dataclass(slots=True)
class StockFlowInterpretation:
    """Derived stock-release metrics from parsed bale items and summary lines."""

    bales_processed: int = 0
    bales_released: int = 0
    bales_pending_approval: int = 0
    total_qty: int | float = 0
    warnings: list[WarningEntry] = field(default_factory=list)


def interpret_stock_flow(parsed: ParsedBaleSummary) -> StockFlowInterpretation:
    """Derive release and pending counts from parsed items and summary counts."""

    item_count = len(parsed.items)
    bales_processed = (
        parsed.declared_bales_processed
        if parsed.declared_bales_processed is not None
        else item_count
    )
    bales_released = (
        parsed.declared_bales_released if parsed.declared_bales_released is not None else 0
    )
    bales_pending_approval = (
        parsed.declared_bales_pending_approval
        if parsed.declared_bales_pending_approval is not None
        else 0
    )
    total_qty = sum(float(item.qty) for item in parsed.items)

    warnings: list[WarningEntry] = []
    if parsed.declared_bales_processed is not None and parsed.declared_bales_processed != item_count:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message="Declared processed bale count does not match the number of parsed bale blocks.",
            )
        )
    if bales_released + bales_pending_approval > bales_processed:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message="Released and pending approval counts exceed processed bale count.",
            )
        )

    normalized_total_qty: int | float
    if total_qty.is_integer():
        normalized_total_qty = int(total_qty)
    else:
        normalized_total_qty = round(total_qty, 2)

    return StockFlowInterpretation(
        bales_processed=bales_processed,
        bales_released=bales_released,
        bales_pending_approval=bales_pending_approval,
        total_qty=normalized_total_qty,
        warnings=warnings,
    )
