"""Pricing interpretation helpers for bale summary items."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.pricing_stock_release_agent.parser import ParsedBaleSummary
from apps.pricing_stock_release_agent.warnings import WarningEntry, make_warning


@dataclass(slots=True)
class PricingItem:
    """Structured item output with calculated pricing."""

    bale_id: str
    item_name: str
    qty: int | float
    amount: float
    price_per_piece: float | None


@dataclass(slots=True)
class PricingInterpretation:
    """Pricing totals and item-level calculations."""

    items: list[PricingItem] = field(default_factory=list)
    total_amount: float = 0.0
    warnings: list[WarningEntry] = field(default_factory=list)


def interpret_pricing(parsed: ParsedBaleSummary) -> PricingInterpretation:
    """Calculate per-piece pricing and detect conservative anomalies."""

    items: list[PricingItem] = []
    warnings: list[WarningEntry] = []
    total_amount = 0.0

    for parsed_item in parsed.items:
        qty_value = float(parsed_item.qty)
        amount_value = float(parsed_item.amount)
        price_per_piece = amount_value / qty_value if qty_value > 0 else None

        if qty_value <= 0 or amount_value < 0 or price_per_piece in {None, 0.0}:
            warnings.append(
                make_warning(
                    code="financial_anomaly",
                    severity="warning",
                    message="One or more bale items has a non-positive quantity or amount.",
                )
            )

        items.append(
            PricingItem(
                bale_id=parsed_item.bale_id,
                item_name=parsed_item.item_name,
                qty=parsed_item.qty,
                amount=amount_value,
                price_per_piece=round(price_per_piece, 2) if price_per_piece is not None else None,
            )
        )
        total_amount += amount_value

    if parsed.declared_total_amount is not None and abs(parsed.declared_total_amount - total_amount) > 0.01:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message=(
                    "Declared total amount "
                    f"{parsed.declared_total_amount:.2f} does not match parsed total amount "
                    f"{round(total_amount, 2):.2f}."
                ),
            )
        )

    return PricingInterpretation(
        items=items,
        total_amount=round(total_amount, 2),
        warnings=warnings,
    )
