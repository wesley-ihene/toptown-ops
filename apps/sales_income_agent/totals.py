"""Totals validation helpers for sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.figures import SalesFigures
from apps.sales_income_agent.warnings import WarningEntry, make_warning

TOLERANCE = 0.01


@dataclass(slots=True)
class TotalsValidation:
    """Validation result for sales total arithmetic."""

    warnings: list[WarningEntry] = field(default_factory=list)


def validate_totals(figures: SalesFigures) -> TotalsValidation:
    """Validate the core sales arithmetic conservatively."""

    warnings: list[WarningEntry] = []
    total_parts = [
        figures.cash_sales,
        figures.eftpos_sales,
        figures.mobile_money_sales,
    ]
    if figures.gross_sales is not None and all(part is not None for part in total_parts):
        computed_total = sum(part for part in total_parts if part is not None)
        if abs(figures.gross_sales - computed_total) > TOLERANCE:
            warnings.append(
                make_warning(
                    code="invalid_totals",
                    severity="warning",
                    message=(
                        f"Gross sales {figures.gross_sales:.2f} do not match payment totals "
                        f"{computed_total:.2f}."
                    ),
                )
            )
    return TotalsValidation(warnings=warnings)
