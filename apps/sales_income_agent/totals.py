"""Totals validation helpers for sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.figures import SalesFigures
from apps.sales_income_agent.warnings import WarningEntry, make_warning
from packages.validation.sales_reconciliation import SalesReconciliation, TOLERANCE


@dataclass(slots=True)
class TotalsValidation:
    """Validation result for sales total arithmetic."""

    warnings: list[WarningEntry] = field(default_factory=list)


def derive_gross_sales_from_payment_components(figures: SalesFigures) -> WarningEntry | None:
    """Populate gross sales from payment components when the explicit total is absent."""

    if figures.gross_sales is not None:
        return None

    payment_parts = (
        figures.cash_sales,
        figures.eftpos_sales,
        figures.mobile_money_sales,
    )
    if not any(part is not None for part in payment_parts):
        return None

    figures.gross_sales = round(sum(part or 0.0 for part in payment_parts), 2)
    return make_warning(
        code="gross_sales_derived_from_payment_components",
        severity="warning",
        message="Gross sales was derived from cash, eftpos, and mobile money totals because the explicit total was missing.",
    )


def validate_totals(
    figures: SalesFigures,
    *,
    reconciliation: SalesReconciliation | None = None,
) -> TotalsValidation:
    """Validate the core sales arithmetic conservatively."""

    warnings: list[WarningEntry] = []
    reconciliation_view = reconciliation
    if (
        reconciliation_view is not None
        and (figures.net_sales is not None or figures.gross_sales is not None)
        and reconciliation_view.expected_total_sales is not None
        and reconciliation_view.unexplained_variance > TOLERANCE
    ):
        compared_total = figures.net_sales if figures.net_sales is not None else figures.gross_sales
        warnings.append(
            make_warning(
                code="invalid_totals",
                severity="warning",
                message=(
                    f"Sales total {compared_total:.2f} does not match reconciled totals "
                    f"{reconciliation_view.expected_total_sales:.2f}."
                ),
            )
        )
    return TotalsValidation(warnings=warnings)
