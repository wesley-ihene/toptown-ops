"""Canonical numeric figures for sales income reports."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class SalesFigures:
    """Normalized numeric figures extracted from a sales report."""

    gross_sales: float | None = None
    cash_sales: float | None = None
    eftpos_sales: float | None = None
    mobile_money_sales: float | None = None
    till_total: float | None = None
    deposit_total: float | None = None
    traffic: int | None = None
    served: int | None = None
    labor_hours: float | None = None

    def to_payload(self) -> dict[str, float | int | None]:
        """Return the base metrics payload values."""

        return {
            "gross_sales": self.gross_sales,
            "cash_sales": self.cash_sales,
            "eftpos_sales": self.eftpos_sales,
            "mobile_money_sales": self.mobile_money_sales,
            "till_total": self.till_total,
            "deposit_total": self.deposit_total,
            "traffic": self.traffic,
            "served": self.served,
        }
