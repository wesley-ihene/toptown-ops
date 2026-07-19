"""Canonical numeric figures for sales income reports."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class SalesFigures:
    """Normalized numeric figures extracted from a sales report."""

    gross_sales: float | None = None
    net_sales: float | None = None
    cash_sales: float | None = None
    eftpos_sales: float | None = None
    item_returns: float | None = None
    cash_over: float | None = None
    cash_down: float | None = None
    mobile_money_sales: float | None = None
    till_total: float | None = None
    deposit_total: float | None = None
    z_reading: float | None = None
    traffic: int | None = None
    served: int | None = None
    labor_hours: float | None = None

    def to_payload(self) -> dict[str, float | int | None]:
        """Return the base metrics payload values."""

        return {
            "gross_sales": self.gross_sales,
            "net_sales": self.net_sales,
            "cash_sales": self.cash_sales,
            "eftpos_sales": self.eftpos_sales,
            "item_returns": self.item_returns,
            "cash_adjustment_return": self.item_returns,
            "item_returns_total": self.item_returns,
            "total_returns": self.item_returns,
            "cash_over": self.cash_over,
            "cash_down": self.cash_down,
            "mobile_money_sales": self.mobile_money_sales,
            "till_total": self.till_total,
            "deposit_total": self.deposit_total,
            "z_reading": self.z_reading,
            "traffic": self.traffic,
            "served": self.served,
        }

    def to_dict(self) -> dict[str, float | int | None]:
        """Return a dict view of the extracted figures."""

        return self.to_payload()
