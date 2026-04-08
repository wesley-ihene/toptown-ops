"""Derived performance metrics for sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.warnings import WarningEntry, make_warning


@dataclass(slots=True)
class PerformanceMetrics:
    """Derived sales performance metrics."""

    sales_per_customer: float = 0.0
    sales_per_labor_hour: float = 0.0
    warnings: list[WarningEntry] = field(default_factory=list)


def compute_performance_metrics(
    *,
    gross_sales: float | None,
    served: int,
    labor_hours: float | None,
) -> PerformanceMetrics:
    """Return derived performance metrics and anomaly warnings."""

    sales_per_customer = 0.0
    sales_per_labor_hour = 0.0
    warnings: list[WarningEntry] = []

    if gross_sales is not None and served > 0:
        sales_per_customer = round(gross_sales / served, 2)
    if gross_sales is not None and labor_hours is not None and labor_hours > 0:
        sales_per_labor_hour = round(gross_sales / labor_hours, 2)
    if gross_sales is not None and gross_sales < 0:
        warnings.append(
            make_warning(
                code="financial_anomaly",
                severity="warning",
                message="Gross sales is negative.",
            )
        )

    return PerformanceMetrics(
        sales_per_customer=sales_per_customer,
        sales_per_labor_hour=sales_per_labor_hour,
        warnings=warnings,
    )
