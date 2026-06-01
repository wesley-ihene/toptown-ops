"""Derived performance metrics for sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.customer_metrics import LOW_CONVERSION_THRESHOLD
from apps.sales_income_agent.warnings import (
    LOW_CONVERSION_ALERT_TYPE,
    AlertEntry,
    WarningEntry,
    make_alert,
    make_warning,
)


@dataclass(slots=True)
class PerformanceMetrics:
    """Derived sales performance metrics."""

    sales_per_customer: float = 0.0
    sales_per_labor_hour: float = 0.0
    warnings: list[WarningEntry] = field(default_factory=list)
    alerts: list[AlertEntry] = field(default_factory=list)


def compute_performance_metrics(
    *,
    gross_sales: float | None,
    traffic: int | None,
    served: int | None,
    conversion_rate: float | None,
    labor_hours: float | None,
) -> PerformanceMetrics:
    """Return derived performance metrics, anomaly warnings, and performance alerts."""

    sales_per_customer = 0.0
    sales_per_labor_hour = 0.0
    warnings: list[WarningEntry] = []
    alerts: list[AlertEntry] = []

    if gross_sales is not None and served is not None and served > 0:
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
    if (
        traffic is not None
        and served is not None
        and conversion_rate is not None
        and conversion_rate < LOW_CONVERSION_THRESHOLD
    ):
        alerts.append(
            make_alert(
                alert_type=LOW_CONVERSION_ALERT_TYPE,
                alert_level="warning",
                alert_message=f"Conversion rate is low at {conversion_rate:.2%}.",
            )
        )

    return PerformanceMetrics(
        sales_per_customer=sales_per_customer,
        sales_per_labor_hour=sales_per_labor_hour,
        warnings=warnings,
        alerts=alerts,
    )
