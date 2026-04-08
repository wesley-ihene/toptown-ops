"""Cash variance helpers for sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.warnings import WarningEntry, make_warning


@dataclass(slots=True)
class CashVarianceResult:
    """Cash variance computation result."""

    cash_variance: float = 0.0
    warnings: list[WarningEntry] = field(default_factory=list)


def compute_cash_variance(
    *,
    cash_sales: float | None,
    till_total: float | None,
    deposit_total: float | None,
) -> CashVarianceResult:
    """Compute cash variance from expected and reported till values."""

    if cash_sales is None or till_total is None:
        return CashVarianceResult()

    expected_till = cash_sales - (deposit_total or 0.0)
    cash_variance = round(till_total - expected_till, 2)
    warnings: list[WarningEntry] = []
    if abs(cash_variance) > 0.01:
        warnings.append(
            make_warning(
                code="cash_variance_present",
                severity="warning",
                message=f"Cash variance of {cash_variance:.2f} is present.",
            )
        )

    return CashVarianceResult(cash_variance=cash_variance, warnings=warnings)
