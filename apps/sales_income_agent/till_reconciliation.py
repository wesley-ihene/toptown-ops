"""Till reconciliation checks for sales totals."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.warnings import WarningEntry, make_warning


@dataclass(slots=True)
class TillReconciliationResult:
    """Reconciliation view of till versus totals fields."""

    warnings: list[WarningEntry] = field(default_factory=list)


def reconcile_till_fields(
    *,
    till_total: float | None,
    cash_sales: float | None,
    deposit_total: float | None,
) -> TillReconciliationResult:
    """Emit structured warnings when till figures conflict materially."""

    warnings: list[WarningEntry] = []
    if till_total is None or cash_sales is None:
        return TillReconciliationResult(warnings=warnings)

    expected_till = cash_sales - (deposit_total or 0.0)
    if abs(till_total - expected_till) > 0.01:
        warnings.append(
            make_warning(
                code="till_mismatch",
                severity="warning",
                message=(
                    f"Till total {till_total:.2f} does not reconcile with cash sales "
                    f"{cash_sales:.2f} and deposit total {(deposit_total or 0.0):.2f}."
                ),
            )
        )

    return TillReconciliationResult(warnings=warnings)
