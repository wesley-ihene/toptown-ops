"""Customer metric derivation and data-quality validation for sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.warnings import (
    MISSING_CUSTOMER_DATA_WARNING_CODE,
    WarningEntry,
    make_warning,
)

LOW_CONVERSION_THRESHOLD = 0.35


@dataclass(slots=True)
class CustomerMetrics:
    """Derived customer metrics and validations."""

    traffic: int | None = None
    served: int | None = None
    conversion_rate: float | None = None
    warnings: list[WarningEntry] = field(default_factory=list)


def evaluate_customer_metrics(
    *,
    traffic: int | None,
    served: int | None,
) -> CustomerMetrics:
    """Return customer metrics and data-quality warnings from traffic/served fields."""

    warnings: list[WarningEntry] = []

    if traffic is None or served is None:
        warnings.append(
            make_warning(
                code=MISSING_CUSTOMER_DATA_WARNING_CODE,
                severity="warning",
                message=_missing_customer_data_message(traffic=traffic, served=served),
            )
        )
    elif served > traffic:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message=(
                    f"Served count {served} exceeds traffic count "
                    f"{traffic}."
                ),
            )
        )

    conversion_rate = _conversion_rate(traffic=traffic, served=served)

    return CustomerMetrics(
        traffic=traffic,
        served=served,
        conversion_rate=conversion_rate,
        warnings=warnings,
    )


def _conversion_rate(*, traffic: int | None, served: int | None) -> float | None:
    """Return one customer conversion rate or ``None`` when inputs are incomplete."""

    if traffic is None or served is None:
        return None
    if traffic == 0:
        return 0.0
    return round(served / traffic, 4)


def _missing_customer_data_message(*, traffic: int | None, served: int | None) -> str:
    """Describe which customer-count inputs are missing."""

    missing_fields = []
    if traffic is None:
        missing_fields.append("traffic")
    if served is None:
        missing_fields.append("served")

    if len(missing_fields) == 2:
        return "Traffic and served counts are missing, so conversion could not be evaluated."
    return f"{missing_fields[0].capitalize()} count is missing, so conversion could not be evaluated."
