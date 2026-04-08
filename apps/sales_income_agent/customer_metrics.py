"""Customer metric derivation and validation for sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.warnings import WarningEntry, make_warning

LOW_CONVERSION_THRESHOLD = 0.35


@dataclass(slots=True)
class CustomerMetrics:
    """Derived customer metrics and validations."""

    traffic: int = 0
    served: int = 0
    conversion_rate: float = 0.0
    warnings: list[WarningEntry] = field(default_factory=list)


def evaluate_customer_metrics(
    *,
    traffic: int | None,
    served: int | None,
) -> CustomerMetrics:
    """Return customer metrics and warnings from traffic/served fields."""

    normalized_traffic = traffic or 0
    normalized_served = served or 0
    warnings: list[WarningEntry] = []

    if normalized_traffic > 0 and normalized_served > normalized_traffic:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message=(
                    f"Served count {normalized_served} exceeds traffic count "
                    f"{normalized_traffic}."
                ),
            )
        )

    conversion_rate = 0.0
    if normalized_traffic > 0:
        conversion_rate = round(normalized_served / normalized_traffic, 4)
        if conversion_rate < LOW_CONVERSION_THRESHOLD:
            warnings.append(
                make_warning(
                    code="low_conversion",
                    severity="warning",
                    message=(
                        f"Conversion rate {conversion_rate:.2%} is below the review threshold."
                    ),
                )
            )

    return CustomerMetrics(
        traffic=normalized_traffic,
        served=normalized_served,
        conversion_rate=conversion_rate,
        warnings=warnings,
    )
