"""Performance summary helpers for HR reports."""

from __future__ import annotations

from apps.hr_agent.figures import PerformanceFigures, PerformanceRecord
from apps.hr_agent.warnings import WarningEntry, make_warning


def summarize_performance(
    records: list[PerformanceRecord],
    *,
    declared_items_moved: int | None,
    declared_assisting_count: int | None,
    declared_record_count: int | None,
) -> tuple[PerformanceFigures, list[WarningEntry]]:
    """Return summarized performance figures and reconciliation warnings."""

    figures = PerformanceFigures(
        parsed_record_count=len(records),
        parsed_items_moved=sum(record.items_moved for record in records),
        parsed_assisting_count=sum(record.assisting_count for record in records),
        declared_record_count=declared_record_count,
        declared_items_moved=declared_items_moved,
        declared_assisting_count=declared_assisting_count,
    )

    warnings: list[WarningEntry] = []
    if declared_record_count is not None and declared_record_count != figures.parsed_record_count:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message=(
                    "Declared staff record total "
                    f"{declared_record_count} does not match parsed record total "
                    f"{figures.parsed_record_count}."
                ),
            )
        )
    if declared_items_moved is not None and declared_items_moved != figures.parsed_items_moved:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message=(
                    "Declared grand total items moved "
                    f"{declared_items_moved} does not match parsed total "
                    f"{figures.parsed_items_moved}."
                ),
            )
        )
    if declared_assisting_count is not None and declared_assisting_count != figures.parsed_assisting_count:
        warnings.append(
            make_warning(
                code="data_mismatch",
                severity="warning",
                message=(
                    "Declared grand total assisting count "
                    f"{declared_assisting_count} does not match parsed total "
                    f"{figures.parsed_assisting_count}."
                ),
            )
        )

    return figures, warnings
