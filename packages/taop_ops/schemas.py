"""Read-only TAOP operations schemas."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class OpsWarning:
    """Structured warning payload for TAOP ops summaries and exports."""

    code: str
    message: str
    severity: str = "warning"
    branch: str | None = None
    date: str | None = None
    category: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }
        if self.branch is not None:
            payload["branch"] = self.branch
        if self.date is not None:
            payload["date"] = self.date
        if self.category is not None:
            payload["category"] = self.category
        return payload


@dataclass(frozen=True, slots=True)
class SalesSummaryRow:
    """Read-only branch/day sales summary row."""

    branch: str
    date: str
    total_cash: float | None = None
    total_card: float | None = None
    total_sales: float | None = None
    till_count: float | None = None
    traffic: int | None = None
    served: int | None = None
    conversion_rate: float | None = None
    sales_per_customer: float | None = None
    status: str | None = None
    governance: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "branch": self.branch,
            "date": self.date,
            "total_cash": self.total_cash,
            "total_card": self.total_card,
            "total_sales": self.total_sales,
            "till_count": self.till_count,
            "traffic": self.traffic,
            "served": self.served,
            "conversion_rate": self.conversion_rate,
            "sales_per_customer": self.sales_per_customer,
            "status": self.status,
            "governance": self.governance,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class BaleReleaseSummaryRow:
    """Read-only branch/day bale-release summary row."""

    branch: str
    date: str
    total_qty: int | None = None
    total_amount: float | None = None
    item_count: int | None = None
    bales_processed: int | None = None
    bales_released: int | None = None
    bales_pending_approval: int | None = None
    status: str | None = None
    governance: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "branch": self.branch,
            "date": self.date,
            "total_qty": self.total_qty,
            "total_amount": self.total_amount,
            "item_count": self.item_count,
            "bales_processed": self.bales_processed,
            "bales_released": self.bales_released,
            "bales_pending_approval": self.bales_pending_approval,
            "status": self.status,
            "governance": self.governance,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class AttendanceSummaryRow:
    """Read-only branch/day attendance summary row."""

    branch: str
    date: str
    present_count: int | None = None
    off_count: int | None = None
    leave_count: int | None = None
    absent_count: int | None = None
    staff_total: int | None = None
    status: str | None = None
    governance: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "branch": self.branch,
            "date": self.date,
            "present_count": self.present_count,
            "off_count": self.off_count,
            "leave_count": self.leave_count,
            "absent_count": self.absent_count,
            "staff_total": self.staff_total,
            "status": self.status,
            "governance": self.governance,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class CategorySummary:
    """One category summary envelope."""

    category: str
    rows: list[dict[str, Any]]
    warnings: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "record_count": len(self.rows),
            "rows": list(self.rows),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class DailyBranchOperations:
    """One branch slice in the TAOP daily operations summary."""

    branch: str
    date: str
    sales: dict[str, Any]
    bale_release: dict[str, Any]
    attendance: dict[str, Any]
    warnings: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "branch": self.branch,
            "date": self.date,
            "sales": dict(self.sales),
            "bale_release": dict(self.bale_release),
            "attendance": dict(self.attendance),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class DailyOperationsSummary:
    """Top-level TAOP daily operations payload."""

    date: str
    branch: str | None
    sales: dict[str, Any]
    bale_release: dict[str, Any]
    attendance: dict[str, Any]
    branches: list[dict[str, Any]]
    warnings: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "branch": self.branch,
            "sales": dict(self.sales),
            "bale_release": dict(self.bale_release),
            "attendance": dict(self.attendance),
            "branches": list(self.branches),
            "warnings": list(self.warnings),
        }
