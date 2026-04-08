"""Worker orchestrator for sales income specialist processing."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from apps.sales_income_agent.confidence import compute_confidence
from apps.sales_income_agent.customer_metrics import evaluate_customer_metrics
from apps.sales_income_agent.parser import parse_work_item
from apps.sales_income_agent.performance import compute_performance_metrics
from apps.sales_income_agent.record_store import write_structured_record
from apps.sales_income_agent.till_reconciliation import reconcile_till_fields
from apps.sales_income_agent.totals import validate_totals
from apps.sales_income_agent.variance import compute_cash_variance
from apps.sales_income_agent.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "sales_income_agent"
SIGNAL_TYPE = "sales_income"


@dataclass(slots=True)
class SalesIncomeAgentWorker:
    """Specialist worker for messy WhatsApp-style sales reports."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured sales result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a conservative structured result for a sales work item."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    validation_warnings = _validate_input(payload)
    if validation_warnings:
        return _failure_result(validation_warnings)

    parsed = parse_work_item(work_item)
    totals_validation = validate_totals(parsed.figures)
    customer_metrics = evaluate_customer_metrics(
        traffic=parsed.figures.traffic,
        served=parsed.figures.served,
    )
    cash_variance = compute_cash_variance(
        cash_sales=parsed.figures.cash_sales,
        till_total=parsed.figures.till_total,
        deposit_total=parsed.figures.deposit_total,
    )
    performance_metrics = compute_performance_metrics(
        gross_sales=parsed.figures.gross_sales,
        served=customer_metrics.served,
        labor_hours=parsed.figures.labor_hours,
    )
    reconciliation = reconcile_till_fields(
        till_total=parsed.figures.till_total,
        cash_sales=parsed.figures.cash_sales,
        deposit_total=parsed.figures.deposit_total,
    )
    warnings = dedupe_warnings(
        parsed.warnings
        + totals_validation.warnings
        + customer_metrics.warnings
        + cash_variance.warnings
        + performance_metrics.warnings
        + reconciliation.warnings
    )
    status = "ready" if not warnings else "needs_review"

    result = AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "branch": parsed.branch_slug or parsed.branch,
            "report_date": parsed.report_date,
            "confidence": compute_confidence(
                branch=parsed.branch,
                report_date=parsed.report_date,
                totals_found=parsed.figures.gross_sales is not None,
                critical_fields_complete=bool(parsed.branch and parsed.report_date),
                warnings=warnings,
            ),
            "metrics": {
                "gross_sales": parsed.figures.gross_sales or 0.0,
                "cash_sales": parsed.figures.cash_sales or 0.0,
                "eftpos_sales": parsed.figures.eftpos_sales or 0.0,
                "mobile_money_sales": parsed.figures.mobile_money_sales or 0.0,
                "till_total": parsed.figures.till_total or 0.0,
                "deposit_total": parsed.figures.deposit_total or 0.0,
                "traffic": customer_metrics.traffic,
                "served": customer_metrics.served,
                "conversion_rate": customer_metrics.conversion_rate,
                "sales_per_customer": performance_metrics.sales_per_customer,
                "sales_per_labor_hour": performance_metrics.sales_per_labor_hour,
                "cash_variance": cash_variance.cash_variance,
            },
            "items": [],
            "provenance": parsed.provenance.to_payload(),
            "warnings": [warning.to_payload() for warning in warnings],
            "status": status,
        },
    )
    write_structured_record(result.payload)
    return result


def _validate_input(payload: dict[str, object]) -> list[WarningEntry]:
    """Validate the strict sales work-item input contract."""

    warnings: list[WarningEntry] = []
    classification = payload.get("classification")
    raw_message = payload.get("raw_message")

    if not isinstance(classification, Mapping) or classification.get("report_type") != "sales":
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item classification must be `sales`.",
            )
        )
    if not isinstance(raw_message, Mapping) or not isinstance(raw_message.get("text"), str):
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item raw_message.text field must be present for sales parsing.",
            )
        )

    return dedupe_warnings(warnings)


def _failure_result(warnings: list[WarningEntry]) -> AgentResult:
    """Return a safe structured failure result."""

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "branch": None,
            "report_date": None,
            "confidence": 0.0,
            "metrics": {
                "gross_sales": 0.0,
                "cash_sales": 0.0,
                "eftpos_sales": 0.0,
                "mobile_money_sales": 0.0,
                "till_total": 0.0,
                "deposit_total": 0.0,
                "traffic": 0,
                "served": 0,
                "conversion_rate": 0.0,
                "sales_per_customer": 0.0,
                "sales_per_labor_hour": 0.0,
                "cash_variance": 0.0,
            },
            "items": [],
            "provenance": {
                "cashier": "",
                "assistant": "",
                "balanced_by": "",
                "supervisor": "",
                "supervisor_confirmation": "",
                "notes": [],
            },
            "warnings": [warning.to_payload() for warning in warnings],
            "status": "invalid_input",
        },
    )
