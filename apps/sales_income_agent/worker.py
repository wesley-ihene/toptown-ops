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
from packages.data_governance import build_governance_context
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.validation import ValidationMetadata, normalize_rejections

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
    candidate_only = _candidate_mode_requested(payload)

    try:
        validation_warnings = _validate_input(payload)
        if validation_warnings:
            return _failure_result(validation_warnings, work_item_payload=payload)

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
        status = _determine_status(parsed=parsed, warnings=warnings)

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
                    "gross_sales": _candidate_metric_value(parsed.figures.gross_sales, candidate_only=candidate_only),
                    "cash_sales": _candidate_metric_value(parsed.figures.cash_sales, candidate_only=candidate_only),
                    "eftpos_sales": _candidate_metric_value(parsed.figures.eftpos_sales, candidate_only=candidate_only),
                    "mobile_money_sales": _candidate_metric_value(parsed.figures.mobile_money_sales, candidate_only=candidate_only),
                    "till_total": _candidate_metric_value(parsed.figures.till_total, candidate_only=candidate_only),
                    "deposit_total": _candidate_metric_value(parsed.figures.deposit_total, candidate_only=candidate_only),
                    "traffic": _candidate_metric_value(parsed.figures.traffic, candidate_only=candidate_only, default=customer_metrics.traffic),
                    "served": _candidate_metric_value(parsed.figures.served, candidate_only=candidate_only, default=customer_metrics.served),
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
            metadata=_validation_metadata(status=status, warnings=warnings, work_item_payload=payload),
        )
    except Exception:
        result = _failure_result(
            [
                make_warning(
                    code="parser_failure",
                    severity="error",
                    message="The sales report could not be parsed safely.",
                )
            ],
            work_item_payload=payload,
            parser_failure=True,
        )

    if candidate_only:
        return result

    write_result = write_structured_record(result.payload, metadata=result.metadata)
    _apply_governance_result(result, write_result)
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


def _failure_result(
    warnings: list[WarningEntry],
    *,
    work_item_payload: Mapping[str, object],
    parser_failure: bool = False,
) -> AgentResult:
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
        metadata=_validation_metadata(
            status="invalid_input",
            warnings=warnings,
            work_item_payload=work_item_payload,
            parser_failure=parser_failure,
        ),
    )


def _determine_status(
    *,
    parsed,
    warnings: list[WarningEntry],
) -> str:
    """Return the final sales status with strict core checks and soft optional warnings."""

    if not parsed.branch_slug or not parsed.report_date or parsed.figures.gross_sales is None:
        return "invalid_input"

    critical_warning_codes = {
        "missing_fields",
        "invalid_totals",
        "data_mismatch",
        "financial_anomaly",
    }
    if any(warning.code in critical_warning_codes for warning in warnings):
        return "needs_review"
    if warnings:
        return "accepted_with_warning"
    return "accepted"


def _validation_metadata(
    *,
    status: str,
    warnings: list[WarningEntry],
    work_item_payload: Mapping[str, object],
    parser_failure: bool = False,
) -> dict[str, object]:
    """Return sidecar validation metadata for structured sales records."""

    return {
        "validation": ValidationMetadata(
            stage=AGENT_NAME,
            status="passed" if status != "invalid_input" else "rejected",
            accepted=status != "invalid_input",
            rejections=normalize_rejections([warning.to_payload() for warning in warnings if warning.severity == "error"]),
            details={
                "final_status": status,
                "parser_failure": parser_failure,
            },
        ).to_payload(),
        "governance_context": build_governance_context(work_item_payload),
    }


def _candidate_mode_requested(payload: Mapping[str, object]) -> bool:
    """Return whether this worker should stop at candidate generation."""

    return payload.get("governance_mode") == "candidate"


def _candidate_metric_value(
    value: object,
    *,
    candidate_only: bool,
    default: int | float | None = 0.0,
) -> object:
    """Preserve missing numeric fields in candidate mode instead of inventing zeroes."""

    if value is None and candidate_only:
        return None
    return value if value is not None else default


def _apply_governance_result(result: AgentResult, write_result: object) -> None:
    """Project the persisted governance result back onto the live agent payload."""

    governance = getattr(write_result, "governance", None)
    if governance is None:
        return
    result.payload["status"] = governance.status
    result.payload["export_allowed"] = governance.export_allowed
    result.payload["governance"] = governance.to_payload()
