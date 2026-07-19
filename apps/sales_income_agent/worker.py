"""Worker orchestrator for sales income specialist processing."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re

from apps.adaptive_sop_engine.worker import apply_adaptive_sop, sync_validation_metadata
from apps.sales_income_agent.confidence import compute_confidence
from apps.sales_income_agent.customer_metrics import evaluate_customer_metrics
from apps.sales_income_agent.parser import parse_work_item
from apps.sales_income_agent.performance import compute_performance_metrics
from apps.sales_income_agent.record_store import write_structured_record
from apps.sales_income_agent.till_reconciliation import reconcile_till_fields
from apps.sales_income_agent.totals import (
    derive_gross_sales_from_payment_components,
    validate_totals,
)
from apps.sales_income_agent.variance import compute_cash_variance
from apps.sales_income_agent.warnings import (
    WarningEntry,
    dedupe_alerts,
    dedupe_warnings,
    make_warning,
    warning_impacts_status,
)
from packages.data_governance import build_governance_context
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.validation import ValidationMetadata, normalize_rejections
from packages.validation.sales_reconciliation import TOLERANCE, build_sales_reconciliation, empty_sales_reconciliation

AGENT_NAME = "sales_income_agent"
SIGNAL_TYPE = "sales_income"
_YES_NO_TEXT_PATTERN = re.compile(r"\b(yes|y|confirmed|ok|okay)\b", flags=re.IGNORECASE)


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
            result = _failure_result(validation_warnings, work_item_payload=payload)
            result.payload = apply_adaptive_sop(
                report_type=SIGNAL_TYPE,
                structured_payload=result.payload,
                work_item_payload=payload,
                enabled=False,
            ).payload
            result.metadata = sync_validation_metadata(result.metadata, result.payload)
            return result

        parsed = parse_work_item(work_item)
        parsed.warnings.extend(_retail_adjustment_warnings(parsed=parsed))
        gross_sales_derivation_warning = derive_gross_sales_from_payment_components(parsed.figures)
        if gross_sales_derivation_warning is not None:
            parsed.warnings.append(gross_sales_derivation_warning)
        elif parsed.figures.gross_sales is None:
            parsed.warnings.append(
                make_warning(
                    code="missing_fields",
                    severity="error",
                    message="Gross sales could not be mapped from the sales report.",
                )
            )
        sales_reconciliation = build_sales_reconciliation(
            metrics=parsed.figures.to_payload(),
            raw_text=_raw_sales_text(payload),
        )
        _derive_retail_adjustment_metrics(parsed.figures, reconciliation=sales_reconciliation)
        totals_validation = validate_totals(
            parsed.figures,
            reconciliation=None if candidate_only and sales_reconciliation.diagnostics_present else sales_reconciliation,
        )
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
            traffic=customer_metrics.traffic,
            served=customer_metrics.served,
            conversion_rate=customer_metrics.conversion_rate,
            labor_hours=parsed.figures.labor_hours,
        )
        till_reconciliation = reconcile_till_fields(
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
            + till_reconciliation.warnings
        )
        performance_alerts = dedupe_alerts(performance_metrics.alerts)
        performance_alert_payloads = [alert.to_payload() for alert in performance_alerts]
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
                    "net_sales": _candidate_metric_value(parsed.figures.net_sales, candidate_only=candidate_only),
                    "cash_sales": _candidate_metric_value(parsed.figures.cash_sales, candidate_only=candidate_only),
                    "eftpos_sales": _candidate_metric_value(parsed.figures.eftpos_sales, candidate_only=candidate_only),
                    "item_returns": _candidate_metric_value(parsed.figures.item_returns, candidate_only=candidate_only),
                    "cash_adjustment_return": _candidate_metric_value(
                        parsed.figures.item_returns,
                        candidate_only=candidate_only,
                    ),
                    "item_returns_total": _candidate_metric_value(parsed.figures.item_returns, candidate_only=candidate_only),
                    "total_returns": _candidate_metric_value(parsed.figures.item_returns, candidate_only=candidate_only),
                    "cash_over": _candidate_metric_value(parsed.figures.cash_over, candidate_only=candidate_only),
                    "cash_down": _candidate_metric_value(parsed.figures.cash_down, candidate_only=candidate_only),
                    "mobile_money_sales": _candidate_metric_value(parsed.figures.mobile_money_sales, candidate_only=candidate_only),
                    "till_total": _candidate_metric_value(parsed.figures.till_total, candidate_only=candidate_only),
                    "deposit_total": _candidate_metric_value(parsed.figures.deposit_total, candidate_only=candidate_only),
                    "z_reading": _candidate_metric_value(parsed.figures.z_reading, candidate_only=candidate_only),
                    "total_cash": _candidate_metric_value(parsed.figures.cash_sales, candidate_only=candidate_only),
                    "total_card": _candidate_metric_value(parsed.figures.eftpos_sales, candidate_only=candidate_only),
                    "total_sales": _candidate_metric_value(
                        parsed.figures.net_sales if parsed.figures.net_sales is not None else parsed.figures.gross_sales,
                        candidate_only=candidate_only,
                    ),
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
                "performance_alerts": performance_alert_payloads,
                "reconciliation": sales_reconciliation.to_payload(),
                **_primary_performance_alert_fields(performance_alert_payloads),
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

    result.payload = apply_adaptive_sop(
        report_type=SIGNAL_TYPE,
        structured_payload=result.payload,
        work_item_payload=payload,
        enabled=not candidate_only,
    ).payload
    result.metadata = sync_validation_metadata(result.metadata, result.payload)

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
                "net_sales": 0.0,
                "cash_sales": 0.0,
                "eftpos_sales": 0.0,
                "item_returns": 0.0,
                "cash_adjustment_return": 0.0,
                "item_returns_total": 0.0,
                "total_returns": 0.0,
                "cash_over": 0.0,
                "cash_down": 0.0,
                "mobile_money_sales": 0.0,
                "till_total": 0.0,
                "deposit_total": 0.0,
                "z_reading": 0.0,
                "total_cash": 0.0,
                "total_card": 0.0,
                "total_sales": 0.0,
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
            "performance_alerts": [],
            "reconciliation": empty_sales_reconciliation().to_payload(),
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
        "high_cash_variance",
        "missing_variance_reason",
    }
    status_warnings = [warning for warning in warnings if warning_impacts_status(warning)]
    if any(warning.code in critical_warning_codes for warning in status_warnings):
        return "needs_review"
    if status_warnings:
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


def _primary_performance_alert_fields(alerts: list[dict[str, str]]) -> dict[str, str]:
    """Return first-alert summary fields when a performance alert is present."""

    if not alerts:
        return {}
    first_alert = alerts[0]
    return {
        field_name: value
        for field_name in ("alert_type", "alert_level", "alert_message", "alert_category")
        if isinstance((value := first_alert.get(field_name)), str) and value.strip()
    }


def _apply_governance_result(result: AgentResult, write_result: object) -> None:
    """Project the persisted governance result back onto the live agent payload."""

    governance = getattr(write_result, "governance", None)
    if governance is None:
        return
    result.payload["status"] = governance.status
    result.payload["export_allowed"] = governance.export_allowed
    result.payload["governance"] = governance.to_payload()


def _raw_sales_text(payload: Mapping[str, object]) -> str | None:
    """Return raw sales text when present on the work item payload."""

    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return None
    for field_name in ("text", "normalized_text"):
        candidate = raw_message.get(field_name)
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    return None


def _derive_retail_adjustment_metrics(figures, *, reconciliation) -> None:
    """Populate adjustment-derived sales figures when enough information is present."""

    if figures.gross_sales is None or figures.item_returns is None:
        return

    derived_net_sales = round(figures.gross_sales - figures.item_returns, 2)
    if figures.net_sales is not None and abs(figures.net_sales - derived_net_sales) > TOLERANCE:
        return

    expected_total_sales = reconciliation.expected_total_sales
    if expected_total_sales is not None and abs(expected_total_sales - figures.gross_sales) <= TOLERANCE:
        figures.net_sales = figures.gross_sales
        return
    figures.net_sales = derived_net_sales


def _retail_adjustment_warnings(*, parsed) -> list[WarningEntry]:
    """Return retail-accounting warnings for returns and cash variance adjustments."""

    warnings: list[WarningEntry] = []
    item_returns = abs(parsed.figures.item_returns or 0.0)
    cash_over = abs(parsed.figures.cash_over or 0.0)
    cash_down = abs(parsed.figures.cash_down or 0.0)
    has_adjustment = item_returns > 0.0 or cash_over > 0.0 or cash_down > 0.0
    explained_adjustment = _has_adjustment_explanation(parsed)

    if item_returns > 0.0:
        warnings.append(
            make_warning(
                code="item_returns_present",
                severity="warning",
                message=(
                    f"Item returns of {item_returns:.2f} were accepted as a reconciliation adjustment "
                    "because Total Cash + Total Card + Items Return matched Total Sales."
                ),
            )
        )
    if cash_over > 0.0:
        warnings.append(
            make_warning(
                code="cash_over_present",
                severity="warning",
                message=f"Cash over of {cash_over:.2f} was applied in the retail reconciliation.",
            )
        )
    if cash_down > 0.0:
        warnings.append(
            make_warning(
                code="cash_down_present",
                severity="warning",
                message=f"Cash down of {cash_down:.2f} was applied in the retail reconciliation.",
            )
        )
    if item_returns > 50.0:
        warnings.append(
            make_warning(
                code="high_item_returns",
                severity="warning",
                message=f"Item returns are high at {item_returns:.2f}.",
            )
        )
    cash_variance_amount = max(cash_over, cash_down)
    if cash_variance_amount > 50.0:
        warnings.append(
            make_warning(
                code="high_cash_variance",
                severity="error",
                message=f"Cash variance is high at {cash_variance_amount:.2f}.",
            )
        )
    elif cash_variance_amount > 20.0:
        warnings.append(
            make_warning(
                code="high_cash_variance",
                severity="warning",
                message=f"Cash variance is elevated at {cash_variance_amount:.2f}.",
            )
        )
    if (cash_over > 0.0 or cash_down > 0.0) and not explained_adjustment:
        warnings.append(
            make_warning(
                code="missing_variance_reason",
                severity="error",
                message="Cash variance was present without a variance reason or supervisor confirmation.",
            )
        )
    if _same_operator(parsed.provenance.cashier, parsed.provenance.balanced_by):
        warnings.append(
            make_warning(
                code="balanced_by_same_as_cashier",
                severity="warning",
                message="Balanced By matches the cashier name.",
            )
        )
    if not has_adjustment:
        return warnings
    return warnings


def _has_adjustment_explanation(parsed) -> bool:
    """Return whether the parsed adjustment has explicit supporting context."""

    supervisor_confirmation = parsed.provenance.supervisor_confirmation or ""
    if _YES_NO_TEXT_PATTERN.search(supervisor_confirmation):
        return True
    notes = parsed.provenance.notes or []
    for note in notes:
        normalized = " ".join(note.casefold().split())
        if normalized.startswith("variance reason:") or normalized.startswith("return type:"):
            return True
        if "declined card" in normalized and "return" in normalized:
            return True
    return False


def _same_operator(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    return _normalize_operator_name(left) == _normalize_operator_name(right)


def _normalize_operator_name(value: str) -> str:
    return " ".join(value.casefold().split())
