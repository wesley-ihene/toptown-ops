"""Worker for contract-driven pricing and stock release signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from apps.pricing_stock_release_agent.approval import interpret_approval
from apps.pricing_stock_release_agent.parser import (
    ParsedBaleSummary,
    parse_work_item,
)
from apps.pricing_stock_release_agent.pricing import interpret_pricing
from apps.pricing_stock_release_agent.record_store import write_structured_record
from apps.pricing_stock_release_agent.stock_flow import interpret_stock_flow
from apps.pricing_stock_release_agent.throughput import interpret_throughput
from apps.pricing_stock_release_agent.warnings import WarningEntry, dedupe_warnings, make_warning
from packages.common.paths import OUTBOX_DIR
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "pricing_stock_release_agent"
SIGNAL_TYPE = "pricing_stock_release"
OUTBOX_PATH = OUTBOX_DIR / AGENT_NAME


@dataclass(slots=True)
class PricingStockReleaseAgentWorker:
    """Specialist worker for bale-summary pricing and release signals."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured signal result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a structured pricing-stock-release result without raising."""

    try:
        payload = work_item.payload if isinstance(work_item.payload, dict) else {}
        validation_warnings = _validate_input(payload)
        if validation_warnings:
            result = _build_failure_result(work_item, warnings=validation_warnings)
            _write_result_to_outbox(result)
            return result

        parsed = parse_work_item(work_item)
        stock_flow = interpret_stock_flow(parsed)
        pricing = interpret_pricing(parsed)
        approval = interpret_approval(stock_flow)
        throughput = interpret_throughput(stock_flow)

        warnings = dedupe_warnings(
            parsed.warnings
            + stock_flow.warnings
            + pricing.warnings
            + approval.warnings
            + throughput.warnings
        )
        status = "ready" if not warnings else "needs_review"

        result = AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "source_agent": AGENT_NAME,
                "branch": parsed.branch,
                "report_date": parsed.report_date,
                "confidence": _compute_confidence(parsed=parsed, warnings=warnings, status=status),
                "metrics": {
                    "bales_processed": stock_flow.bales_processed,
                    "bales_released": stock_flow.bales_released,
                    "bales_pending_approval": approval.bales_pending_approval,
                    "total_qty": stock_flow.total_qty,
                    "total_amount": pricing.total_amount,
                    "release_ratio": throughput.release_ratio,
                },
                "items": [
                    {
                        "bale_id": item.bale_id,
                        "item_name": item.item_name,
                        "qty": item.qty,
                        "amount": item.amount,
                        "price_per_piece": item.price_per_piece,
                    }
                    for item in pricing.items
                ],
                "provenance": {
                    "prepared_by": parsed.prepared_by,
                    "role": parsed.role,
                },
                "warnings": [warning.to_payload() for warning in warnings],
                "status": status,
            },
        )
        _write_result_to_outbox(result)
        write_structured_record(result.payload)
        return result
    except Exception:
        result = _build_failure_result(
            work_item,
            warnings=[
                make_warning(
                    code="missing_fields",
                    severity="error",
                    message="The work item could not be processed safely.",
                )
            ],
        )
        _write_result_to_outbox(result)
        write_structured_record(result.payload)
        return result


def _validate_input(payload: dict[str, Any]) -> list[WarningEntry]:
    """Validate the strict input contract for routed bale-summary items."""

    warnings: list[WarningEntry] = []
    classification = payload.get("classification")
    raw_message = payload.get("raw_message")

    if not isinstance(classification, Mapping) or classification.get("report_type") != "bale_summary":
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item classification must be `bale_summary`.",
            )
        )

    if not isinstance(raw_message, Mapping):
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="The work item raw_message must be a mapping with a `text` field.",
            )
        )
    else:
        text = raw_message.get("text")
        if not isinstance(text, str) or not text.strip():
            warnings.append(
                make_warning(
                    code="missing_fields",
                    severity="error",
                    message="The work item raw_message.text field must be a non-empty string.",
                )
            )

    return dedupe_warnings(warnings)


def _build_failure_result(
    work_item: WorkItem,
    *,
    parsed: ParsedBaleSummary | None = None,
    warnings: list[WarningEntry] | None = None,
) -> AgentResult:
    """Return a safe failure result that still matches the output contract."""

    del work_item
    warning_list = dedupe_warnings(
        warnings
        or [
            make_warning(
                code="missing_fields",
                severity="error",
                message="The bale summary input was incomplete or invalid.",
            )
        ]
    )

    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "branch": parsed.branch if parsed is not None else None,
            "report_date": parsed.report_date if parsed is not None else None,
            "confidence": 0.0,
            "metrics": {
                "bales_processed": 0,
                "bales_released": 0,
                "bales_pending_approval": 0,
                "total_qty": 0,
                "total_amount": 0.0,
                "release_ratio": 0.0,
            },
            "items": [],
            "provenance": {
                "prepared_by": parsed.prepared_by if parsed is not None else None,
                "role": parsed.role if parsed is not None else None,
            },
            "warnings": [warning.to_payload() for warning in warning_list],
            "status": "invalid_input",
        },
    )


def _compute_confidence(
    *,
    parsed: ParsedBaleSummary,
    warnings: list[WarningEntry],
    status: str,
) -> float:
    """Return a conservative confidence score for the structured result."""

    if status == "invalid_input":
        return 0.0

    confidence = 1.0
    if not parsed.branch:
        confidence -= 0.15
    if not parsed.report_date:
        confidence -= 0.15
    if not parsed.prepared_by:
        confidence -= 0.1
    if not parsed.role:
        confidence -= 0.1

    penalties = {
        "missing_fields": 0.25,
        "data_mismatch": 0.2,
        "financial_anomaly": 0.15,
        "approval_backlog": 0.05,
        "low_release_ratio": 0.1,
    }
    for warning in warnings:
        confidence -= penalties.get(warning.code, 0.0)

    return round(max(confidence, 0.0), 2)


def _write_result_to_outbox(result: AgentResult) -> Path:
    """Persist the agent result payload to the pricing-stock-release outbox."""

    OUTBOX_PATH.mkdir(parents=True, exist_ok=True)
    output_path = OUTBOX_PATH / _build_output_filename(result.payload)
    temp_path = output_path.with_suffix(".json.tmp")
    temp_path.write_text(
        json.dumps(result.payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(output_path)
    return output_path


def _build_output_filename(payload: dict[str, Any]) -> str:
    """Return a stable outbox filename for an agent payload."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    branch = _sanitize_filename_component(payload.get("branch"))
    return f"{timestamp}__{branch}__pricing_stock_release.json"


def _sanitize_filename_component(value: Any) -> str:
    """Return a filesystem-safe lowercase filename component."""

    if not isinstance(value, str) or not value.strip():
        return "unknown_branch"

    normalized = []
    for character in value.strip().lower():
        if character.isalnum():
            normalized.append(character)
        elif character in {" ", "-", "_"}:
            normalized.append("_")

    cleaned = "".join(normalized).strip("_")
    return cleaned or "unknown_branch"
