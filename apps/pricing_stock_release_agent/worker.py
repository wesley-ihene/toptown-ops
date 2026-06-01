"""Worker for contract-driven pricing and stock release signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any

from apps.adaptive_sop_engine.worker import apply_adaptive_sop, sync_validation_metadata
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
from packages.data_governance import build_governance_context
from packages.report_policy import get_report_policy
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.validation import ValidationMetadata, normalize_rejections

AGENT_NAME = "pricing_stock_release_agent"
SIGNAL_TYPE = "pricing_stock_release"
OUTBOX_PATH = OUTBOX_DIR / AGENT_NAME
_CORRECTION_INTENT_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\bcorrection\s*/\s*replacement\s+report\b", flags=re.IGNORECASE),
        "correction_replacement_report",
    ),
    (
        re.compile(r"\bcorrection\s+report\b", flags=re.IGNORECASE),
        "correction_report",
    ),
    (
        re.compile(r"\breplacement\s+report\b", flags=re.IGNORECASE),
        "replacement_report",
    ),
    (
        re.compile(r"\breplaces\s+earlier\s+submitted\b", flags=re.IGNORECASE),
        "replaces_earlier_submitted",
    ),
    (
        re.compile(r"\bsupersede\s+previous\s+record\b", flags=re.IGNORECASE),
        "supersede_previous_record",
    ),
    (
        re.compile(r"\bretain\s+corrected\s+version\s+as\s+active\s+report\b", flags=re.IGNORECASE),
        "retain_corrected_version_as_active_report",
    ),
)
_BALE_ITEM_ROW_PATTERN = re.compile(r"^\s*#?\s*\d+\s*(?:\.\s*|\s+)\S", flags=re.MULTILINE)
_CORRECTION_FULL_REPORT_REQUIRED_MESSAGE = "Correction request detected, but full replacement report rows are required."


@dataclass(slots=True)
class PricingStockReleaseAgentWorker:
    """Specialist worker for bale-summary pricing and release signals."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item into a structured signal result."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return a structured pricing-stock-release result without raising."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    candidate_only = _candidate_mode_requested(payload)
    raw_text = _raw_text_from_payload(payload)
    correction_context = _correction_context(raw_text)
    try:
        validation_warnings = _validate_input(payload)
        if validation_warnings:
            result = _build_failure_result(
                work_item,
                warnings=validation_warnings,
                work_item_payload=payload,
                correction_context=correction_context,
            )
            result.payload = apply_adaptive_sop(
                report_type=SIGNAL_TYPE,
                structured_payload=result.payload,
                work_item_payload=payload,
                enabled=False,
            ).payload
            result.metadata = sync_validation_metadata(result.metadata, result.payload)
            _write_result_to_outbox(result)
            return result

        if correction_context["requested"] is True and correction_context["has_item_rows"] is False:
            result = _build_failure_result(
                work_item,
                warnings=[
                    make_warning(
                        code="correction_request_requires_full_report",
                        severity="error",
                        message=_CORRECTION_FULL_REPORT_REQUIRED_MESSAGE,
                    )
                ],
                work_item_payload=payload,
                correction_context=correction_context,
            )
            result.payload = apply_adaptive_sop(
                report_type=SIGNAL_TYPE,
                structured_payload=result.payload,
                work_item_payload=payload,
                enabled=False,
            ).payload
            result.metadata = sync_validation_metadata(result.metadata, result.payload)
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
        status = _determine_status(parsed=parsed, warnings=warnings)

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
                "provenance": _provenance_payload(parsed),
                "warnings": [warning.to_payload() for warning in warnings],
                "status": status,
            },
            metadata=_validation_metadata(
                status=status,
                warnings=warnings,
                work_item_payload=payload,
                correction_context=correction_context,
            ),
        )
        result.payload = apply_adaptive_sop(
            report_type=SIGNAL_TYPE,
            structured_payload=result.payload,
            work_item_payload=payload,
            enabled=not candidate_only,
        ).payload
        result.metadata = sync_validation_metadata(result.metadata, result.payload)
        if not candidate_only:
            write_result = write_structured_record(result.payload, metadata=result.metadata)
            _apply_governance_result(result, write_result)
        _write_result_to_outbox(result)
        return result
    except Exception:
        result = _build_failure_result(
            work_item,
            warnings=[
                make_warning(
                    code="parser_failure",
                    severity="error",
                    message="The bale summary could not be parsed safely.",
                )
            ],
            work_item_payload=payload,
            correction_context=correction_context,
            parser_failure=True,
        )
        result.payload = apply_adaptive_sop(
            report_type=SIGNAL_TYPE,
            structured_payload=result.payload,
            work_item_payload=payload,
            enabled=not candidate_only,
        ).payload
        result.metadata = sync_validation_metadata(result.metadata, result.payload)
        if not candidate_only:
            write_result = write_structured_record(result.payload, metadata=result.metadata)
            _apply_governance_result(result, write_result)
        _write_result_to_outbox(result)
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
    work_item_payload: Mapping[str, Any] | None = None,
    correction_context: Mapping[str, Any] | None = None,
    parser_failure: bool = False,
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
            "provenance": _provenance_payload(parsed),
            "warnings": [warning.to_payload() for warning in warning_list],
            "status": "invalid_input",
        },
        metadata=_validation_metadata(
            status="invalid_input",
            warnings=warning_list,
            work_item_payload=work_item_payload or {},
            correction_context=correction_context,
            parser_failure=parser_failure,
        ),
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
        confidence -= 0.04
    if not parsed.checked_by:
        confidence -= 0.04

    penalties = {
        "missing_fields": 0.25,
        "data_mismatch": 0.2,
        "financial_anomaly": 0.15,
        "approval_backlog": 0.05,
        "low_release_ratio": 0.1,
    }
    for warning in warnings:
        confidence -= penalties.get(warning.code, 0.0)

    confidence = round(max(confidence, 0.0), 2)
    if _preserves_auto_accept_confidence(parsed=parsed, warnings=warnings, status=status):
        floor = get_report_policy("pricing_stock_release").confidence_thresholds.auto_accept_min
        confidence = max(confidence, round(floor, 2))
    return round(confidence, 2)


def _determine_status(*, parsed: ParsedBaleSummary, warnings: list[WarningEntry]) -> str:
    """Return the final bale-summary status from critical vs non-critical warnings."""

    if not parsed.branch or not parsed.report_date or not parsed.items:
        return "needs_review"

    critical_warning_codes = {"missing_fields", "data_mismatch", "financial_anomaly"}
    if any(warning.severity == "error" for warning in warnings):
        return "needs_review"
    if any(
        warning.code in critical_warning_codes and warning.severity in {"warning", "error"}
        for warning in warnings
    ):
        return "needs_review"
    if warnings:
        return "accepted_with_warning"
    return "accepted"


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


def _validation_metadata(
    *,
    status: str,
    warnings: list[WarningEntry],
    work_item_payload: Mapping[str, Any],
    correction_context: Mapping[str, Any] | None = None,
    parser_failure: bool = False,
) -> dict[str, object]:
    """Return sidecar validation metadata for pricing-stock-release records."""

    correction_metadata = dict(correction_context) if isinstance(correction_context, Mapping) else {}
    governance_context = {
        **build_governance_context(work_item_payload),
        **_correction_governance_context(correction_metadata),
    }
    return {
        "validation": ValidationMetadata(
            stage=AGENT_NAME,
            status="passed" if status != "invalid_input" else "rejected",
            accepted=status != "invalid_input",
            rejections=normalize_rejections([warning.to_payload() for warning in warnings if warning.severity == "error"]),
            details={
                "final_status": status,
                "parser_failure": parser_failure,
                "correction_intent_detected": correction_metadata.get("requested") is True,
            },
        ).to_payload(),
        "governance_context": governance_context,
    }


def _candidate_mode_requested(payload: Mapping[str, Any]) -> bool:
    """Return whether this worker should stop at candidate generation."""

    return payload.get("governance_mode") == "candidate"


def _provenance_payload(parsed: ParsedBaleSummary | None) -> dict[str, Any]:
    """Return one stable provenance payload for pricing summaries."""

    provenance = {
        "prepared_by": parsed.prepared_by if parsed is not None else None,
        "role": parsed.role if parsed is not None else None,
    }
    if parsed is not None and parsed.checked_by is not None:
        provenance["checked_by"] = parsed.checked_by
    if parsed is not None and parsed.checked_role is not None:
        provenance["checked_role"] = parsed.checked_role
    return provenance


def _preserves_auto_accept_confidence(
    *,
    parsed: ParsedBaleSummary,
    warnings: list[WarningEntry],
    status: str,
) -> bool:
    """Return whether one accepted pricing parse should keep the auto-accept floor."""

    warning_codes = {warning.code for warning in warnings}
    return (
        status in {"accepted", "accepted_with_warning"}
        and parsed.branch is not None
        and parsed.report_date is not None
        and bool(parsed.items)
        and parsed.declared_total_qty is not None
        and parsed.declared_total_amount is not None
        and "totals_inferred" not in warning_codes
    )


def _apply_governance_result(result: AgentResult, write_result: object) -> None:
    """Project the persisted governance result back onto the live agent payload."""

    governance = getattr(write_result, "governance", None)
    if governance is None:
        return
    result.payload["status"] = governance.status
    result.payload["export_allowed"] = governance.export_allowed
    result.payload["governance"] = governance.to_payload()


def _raw_text_from_payload(payload: Mapping[str, Any]) -> str:
    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return ""
    for field_name in ("normalized_text", "text"):
        value = raw_message.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _correction_context(raw_text: str) -> dict[str, Any]:
    matched_labels: list[str] = []
    for pattern, label in _CORRECTION_INTENT_PATTERNS:
        if pattern.search(raw_text):
            matched_labels.append(label)

    return {
        "requested": bool(matched_labels),
        "labels": matched_labels,
        "reason": matched_labels[0] if matched_labels else None,
        "has_item_rows": bool(_BALE_ITEM_ROW_PATTERN.search(raw_text)),
    }


def _correction_governance_context(correction_context: Mapping[str, Any]) -> dict[str, Any]:
    if correction_context.get("requested") is not True:
        return {}
    return {
        "correction_intent_detected": True,
        "correction_intent_labels": list(correction_context.get("labels") or []),
        "replacement_reason": correction_context.get("reason"),
        "allow_same_scope_supersede": correction_context.get("has_item_rows") is True,
    }
