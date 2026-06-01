"""Runtime application of adaptive SOP learning onto structured payloads."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
import re
from typing import Any

from apps.adaptive_sop_engine.feedback import build_corrections_feedback
from apps.adaptive_sop_engine.learner import AdaptiveObservation, learn_variations
from apps.adaptive_sop_engine.registry import lookup_mapping
from apps.adaptive_sop_engine.rules import (
    AUTO_APPLY_CONFIDENCE,
    REGISTRY_VERSION,
    is_allowed_target,
    is_financial_target,
    normalize_token,
    payload_path_for_target,
    source_field_for_target,
    target_display_for_internal,
)
from apps.adaptive_sop_engine.storage import load_variation_registry
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "adaptive_sop_engine"
SIGNAL_TYPE = "adaptive_sop"
RUNTIME_STATUS = "LIVE_RUNTIME"
RUNTIME_OWNER = "adaptive_sop_engine"
RUNTIME_NOTE = (
    "Live adaptive SOP helper applied inline by active specialists; it is not "
    "a separately dispatched runtime route."
)
_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")


@dataclass(slots=True)
class AdaptiveSopOutcome:
    """Structured result from applying adaptive SOP learning to one payload."""

    payload: dict[str, Any]


@dataclass(slots=True)
class AdaptiveSopEngineWorker:
    """Worker wrapper for the adaptive SOP engine."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Process one work item through the adaptive engine."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Return one structured adaptive-engine result."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    structured_payload = payload.get("structured_payload")
    if not isinstance(structured_payload, Mapping):
        return AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "source_agent": AGENT_NAME,
                "status": "invalid_input",
                "message": "structured_payload is required for adaptive SOP processing.",
            },
        )

    report_type = _text_or_none(payload.get("report_type")) or _text_or_none(structured_payload.get("signal_type"))
    if report_type is None:
        return AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "source_agent": AGENT_NAME,
                "status": "invalid_input",
                "message": "report_type is required for adaptive SOP processing.",
            },
        )

    outcome = apply_adaptive_sop(
        report_type=report_type,
        structured_payload=structured_payload,
        work_item_payload=payload.get("work_item_payload") if isinstance(payload.get("work_item_payload"), Mapping) else payload,
        output_root=_text_or_none(payload.get("output_root")),
        enabled=payload.get("enabled") is not False,
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "status": "processed",
            "structured_payload": outcome.payload,
        },
    )


def empty_adaptive_sop_metadata() -> dict[str, Any]:
    """Return the default adaptive SOP metadata payload."""

    return {
        "corrections_applied": [],
        "observed_variations": [],
        "suggested_mappings": [],
        "registry_version": REGISTRY_VERSION,
    }


def sync_validation_metadata(
    metadata: Mapping[str, Any] | None,
    structured_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Return validation metadata synchronized to the current payload status."""

    synchronized = dict(metadata) if isinstance(metadata, Mapping) else {}
    validation = synchronized.get("validation")
    if not isinstance(validation, Mapping):
        return synchronized

    validation_payload = deepcopy(dict(validation))
    details = validation_payload.get("details")
    details_payload = dict(details) if isinstance(details, Mapping) else {}
    final_status = _text_or_none(structured_payload.get("status")) or details_payload.get("final_status")
    if final_status is not None:
        details_payload["final_status"] = final_status
        validation_payload["accepted"] = final_status != "invalid_input"
        validation_payload["status"] = "passed" if final_status != "invalid_input" else "rejected"
    validation_payload["details"] = details_payload
    synchronized["validation"] = validation_payload
    return synchronized


def apply_adaptive_sop(
    *,
    report_type: str,
    structured_payload: Mapping[str, Any],
    work_item_payload: Mapping[str, Any],
    output_root: str | None = None,
    enabled: bool = True,
) -> AdaptiveSopOutcome:
    """Apply adaptive SOP learning and safe approved mappings to one payload."""

    adapted_payload = deepcopy(dict(structured_payload))
    if not enabled or _text_or_none(adapted_payload.get("status")) == "invalid_input":
        adapted_payload["adaptive_sop"] = empty_adaptive_sop_metadata()
        return AdaptiveSopOutcome(payload=adapted_payload)

    raw_text = _raw_text_from_work_item(work_item_payload)
    report_date = _text_or_none(adapted_payload.get("report_date"))
    if raw_text is None or report_date is None:
        adapted_payload["adaptive_sop"] = empty_adaptive_sop_metadata()
        return AdaptiveSopOutcome(payload=adapted_payload)

    registry = load_variation_registry(output_root)
    observations = _collect_observations(
        report_type=report_type,
        structured_payload=adapted_payload,
        raw_text=raw_text,
        registry=registry,
    )

    learning = learn_variations(
        observations,
        registry=registry,
        output_root=output_root,
    )
    corrections = _apply_approved_observations(
        report_type=report_type,
        structured_payload=adapted_payload,
        observations=observations,
        registry=learning.registry,
    )
    if corrections and report_type == "sales_income":
        _refresh_sales_payload(adapted_payload)

    adaptive_sop = empty_adaptive_sop_metadata()
    adaptive_sop["corrections_applied"] = corrections
    adaptive_sop["observed_variations"] = learning.observed_variations
    adaptive_sop["suggested_mappings"] = learning.suggested_mappings
    if learning.registry_path is not None:
        adaptive_sop["registry_path"] = learning.registry_path
    if learning.review_queue_paths:
        adaptive_sop["review_queue_paths"] = list(learning.review_queue_paths)
    feedback_message = build_corrections_feedback(report_type, corrections)
    if feedback_message is not None:
        adaptive_sop["feedback_message"] = feedback_message
    adapted_payload["adaptive_sop"] = adaptive_sop
    return AdaptiveSopOutcome(payload=adapted_payload)


def _collect_observations(
    *,
    report_type: str,
    structured_payload: Mapping[str, Any],
    raw_text: str,
    registry: Mapping[str, Any],
) -> list[AdaptiveObservation]:
    if report_type == "sales_income":
        return _collect_sales_observations(structured_payload, raw_text, registry)
    if report_type == "pricing_stock_release":
        return _collect_pricing_observations(structured_payload, raw_text, registry)
    if report_type == "supervisor_control":
        return _collect_supervisor_observations(structured_payload, raw_text, registry)
    if report_type == "hr_attendance":
        return []
    return []


def _collect_sales_observations(
    structured_payload: Mapping[str, Any],
    raw_text: str,
    registry: Mapping[str, Any],
) -> list[AdaptiveObservation]:
    from apps.sales_income_agent.field_mapper import canonical_field_name
    from apps.sales_income_agent.normalizer import parse_count

    observations: list[AdaptiveObservation] = []
    branch = _text_or_none(structured_payload.get("branch"))
    report_date = _text_or_none(structured_payload.get("report_date"))

    for line in raw_text.splitlines():
        match = _KEY_VALUE_PATTERN.match(line)
        if match is None:
            continue
        raw_key = match.group(1).strip()
        raw_value = match.group(2).strip()
        target = None

        field_name = canonical_field_name(raw_key)
        if field_name is not None:
            target = target_display_for_internal("sales_income", field_name)
        if target is None:
            learned = lookup_mapping("sales_income", raw_key, registry=registry)
            if learned is not None:
                target = _text_or_none(learned.get("mapped_to"))
        if target is None:
            target = _sales_target_from_heuristics(raw_key)
        if not _should_track_variation("sales_income", raw_key, target):
            continue

        value: Any = None
        if target in {"Traffic", "Served"}:
            value = parse_count(raw_value)
        elif target in {"Monitor", "Cash_Variance_Reason"}:
            value = raw_value or None

        observations.append(
            AdaptiveObservation(
                report_type="sales_income",
                source_field=source_field_for_target("sales_income", target),
                source_label=raw_key,
                mapped_to=target,
                branch=branch,
                report_date=report_date,
                value=value,
                payload_path=payload_path_for_target("sales_income", target),
            )
        )
    return _dedupe_runtime_observations(observations)


def _collect_pricing_observations(
    structured_payload: Mapping[str, Any],
    raw_text: str,
    registry: Mapping[str, Any],
) -> list[AdaptiveObservation]:
    from packages.normalization.labels import internal_field_name

    observations: list[AdaptiveObservation] = []
    branch = _text_or_none(structured_payload.get("branch"))
    report_date = _text_or_none(structured_payload.get("report_date"))

    for line in raw_text.splitlines():
        match = _KEY_VALUE_PATTERN.match(line)
        if match is None:
            continue
        raw_key = match.group(1).strip()
        raw_value = match.group(2).strip()
        target = None

        field_name = internal_field_name(raw_key, report_family="bale_summary")
        if field_name is not None:
            target = target_display_for_internal("pricing_stock_release", field_name)
        if target is None:
            learned = lookup_mapping("pricing_stock_release", raw_key, registry=registry)
            if learned is not None:
                target = _text_or_none(learned.get("mapped_to"))
        if target is None:
            target = _pricing_target_from_heuristics(raw_key)
        if not _should_track_variation("pricing_stock_release", raw_key, target):
            continue

        value: Any = None
        if target in {"Prepared_By", "Checked_By"}:
            value = _clean_person_name(raw_value)
        elif target == "Qty":
            value = _parse_integer(raw_value)
        elif target == "Note":
            value = raw_value or None

        observations.append(
            AdaptiveObservation(
                report_type="pricing_stock_release",
                source_field=source_field_for_target("pricing_stock_release", target),
                source_label=raw_key,
                mapped_to=target,
                branch=branch,
                report_date=report_date,
                value=value,
                payload_path=payload_path_for_target("pricing_stock_release", target),
            )
        )
    return _dedupe_runtime_observations(observations)


def _collect_supervisor_observations(
    structured_payload: Mapping[str, Any],
    raw_text: str,
    registry: Mapping[str, Any],
) -> list[AdaptiveObservation]:
    observations: list[AdaptiveObservation] = []
    branch = _text_or_none(structured_payload.get("branch"))
    report_date = _text_or_none(structured_payload.get("report_date"))

    for line in raw_text.splitlines():
        match = _KEY_VALUE_PATTERN.match(line)
        if match is None:
            continue
        raw_key = match.group(1).strip()
        target = _supervisor_target_from_known_labels(raw_key)
        if target is None:
            learned = lookup_mapping("supervisor_control", raw_key, registry=registry)
            if learned is not None:
                target = _text_or_none(learned.get("mapped_to"))
        if target is None:
            continue
        if not _should_track_variation("supervisor_control", raw_key, target):
            continue
        observations.append(
            AdaptiveObservation(
                report_type="supervisor_control",
                source_field=source_field_for_target("supervisor_control", target),
                source_label=raw_key,
                mapped_to=target,
                branch=branch,
                report_date=report_date,
            )
        )
    return _dedupe_runtime_observations(observations)


def _apply_approved_observations(
    *,
    report_type: str,
    structured_payload: dict[str, Any],
    observations: list[AdaptiveObservation],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    corrections: list[dict[str, Any]] = []
    for observation in observations:
        approved = lookup_mapping(
            report_type,
            observation.source_label,
            registry=registry,
            statuses={"approved"},
        )
        if approved is None:
            continue
        mapped_to = _text_or_none(approved.get("mapped_to"))
        if mapped_to is None:
            continue
        if normalize_token(mapped_to) != normalize_token(observation.mapped_to):
            continue
        if not is_allowed_target(report_type, mapped_to) or is_financial_target(mapped_to):
            continue
        if float(approved.get("confidence", 0.0)) < AUTO_APPLY_CONFIDENCE:
            continue
        if observation.payload_path is None or _is_missing_value(observation.value):
            continue
        current_value = _get_payload_path(structured_payload, observation.payload_path)
        if not _is_missing_value(current_value):
            continue
        _set_payload_path(structured_payload, observation.payload_path, observation.value)
        corrections.append(
            {
                "source_label": observation.source_label,
                "mapped_to": mapped_to,
            }
        )
    return _dedupe_corrections(corrections)


def _refresh_sales_payload(structured_payload: dict[str, Any]) -> None:
    from apps.sales_income_agent.confidence import compute_confidence
    from apps.sales_income_agent.customer_metrics import evaluate_customer_metrics
    from apps.sales_income_agent.performance import compute_performance_metrics
    from apps.sales_income_agent.warnings import (
        MISSING_CUSTOMER_DATA_WARNING_CODE,
        PERFORMANCE_ALERT_WARNING_CODES,
        WarningEntry,
        dedupe_alerts,
        dedupe_warnings,
    )

    metrics = structured_payload.get("metrics")
    if not isinstance(metrics, dict):
        return

    traffic = _int_or_none(metrics.get("traffic"))
    served = _int_or_none(metrics.get("served"))
    gross_sales = _float_or_none(metrics.get("gross_sales"))
    branch = _text_or_none(structured_payload.get("branch"))
    report_date = _text_or_none(structured_payload.get("report_date"))
    if gross_sales is None or branch is None or report_date is None:
        return

    customer_metrics = evaluate_customer_metrics(traffic=traffic, served=served)
    performance_metrics = compute_performance_metrics(
        gross_sales=gross_sales,
        traffic=customer_metrics.traffic,
        served=customer_metrics.served,
        conversion_rate=customer_metrics.conversion_rate,
        labor_hours=None,
    )
    metrics["traffic"] = customer_metrics.traffic
    metrics["served"] = customer_metrics.served
    metrics["conversion_rate"] = customer_metrics.conversion_rate
    metrics["sales_per_customer"] = performance_metrics.sales_per_customer

    kept_warnings = [
        warning
        for warning in _warning_entries(structured_payload.get("warnings"))
        if warning.code not in {MISSING_CUSTOMER_DATA_WARNING_CODE, "data_mismatch"}
    ]
    refreshed_warnings = dedupe_warnings(kept_warnings + customer_metrics.warnings + performance_metrics.warnings)
    structured_payload["warnings"] = [warning.to_payload() for warning in refreshed_warnings]

    alerts = dedupe_alerts(performance_metrics.alerts)
    alert_payloads = [alert.to_payload() for alert in alerts]
    structured_payload["performance_alerts"] = alert_payloads
    for field_name in ("alert_type", "alert_level", "alert_message", "alert_category"):
        structured_payload.pop(field_name, None)
    if alert_payloads:
        first_alert = alert_payloads[0]
        for field_name in ("alert_type", "alert_level", "alert_message", "alert_category"):
            value = first_alert.get(field_name)
            if isinstance(value, str) and value.strip():
                structured_payload[field_name] = value.strip()

    status_warnings = [warning for warning in refreshed_warnings if warning.code not in PERFORMANCE_ALERT_WARNING_CODES]
    critical_warning_codes = {"missing_fields", "invalid_totals", "data_mismatch", "financial_anomaly"}
    if any(warning.code in critical_warning_codes for warning in status_warnings):
        structured_payload["status"] = "needs_review"
    elif status_warnings:
        structured_payload["status"] = "accepted_with_warning"
    else:
        structured_payload["status"] = "accepted"

    structured_payload["confidence"] = compute_confidence(
        branch=branch,
        report_date=report_date,
        totals_found=gross_sales is not None,
        critical_fields_complete=bool(branch and report_date),
        warnings=refreshed_warnings,
    )


def _warning_entries(value: object) -> list[Any]:
    from apps.sales_income_agent.warnings import WarningEntry

    if not isinstance(value, list):
        return []
    entries: list[WarningEntry] = []
    for warning in value:
        if not isinstance(warning, Mapping):
            continue
        code = _text_or_none(warning.get("code"))
        severity = _text_or_none(warning.get("severity"))
        message = _text_or_none(warning.get("message"))
        if code is None or severity is None or message is None:
            continue
        entries.append(WarningEntry(code=code, severity=severity, message=message))
    return entries


def _raw_text_from_work_item(payload: Mapping[str, Any]) -> str | None:
    raw_message = payload.get("raw_message")
    if not isinstance(raw_message, Mapping):
        return None
    for field_name in ("text", "normalized_text"):
        candidate = raw_message.get(field_name)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _sales_target_from_heuristics(raw_key: str) -> str | None:
    normalized = normalize_token(raw_key)
    if any(token in normalized for token in ("main door", "door count", "foot traffic", "walk in", "walkin", "traffic")):
        return "Traffic"
    if "customer" in normalized and "serve" in normalized:
        return "Served"
    if "guest" in normalized and "serve" in normalized:
        return "Served"
    if "served" in normalized:
        return "Served"
    if "monitor" in normalized:
        return "Monitor"
    if "variance" in normalized and "reason" in normalized:
        return "Cash_Variance_Reason"
    return None


def _pricing_target_from_heuristics(raw_key: str) -> str | None:
    normalized = normalize_token(raw_key)
    if normalized in {"prepared by", "preparedby"}:
        return "Prepared_By"
    if normalized in {"checked by", "checkedby"}:
        return "Checked_By"
    if normalized in {"qty", "quantity"}:
        return "Qty"
    if "weight" in normalized or "kg" == normalized:
        return "Weight"
    if normalized in {"note", "notes", "remark", "remarks"}:
        return "Note"
    if normalized in {"item", "item name"}:
        return "Item"
    return None


def _supervisor_target_from_known_labels(raw_key: str) -> str | None:
    normalized = normalize_token(raw_key)
    known_labels = {
        "cash variance": "Cash variance",
        "staffing issues": "Staffing issues",
        "staffing issue": "Staffing issues",
        "stock issues affecting sales": "Stock issues affecting sales",
        "stock issue": "Stock issues affecting sales",
        "stock issues": "Stock issues affecting sales",
        "pricing system issues": "Pricing/system issues",
        "pricing or system issues": "Pricing/system issues",
        "pricing system issue": "Pricing/system issues",
        "exceptions escalated to ops manager": "Escalation",
        "exceptions": "Escalation",
        "escalation": "Escalation",
    }
    return known_labels.get(normalized)


def _should_track_variation(report_type: str, raw_key: str, target: str | None) -> bool:
    if target is None:
        return False
    if not is_allowed_target(report_type, target):
        return False
    if is_financial_target(target):
        return False
    return normalize_token(raw_key) != normalize_token(target)


def _get_payload_path(payload: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _set_payload_path(payload: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    current = payload
    for key in path[:-1]:
        node = current.get(key)
        if not isinstance(node, dict):
            node = {}
            current[key] = node
        current = node
    current[path[-1]] = value


def _dedupe_runtime_observations(observations: list[AdaptiveObservation]) -> list[AdaptiveObservation]:
    unique: dict[tuple[str, str, str, str, str], AdaptiveObservation] = {}
    for observation in observations:
        key = (
            normalize_token(observation.report_type),
            normalize_token(observation.source_field),
            normalize_token(observation.source_label),
            normalize_token(observation.mapped_to),
            observation.report_date or "",
        )
        unique.setdefault(key, observation)
    return list(unique.values())


def _dedupe_corrections(corrections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for correction in corrections:
        key = (
            normalize_token(correction.get("source_label")),
            normalize_token(correction.get("mapped_to")),
        )
        unique.setdefault(key, correction)
    return list(unique.values())


def _clean_person_name(raw_value: str) -> str | None:
    value = raw_value.strip()
    for separator in (" - ", " / ", ", "):
        if separator in value:
            value = value.split(separator, 1)[0]
            break
    if "(" in value:
        value = value.split("(", 1)[0]
    cleaned = " ".join(value.strip().split())
    return cleaned or None


def _parse_integer(raw_value: str) -> int | None:
    digits = "".join(character for character in raw_value if character.isdigit())
    if not digits:
        return None
    return int(digits)


def _float_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    return False


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
