"""Generate proposal-only learning artifacts from fallback provenance."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.proposal_store import write_proposal_record
from packages.report_policy import get_report_policy

_MIN_PATTERN_COUNT = 2


def generate_fallback_learning_proposals() -> list[str]:
    """Analyze repeated fallback provenance patterns and persist proposal files."""

    records = [record for record in _load_provenance_records() if _is_fallback_record(record)]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        report_type = str(record.get("report_type") or "unknown")
        acceptance = _acceptance_decision(record)
        reason = _acceptance_reason(record)
        grouped[(report_type, acceptance, reason)].append(record)

    written_paths: list[str] = []
    generated_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for (report_type, acceptance, reason), matches in grouped.items():
        if len(matches) < _MIN_PATTERN_COUNT:
            continue
        proposal_type, proposal_payload = _build_proposal(
            generated_date=generated_date,
            report_type=report_type,
            acceptance_decision=acceptance,
            acceptance_reason=reason,
            records=matches,
        )
        proposal_key = f"{acceptance}__{reason}"
        written_paths.append(
            write_proposal_record(
                generated_date=generated_date,
                report_type=report_type,
                proposal_type=proposal_type,
                proposal_key=proposal_key,
                payload=proposal_payload,
            )
        )
    return written_paths


def _build_proposal(
    *,
    generated_date: str,
    report_type: str,
    acceptance_decision: str,
    acceptance_reason: str,
    records: list[dict[str, Any]],
) -> tuple[str, dict[str, object]]:
    """Return proposal type and payload for a repeated fallback pattern."""

    if acceptance_decision == "review":
        proposal_type = "confidence_threshold_adjustment"
        recommendation = _threshold_adjustment_recommendation(report_type=report_type, records=records)
    elif acceptance_decision == "accept":
        proposal_type = "prompt_improvement"
        recommendation = {
            "summary": f"Strict {report_type} parsing missed patterns now recovered by fallback.",
            "suggested_change": "Capture these observed fallback-success patterns in strict parser prompts or examples.",
        }
    else:
        proposal_type = "parser_rule_candidate"
        recommendation = {
            "summary": f"Fallback extraction repeatedly failed for {report_type}.",
            "suggested_change": "Add or refine strict parser rules before fallback for the repeated failure pattern.",
        }

    return proposal_type, {
        "generated_date": generated_date,
        "proposal_type": proposal_type,
        "status": "proposal_only",
        "report_type": report_type,
        "pattern": {
            "parse_mode": "fallback",
            "acceptance_decision": acceptance_decision,
            "acceptance_reason": acceptance_reason,
        },
        "observation_count": len(records),
        "sample_raw_message_hashes": [str(record.get("raw_message_hash")) for record in records[:5]],
        "recommendation": recommendation,
    }


def _threshold_adjustment_recommendation(*, report_type: str, records: list[dict[str, Any]]) -> dict[str, object]:
    """Return a threshold-adjustment proposal from repeated review patterns."""

    policy = get_report_policy(report_type)
    confidences = [
        float(record["confidence"])
        for record in records
        if isinstance(record.get("confidence"), (int, float)) and not isinstance(record.get("confidence"), bool)
    ]
    average_confidence = round(sum(confidences) / len(confidences), 2) if confidences else None
    suggested_auto_accept = average_confidence
    if suggested_auto_accept is not None:
        suggested_auto_accept = max(policy.confidence_thresholds.review_min, min(suggested_auto_accept, 0.99))
    return {
        "summary": f"Repeated fallback review outcomes observed for {report_type}.",
        "current_thresholds": policy.confidence_thresholds.to_payload(),
        "suggested_thresholds": {
            "auto_accept_min": suggested_auto_accept,
            "review_min": policy.confidence_thresholds.review_min,
            "reject_max": policy.confidence_thresholds.reject_max,
        },
        "average_confidence": average_confidence,
    }


def _load_provenance_records() -> list[dict[str, Any]]:
    """Load all provenance JSON records from disk."""

    if not record_paths.PROVENANCE_DIR.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(record_paths.PROVENANCE_DIR.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _is_fallback_record(record: Mapping[str, Any]) -> bool:
    """Return whether a provenance record came from fallback parsing."""

    return record.get("parse_mode") == "fallback"


def _acceptance_decision(record: Mapping[str, Any]) -> str:
    """Return acceptance decision from provenance."""

    acceptance = record.get("acceptance_outcome")
    if isinstance(acceptance, Mapping):
        value = acceptance.get("decision") or acceptance.get("status")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unknown"


def _acceptance_reason(record: Mapping[str, Any]) -> str:
    """Return acceptance reason from provenance."""

    acceptance = record.get("acceptance_outcome")
    if isinstance(acceptance, Mapping):
        value = acceptance.get("reason")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unknown"
