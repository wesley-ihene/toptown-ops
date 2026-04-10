"""Persist provenance separately from business payload records."""

from __future__ import annotations

from typing import Any

from packages.record_store.naming import safe_segment
from packages.record_store.paths import get_provenance_path
from packages.record_store.writer import write_json_file
from packages.observability import record_processing_event


def write_provenance_record(
    *,
    outcome: str,
    report_type: str,
    branch: str,
    report_date: str,
    raw_message_hash: str,
    parser_used: str,
    parse_mode: str,
    confidence: float | None,
    warnings: list[dict[str, Any]],
    validation_outcome: dict[str, Any],
    acceptance_outcome: dict[str, Any],
    downstream_references: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> str:
    """Write one provenance record and return its path."""

    provenance_path = get_provenance_path(outcome, report_date, branch, report_type) / f"{safe_segment(raw_message_hash)}.json"
    payload = {
        "outcome": outcome,
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "raw_message_hash": raw_message_hash,
        "parser_used": parser_used,
        "parse_mode": parse_mode,
        "confidence": confidence,
        "warnings": warnings,
        "validation_outcome": validation_outcome,
        "acceptance_outcome": acceptance_outcome,
        "downstream_references": downstream_references,
    }
    if extra:
        payload.update(extra)
    write_json_file(provenance_path, payload)
    record_processing_event(
        report_date=report_date,
        branch=branch,
        report_type=report_type,
        outcome=outcome,
        parse_mode=parse_mode,
        parser_used=parser_used,
        confidence=confidence,
        warnings=[warning for warning in warnings if isinstance(warning, dict)],
    )
    if parse_mode == "fallback":
        from packages.review_learning import generate_fallback_learning_proposals

        generate_fallback_learning_proposals()
    return str(provenance_path)
