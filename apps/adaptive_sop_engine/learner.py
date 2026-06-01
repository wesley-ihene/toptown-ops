"""Observation and suggestion logic for adaptive SOP learning."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from apps.adaptive_sop_engine.feedback import build_suggestion_message
from apps.adaptive_sop_engine.registry import lookup_mapping
from apps.adaptive_sop_engine.rules import (
    SUGGESTION_THRESHOLD,
    is_allowed_target,
    is_financial_target,
    normalize_token,
)
from apps.adaptive_sop_engine.storage import load_variation_registry, write_review_queue, write_variation_registry


@dataclass(frozen=True, slots=True)
class AdaptiveObservation:
    """One runtime observation of a non-standard label and inferred target."""

    report_type: str
    source_field: str
    source_label: str
    mapped_to: str
    branch: str | None
    report_date: str | None
    value: Any = None
    payload_path: tuple[str, ...] | None = None


@dataclass(slots=True)
class LearningOutcome:
    """Result of one learning pass over observed adaptive variations."""

    registry: dict[str, Any]
    observed_variations: list[dict[str, Any]]
    suggested_mappings: list[dict[str, Any]]
    registry_path: str | None = None
    review_queue_paths: list[str] | None = None


def learn_variations(
    observations: Sequence[AdaptiveObservation],
    *,
    registry: Mapping[str, Any] | None = None,
    output_root: str | None = None,
) -> LearningOutcome:
    """Persist one learning pass and return updated adaptive metadata."""

    working_registry = dict(registry) if isinstance(registry, Mapping) else load_variation_registry(output_root)
    observed_variations: list[dict[str, Any]] = []
    suggested_mappings: list[dict[str, Any]] = []

    for observation in _dedupe_observations(observations):
        if not is_allowed_target(observation.report_type, observation.mapped_to):
            continue
        if is_financial_target(observation.mapped_to):
            continue

        observed_row, suggested_row = _record_observation(working_registry, observation)
        if observed_row is not None:
            observed_variations.append(observed_row)
        if suggested_row is not None:
            suggested_mappings.append(suggested_row)

    registry_path = None
    review_queue_paths: list[str] = []
    if observed_variations:
        registry_path = write_variation_registry(working_registry, output_root=output_root)

    if suggested_mappings:
        by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for suggestion in suggested_mappings:
            suggestion_date = _date_or_default(suggestion.get("last_seen"))
            by_date[suggestion_date].append(suggestion)
        for suggestion_date, dated_suggestions in sorted(by_date.items()):
            review_queue_paths.append(
                write_review_queue(
                    suggestion_date,
                    dated_suggestions,
                    output_root=output_root,
                )
            )

    return LearningOutcome(
        registry=working_registry,
        observed_variations=observed_variations,
        suggested_mappings=suggested_mappings,
        registry_path=registry_path,
        review_queue_paths=review_queue_paths or None,
    )


def _record_observation(
    registry: dict[str, Any],
    observation: AdaptiveObservation,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    report_type = observation.report_type
    source_field = observation.source_field
    source_label = observation.source_label
    mapped_to = observation.mapped_to
    report_date = observation.report_date or _today()

    report_bucket = registry.setdefault(report_type, {})
    source_bucket = report_bucket.setdefault(source_field, {})
    stored_label = _existing_label_key(source_bucket, source_label) or source_label
    entry = source_bucket.setdefault(stored_label, {})
    if _conflicts_with_existing_target(entry, mapped_to):
        return None, None

    entry["mapped_to"] = mapped_to
    entry["count"] = int(entry.get("count", 0)) + 1
    entry["first_seen"] = _date_or_default(entry.get("first_seen"), report_date)
    entry["last_seen"] = report_date
    entry["report_type"] = report_type
    entry["source_field"] = source_field
    entry["last_branch"] = observation.branch.strip() if isinstance(observation.branch, str) and observation.branch.strip() else None
    entry["last_report_date"] = report_date

    status = str(entry.get("status") or "observed")
    if status == "approved":
        entry["confidence"] = round(max(float(entry.get("confidence", 0.0)), 0.85), 2)
    elif status == "rejected":
        entry["confidence"] = 0.0
    else:
        status = "suggested" if int(entry["count"]) >= SUGGESTION_THRESHOLD else "observed"
        entry["status"] = status
        entry["confidence"] = _confidence_for_count(int(entry["count"]), status=status)

    flattened = _flatten_entry(report_type, source_field, stored_label, entry)
    suggested = None
    if flattened["status"] == "suggested":
        suggested = dict(flattened)
        suggested["review_message"] = build_suggestion_message(stored_label, mapped_to)
    return flattened, suggested


def _existing_label_key(source_bucket: Mapping[str, Any], source_label: str) -> str | None:
    normalized_source_label = normalize_token(source_label)
    for stored_label in source_bucket:
        if normalize_token(stored_label) == normalized_source_label:
            return str(stored_label)
    return None


def _conflicts_with_existing_target(entry: Mapping[str, Any], mapped_to: str) -> bool:
    existing = entry.get("mapped_to")
    if not isinstance(existing, str) or not existing.strip():
        return False
    return normalize_token(existing) != normalize_token(mapped_to)


def _flatten_entry(
    report_type: str,
    source_field: str,
    source_label: str,
    entry: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "report_type": report_type,
        "source_field": source_field,
        "source_label": source_label,
        "mapped_to": str(entry.get("mapped_to")),
        "count": int(entry.get("count", 0)),
        "first_seen": _date_or_default(entry.get("first_seen")),
        "last_seen": _date_or_default(entry.get("last_seen")),
        "status": str(entry.get("status") or "observed"),
        "confidence": round(float(entry.get("confidence", 0.0)), 2),
        "last_branch": entry.get("last_branch"),
        "last_report_date": _date_or_default(entry.get("last_report_date")),
    }


def _dedupe_observations(observations: Sequence[AdaptiveObservation]) -> list[AdaptiveObservation]:
    unique: dict[tuple[str, str, str, str, str, str], AdaptiveObservation] = {}
    for observation in observations:
        key = (
            normalize_token(observation.report_type),
            normalize_token(observation.source_field),
            normalize_token(observation.source_label),
            normalize_token(observation.mapped_to),
            normalize_token(observation.branch or ""),
            observation.report_date or "",
        )
        unique.setdefault(key, observation)
    return list(unique.values())


def _confidence_for_count(count: int, *, status: str) -> float:
    base = 0.55 + max(count - 1, 0) * 0.1
    if status == "suggested":
        base = min(base, 0.84)
    return round(min(base, 0.84), 2)


def _date_or_default(value: object, default: str | None = None) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return default or _today()


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()

