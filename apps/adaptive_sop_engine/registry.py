"""Registry helpers for adaptive SOP variation mappings."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from apps.adaptive_sop_engine.rules import (
    AUTO_APPLY_CONFIDENCE,
    is_allowed_target,
    is_financial_target,
    normalize_token,
    source_field_for_target,
)
from apps.adaptive_sop_engine.storage import load_variation_registry, write_variation_registry

_STATUS_RANK = {
    "approved": 4,
    "suggested": 3,
    "observed": 2,
    "rejected": 1,
}


def list_entries(
    *,
    registry: Mapping[str, Any] | None = None,
    output_root: str | None = None,
) -> list[dict[str, Any]]:
    """Return the registry flattened into deterministic rows."""

    loaded = registry if registry is not None else load_variation_registry(output_root)
    rows: list[dict[str, Any]] = []
    if not isinstance(loaded, Mapping):
        return rows

    for report_type in sorted(loaded, key=normalize_token):
        report_bucket = loaded.get(report_type)
        if not isinstance(report_bucket, Mapping):
            continue
        for source_field in sorted(report_bucket, key=normalize_token):
            source_bucket = report_bucket.get(source_field)
            if not isinstance(source_bucket, Mapping):
                continue
            for source_label in sorted(source_bucket, key=normalize_token):
                entry = source_bucket.get(source_label)
                if not isinstance(entry, Mapping):
                    continue
                rows.append(_flatten_entry(report_type, source_field, source_label, entry))
    return rows


def lookup_mapping(
    report_type: str,
    source_label: str,
    *,
    registry: Mapping[str, Any] | None = None,
    statuses: set[str] | None = None,
    output_root: str | None = None,
) -> dict[str, Any] | None:
    """Return one best registry row for a source label when unambiguous."""

    normalized_report_type = normalize_token(report_type)
    normalized_source_label = normalize_token(source_label)
    matches = [
        row
        for row in list_entries(registry=registry, output_root=output_root)
        if normalize_token(row.get("report_type")) == normalized_report_type
        and normalize_token(row.get("source_label")) == normalized_source_label
        and (statuses is None or row.get("status") in statuses)
    ]
    if not matches:
        return None

    unique_targets = {normalize_token(row.get("mapped_to")) for row in matches}
    if len(unique_targets) != 1:
        return None

    matches.sort(
        key=lambda row: (
            -_STATUS_RANK.get(str(row.get("status") or ""), 0),
            -int(row.get("count") or 0),
            normalize_token(row.get("source_field")),
            normalize_token(row.get("source_label")),
        )
    )
    return dict(matches[0])


def update_mapping_status(
    report_type: str,
    source_label: str,
    *,
    status: str,
    target: str | None = None,
    confidence: float | None = None,
    output_root: str | None = None,
) -> dict[str, Any]:
    """Approve or reject one adaptive mapping safely."""

    normalized_report_type = _required_text(report_type, field_name="report_type")
    normalized_source_label = _required_text(source_label, field_name="source")
    normalized_status = _required_text(status, field_name="status")
    if normalized_status not in {"approved", "rejected"}:
        raise ValueError("adaptive mapping status must be `approved` or `rejected`")

    registry = load_variation_registry(output_root)
    matches = _matching_locations(registry, normalized_report_type, normalized_source_label)
    today = _today()

    if normalized_status == "approved":
        resolved_target = _required_text(target, field_name="target")
        if not is_allowed_target(normalized_report_type, resolved_target):
            raise ValueError(f"target `{resolved_target}` is not allowed for `{normalized_report_type}`")
        if is_financial_target(resolved_target):
            raise ValueError(f"target `{resolved_target}` is blocked by the financial safety boundary")

        if matches:
            source_field, stored_label = _selected_match(matches, target=resolved_target)
        else:
            source_field = source_field_for_target(normalized_report_type, resolved_target)
            stored_label = normalized_source_label
            registry.setdefault(normalized_report_type, {}).setdefault(source_field, {})[stored_label] = {}

        entry = registry.setdefault(normalized_report_type, {}).setdefault(source_field, {}).setdefault(stored_label, {})
        entry["mapped_to"] = resolved_target
        entry["count"] = int(entry.get("count", 0))
        entry["first_seen"] = _date_text_or_default(entry.get("first_seen"), today)
        entry["last_seen"] = today
        entry["status"] = "approved"
        entry["confidence"] = round(max(float(confidence if confidence is not None else 0.94), AUTO_APPLY_CONFIDENCE), 2)
        entry["report_type"] = normalized_report_type
        entry["source_field"] = source_field
        entry["last_report_date"] = today
        entry["last_branch"] = _text_or_none(entry.get("last_branch"))
    else:
        if not matches:
            raise ValueError(f"adaptive mapping `{normalized_source_label}` not found for `{normalized_report_type}`")
        for source_field, stored_label in matches:
            entry = registry[normalized_report_type][source_field][stored_label]
            if not isinstance(entry, dict):
                entry = {}
                registry[normalized_report_type][source_field][stored_label] = entry
            entry["mapped_to"] = _text_or_none(entry.get("mapped_to"))
            entry["count"] = int(entry.get("count", 0))
            entry["first_seen"] = _date_text_or_default(entry.get("first_seen"), today)
            entry["last_seen"] = today
            entry["status"] = "rejected"
            entry["confidence"] = 0.0
            entry["report_type"] = normalized_report_type
            entry["source_field"] = source_field
            entry["last_report_date"] = today
            entry["last_branch"] = _text_or_none(entry.get("last_branch"))

    write_variation_registry(registry, output_root=output_root)
    updated = lookup_mapping(
        normalized_report_type,
        normalized_source_label,
        registry=registry,
        output_root=output_root,
    )
    if updated is None:
        updated_rows = [
            _flatten_entry(normalized_report_type, source_field, stored_label, registry[normalized_report_type][source_field][stored_label])
            for source_field, stored_label in matches
        ]
        if not updated_rows:
            raise ValueError("adaptive mapping update could not be resolved safely")
        return updated_rows[0]
    return updated


def _matching_locations(
    registry: Mapping[str, Any],
    report_type: str,
    source_label: str,
) -> list[tuple[str, str]]:
    report_bucket = registry.get(report_type)
    if not isinstance(report_bucket, Mapping):
        return []
    normalized_source_label = normalize_token(source_label)
    matches: list[tuple[str, str]] = []
    for source_field, source_bucket in report_bucket.items():
        if not isinstance(source_bucket, Mapping):
            continue
        for stored_label in source_bucket:
            if normalize_token(stored_label) == normalized_source_label:
                matches.append((str(source_field), str(stored_label)))
    matches.sort(key=lambda item: (normalize_token(item[0]), normalize_token(item[1])))
    return matches


def _selected_match(matches: list[tuple[str, str]], *, target: str) -> tuple[str, str]:
    del target
    return matches[0]


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
        "mapped_to": _text_or_none(entry.get("mapped_to")),
        "count": int(entry.get("count", 0)),
        "first_seen": _date_text_or_default(entry.get("first_seen"), _today()),
        "last_seen": _date_text_or_default(entry.get("last_seen"), _today()),
        "status": _text_or_none(entry.get("status")) or "observed",
        "confidence": round(float(entry.get("confidence", 0.0)), 2),
        "last_branch": _text_or_none(entry.get("last_branch")),
        "last_report_date": _date_text_or_default(entry.get("last_report_date"), _today()),
    }


def _required_text(value: object, *, field_name: str) -> str:
    text = _text_or_none(value)
    if text is None:
        raise ValueError(f"adaptive mapping field `{field_name}` must be a non-empty string")
    return text


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _date_text_or_default(value: object, default: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return default


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()
