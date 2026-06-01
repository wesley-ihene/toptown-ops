"""File-backed storage for adaptive SOP learning artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
import json
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.record_store.writer import write_json_file

from apps.adaptive_sop_engine.rules import REGISTRY_VERSION, normalize_token


def sop_learning_root(output_root: str | Path | None = None) -> Path:
    """Return the root directory for adaptive SOP learning artifacts."""

    records_root = record_paths.RECORDS_DIR if output_root is None else Path(output_root) / "records"
    return records_root / "sop_learning"


def variation_registry_path(output_root: str | Path | None = None) -> Path:
    """Return the canonical path for the adaptive variation registry."""

    return sop_learning_root(output_root) / "variation_registry.json"


def review_queue_path(report_date: str, output_root: str | Path | None = None) -> Path:
    """Return the canonical review-queue export path for one report date."""

    return sop_learning_root(output_root) / "review_queue" / f"{report_date}.json"


def load_variation_registry(output_root: str | Path | None = None) -> dict[str, Any]:
    """Return the adaptive variation registry, or an empty mapping when absent."""

    path = variation_registry_path(output_root)
    if not path.exists():
        return {}
    payload = _read_json(path)
    return dict(payload) if isinstance(payload, Mapping) else {}


def write_variation_registry(
    registry: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> str:
    """Persist one adaptive variation registry deterministically."""

    path = variation_registry_path(output_root)
    write_json_file(path, deepcopy(dict(registry)))
    return str(path)


def write_review_queue(
    report_date: str,
    suggestions: Sequence[Mapping[str, Any]],
    *,
    output_root: str | Path | None = None,
) -> str:
    """Merge and persist one daily suggested-mapping review queue artifact."""

    path = review_queue_path(report_date, output_root)
    existing = _read_json(path) if path.exists() else {}
    merged = _merged_suggestions(existing.get("pending_suggestions"), suggestions)
    payload = {
        "date": report_date,
        "registry_version": REGISTRY_VERSION,
        "pending_suggestions": merged,
    }
    write_json_file(path, payload)
    return str(path)


def _merged_suggestions(
    existing: object,
    incoming: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for entry in _iter_suggestions(existing):
        merged[_suggestion_key(entry)] = entry
    for entry in _iter_suggestions(incoming):
        merged[_suggestion_key(entry)] = entry
    return sorted(
        merged.values(),
        key=lambda item: (
            normalize_token(item.get("report_type")),
            normalize_token(item.get("source_field")),
            normalize_token(item.get("source_label")),
            normalize_token(item.get("mapped_to")),
        ),
    )


def _iter_suggestions(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    output: list[dict[str, Any]] = []
    for entry in value:
        if isinstance(entry, Mapping):
            output.append(deepcopy(dict(entry)))
    return output


def _suggestion_key(entry: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        normalize_token(entry.get("report_type")),
        normalize_token(entry.get("source_field")),
        normalize_token(entry.get("source_label")),
        normalize_token(entry.get("mapped_to")),
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    return payload if isinstance(payload, dict) else {}

