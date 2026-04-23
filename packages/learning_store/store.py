"""Deterministic file-backed storage for daily learning artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.record_store.writer import write_json_file

LEARNING_CATEGORIES = {
    "review_summary",
    "action_effectiveness",
    "threshold_recommendations",
    "format_drift",
    "daily_summary",
}


def write_daily_learning_artifact(
    category: str,
    artifact_date: str,
    payload: Mapping[str, Any],
    *,
    generated_at: str | None = None,
    source_paths: Sequence[str] | None = None,
    analysis_window: Mapping[str, Any] | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Write one daily learning artifact to its deterministic category/date path."""

    normalized_category = _validated_category(category)
    normalized_date = _required_text(artifact_date, field_name="artifact_date")
    artifact_path = get_learning_artifact_path(
        normalized_category,
        normalized_date,
        output_root=output_root,
    )
    persisted_payload = _build_persisted_payload(
        artifact_date=normalized_date,
        payload=payload,
        generated_at=generated_at,
        source_paths=source_paths,
        analysis_window=analysis_window,
    )
    write_json_file(artifact_path, persisted_payload)
    return str(artifact_path)


def read_latest_learning_artifact(
    category: str,
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Read the latest daily artifact for one learning category."""

    normalized_category = _validated_category(category)
    category_dir = _learning_root(output_root) / normalized_category
    if not category_dir.exists():
        return None

    latest_path = max(
        (path for path in category_dir.glob("*.json") if path.is_file()),
        default=None,
        key=lambda path: path.stem,
    )
    if latest_path is None:
        return None
    return _read_json(latest_path)


def get_learning_artifact_path(
    category: str,
    artifact_date: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    """Return the deterministic path for one learning artifact."""

    normalized_category = _validated_category(category)
    normalized_date = _required_text(artifact_date, field_name="artifact_date")
    return _learning_root(output_root) / normalized_category / f"{normalized_date}.json"


def write_review_summary(
    artifact_date: str,
    payload: Mapping[str, Any],
    *,
    generated_at: str | None = None,
    source_paths: Sequence[str] | None = None,
    analysis_window: Mapping[str, Any] | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Write one review-summary artifact."""

    return write_daily_learning_artifact(
        "review_summary",
        artifact_date,
        payload,
        generated_at=generated_at,
        source_paths=source_paths,
        analysis_window=analysis_window,
        output_root=output_root,
    )


def write_action_effectiveness(
    artifact_date: str,
    payload: Mapping[str, Any],
    *,
    generated_at: str | None = None,
    source_paths: Sequence[str] | None = None,
    analysis_window: Mapping[str, Any] | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Write one action-effectiveness artifact."""

    return write_daily_learning_artifact(
        "action_effectiveness",
        artifact_date,
        payload,
        generated_at=generated_at,
        source_paths=source_paths,
        analysis_window=analysis_window,
        output_root=output_root,
    )


def write_threshold_recommendations(
    artifact_date: str,
    payload: Mapping[str, Any],
    *,
    generated_at: str | None = None,
    source_paths: Sequence[str] | None = None,
    analysis_window: Mapping[str, Any] | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Write one threshold-recommendations artifact."""

    return write_daily_learning_artifact(
        "threshold_recommendations",
        artifact_date,
        payload,
        generated_at=generated_at,
        source_paths=source_paths,
        analysis_window=analysis_window,
        output_root=output_root,
    )


def write_format_drift(
    artifact_date: str,
    payload: Mapping[str, Any],
    *,
    generated_at: str | None = None,
    source_paths: Sequence[str] | None = None,
    analysis_window: Mapping[str, Any] | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Write one format-drift artifact."""

    return write_daily_learning_artifact(
        "format_drift",
        artifact_date,
        payload,
        generated_at=generated_at,
        source_paths=source_paths,
        analysis_window=analysis_window,
        output_root=output_root,
    )


def write_daily_summary(
    artifact_date: str,
    payload: Mapping[str, Any],
    *,
    generated_at: str | None = None,
    source_paths: Sequence[str] | None = None,
    analysis_window: Mapping[str, Any] | None = None,
    output_root: str | Path | None = None,
) -> str:
    """Write one daily-summary artifact."""

    return write_daily_learning_artifact(
        "daily_summary",
        artifact_date,
        payload,
        generated_at=generated_at,
        source_paths=source_paths,
        analysis_window=analysis_window,
        output_root=output_root,
    )


def _learning_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.RECORDS_DIR / "learning"
    return Path(output_root) / "records" / "learning"


def _build_persisted_payload(
    *,
    artifact_date: str,
    payload: Mapping[str, Any],
    generated_at: str | None,
    source_paths: Sequence[str] | None,
    analysis_window: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("learning artifact payload must be a mapping")

    persisted_payload = deepcopy(dict(payload))
    persisted_payload["date"] = artifact_date
    persisted_payload["generated_at"] = _resolved_generated_at(
        generated_at=generated_at,
        payload=persisted_payload,
    )
    persisted_payload["source_paths"] = _resolved_source_paths(
        source_paths=source_paths,
        payload=persisted_payload,
    )
    persisted_payload["analysis_window"] = _resolved_analysis_window(
        analysis_window=analysis_window,
        payload=persisted_payload,
    )
    return persisted_payload


def _resolved_generated_at(
    *,
    generated_at: str | None,
    payload: Mapping[str, Any],
) -> str | None:
    if generated_at is not None:
        return _required_text(generated_at, field_name="generated_at")
    existing = payload.get("generated_at")
    if existing is None:
        return None
    return _required_text(existing, field_name="generated_at")


def _resolved_source_paths(
    *,
    source_paths: Sequence[str] | None,
    payload: Mapping[str, Any],
) -> list[str]:
    candidate = source_paths if source_paths is not None else payload.get("source_paths")
    return _string_list(candidate)


def _resolved_analysis_window(
    *,
    analysis_window: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    candidate = analysis_window if analysis_window is not None else payload.get("analysis_window")
    if isinstance(candidate, Mapping):
        return deepcopy(dict(candidate))
    return {}


def _validated_category(category: str) -> str:
    normalized_category = _required_text(category, field_name="category")
    if normalized_category not in LEARNING_CATEGORIES:
        raise ValueError(f"unsupported learning artifact category: {normalized_category}")
    return normalized_category


def _required_text(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"learning artifact field `{field_name}` must be a non-empty string")


def _string_list(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        import json

        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None
