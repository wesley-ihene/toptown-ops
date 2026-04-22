"""Helpers for stable machine-readable rejection payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .types import ValidationRejection


def build_rejection(
    *,
    reason_code: str,
    reason_detail: str,
    field: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Return one normalized rejection payload."""

    return ValidationRejection(
        reason_code=reason_code.strip() or "unknown_rejection",
        reason_detail=reason_detail.strip() or "No rejection detail was provided.",
        field=field.strip() if isinstance(field, str) and field.strip() else None,
        extra={key: value for key, value in extra.items() if value is not None},
    ).to_payload()


def normalize_rejection_entry(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one rejection-like mapping into the stable format."""

    reason_code = entry.get("reason_code")
    if not isinstance(reason_code, str) or not reason_code.strip():
        reason_code = entry.get("code")
    reason_detail = entry.get("reason_detail")
    if not isinstance(reason_detail, str) or not reason_detail.strip():
        reason_detail = entry.get("message")
    field = entry.get("field")
    extras = {
        key: value
        for key, value in entry.items()
        if key not in {"reason_code", "reason_detail", "code", "message", "field"}
    }
    return build_rejection(
        reason_code=str(reason_code or "unknown_rejection"),
        reason_detail=str(reason_detail or "No rejection detail was provided."),
        field=field if isinstance(field, str) else None,
        **extras,
    )


def normalize_rejections(entries: Sequence[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    """Normalize a list of rejection-like mappings."""

    if entries is None:
        return []
    return [normalize_rejection_entry(entry) for entry in entries if isinstance(entry, Mapping)]
