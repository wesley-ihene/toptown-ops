"""Structured record readers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from packages.data_governance import read_governance_sidecar

from .paths import (
    RECORDS_DIR,
    get_legacy_structured_path,
    get_legacy_structured_path_for_root,
    get_structured_path,
    get_structured_path_for_root,
    is_intelligence_signal_type,
)


def read_structured(
    signal_type: str,
    branch: str,
    date: str,
    root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Read a structured JSON record or return `None` if it is missing."""

    path = _resolved_record_path(signal_type, branch, date, root=root)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_structured_record(
    record_type: str,
    branch: str,
    record_date: str,
    root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Backward-compatible wrapper for reading one structured record."""

    return read_structured(record_type, branch, record_date, root=root)


def read_structured_governance(
    record_type: str,
    branch: str,
    record_date: str,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Read the governance sidecar for one structured record."""

    path = _resolved_record_path(record_type, branch, record_date, root=root)
    return read_governance_sidecar(path)


def _resolved_record_path(
    signal_type: str,
    branch: str,
    date: str,
    *,
    root: str | Path | None,
) -> Path:
    """Return the primary intelligence path or a legacy structured fallback."""

    if root is None:
        path = get_structured_path(
            signal_type=signal_type,
            branch=branch,
            date=date,
        )
        if path.exists() or not is_intelligence_signal_type(signal_type):
            return path
        legacy_path = get_legacy_structured_path(
            signal_type=signal_type,
            branch=branch,
            date=date,
        )
        return legacy_path if legacy_path.exists() else path

    structured_root = Path(root) / RECORDS_DIR.name / "structured"
    path = get_structured_path_for_root(
        structured_root,
        signal_type=signal_type,
        branch=branch,
        date=date,
    )
    if path.exists() or not is_intelligence_signal_type(signal_type):
        return path
    legacy_path = get_legacy_structured_path_for_root(
        structured_root,
        signal_type=signal_type,
        branch=branch,
        date=date,
    )
    return legacy_path if legacy_path.exists() else path
