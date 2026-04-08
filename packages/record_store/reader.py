"""Structured record readers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .paths import RECORDS_DIR, get_structured_path, get_structured_path_for_root


def read_structured(
    signal_type: str,
    branch: str,
    date: str,
    root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Read a structured JSON record or return `None` if it is missing."""

    if root is None:
        path = get_structured_path(
            signal_type=signal_type,
            branch=branch,
            date=date,
        )
    else:
        path = get_structured_path_for_root(
            Path(root) / RECORDS_DIR.name / "structured",
            signal_type=signal_type,
            branch=branch,
            date=date,
        )
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
