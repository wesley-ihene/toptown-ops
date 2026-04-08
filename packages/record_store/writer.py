"""Safe file writers for record storage."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .paths import (
    get_raw_path,
    get_rejected_path,
    get_structured_path,
    get_structured_path_for_root,
)


def ensure_directory(path: Path) -> Path:
    """Create a directory path if it does not already exist."""

    path.mkdir(parents=True, exist_ok=True)
    return path


def _atomic_write_text(path: Path, content: str) -> Path:
    ensure_directory(path.parent)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    os.replace(temporary_path, path)
    return path


def write_json_file(path: Path, payload: dict[str, Any]) -> Path:
    """Write JSON content deterministically and atomically."""

    content = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)
    return _atomic_write_text(path, f"{content}\n")


def write_text_file(path: Path, content: str) -> Path:
    """Write plain text content atomically."""

    return _atomic_write_text(path, content)


def write_structured(
    signal_type: str,
    branch: str,
    date: str,
    payload: dict[str, Any],
    *,
    root: str | Path | None = None,
    colony_root: str | Path | None = None,
) -> Path:
    """Write one structured JSON record to its canonical location."""

    explicit_root = Path(root) if root is not None else None
    structured_path = (
        get_structured_path(signal_type, branch, date)
        if explicit_root is None
        else get_structured_path_for_root(
            explicit_root / "records" / "structured",
            signal_type=signal_type,
            branch=branch,
            date=date,
        )
    )
    written_path = write_json_file(structured_path, payload)
    source_root = explicit_root if explicit_root is not None else structured_path.parents[4]

    from .automation import log_post_write_failure, run_post_write_automation

    try:
        run_post_write_automation(
            signal_type,
            branch,
            date,
            source_root=source_root,
            colony_root=colony_root,
        )
    except Exception as error:
        log_post_write_failure(
            signal_type=signal_type,
            branch=branch,
            report_date=date,
            structured_path=written_path,
            error=error,
        )
    return written_path


def write_raw(report_type: str, filename: str, text: str) -> Path:
    """Write one raw WhatsApp report under its canonical base directory."""

    return write_text_file(get_raw_path(report_type) / filename, text)


def write_rejected(report_type: str, filename: str, text: str) -> Path:
    """Write one rejected report under its canonical base directory."""

    return write_text_file(get_rejected_path(report_type) / filename, text)
