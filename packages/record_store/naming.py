"""Deterministic naming helpers for stored records."""

from __future__ import annotations

from datetime import datetime
import re

_UNSAFE_SEGMENT_RE = re.compile(r"[^a-z0-9]+")


def safe_segment(value: str) -> str:
    """Collapse a value into a safe path segment."""

    cleaned = _UNSAFE_SEGMENT_RE.sub("_", value.strip().lower()).strip("_")
    if cleaned:
        return cleaned
    return "unknown"


def _normalize_date(date: str) -> str:
    """Return a validated ISO date string."""

    return datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")


def build_structured_filename(date: str) -> str:
    """Return the canonical filename for one structured record."""

    return f"{_normalize_date(date)}.json"


def build_raw_filename(date: str, branch: str, report_type: str) -> str:
    """Return the canonical filename for a raw WhatsApp report."""

    return (
        f"{_normalize_date(date)}__"
        f"{safe_segment(branch)}__"
        f"{safe_segment(report_type)}.txt"
    )


def build_rejected_filename(report_type: str, reason: str) -> str:
    """Return a safe rejected filename with a UTC timestamp prefix."""

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    return f"{stamp}__{safe_segment(report_type)}__{safe_segment(reason)}.txt"


def raw_record_name(record_date: str, branch: str, record_type: str) -> str:
    """Backward-compatible wrapper for the raw record filename helper."""

    return build_raw_filename(record_date, branch, record_type)


def rejected_record_name(report_type: str, reason: str) -> str:
    """Backward-compatible wrapper for the rejected filename helper."""

    return build_rejected_filename(report_type, reason)
