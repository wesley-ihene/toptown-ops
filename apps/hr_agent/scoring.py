"""Simple scoring helpers for HR specialist outputs."""

from __future__ import annotations


def compute_performance_score(*, items_moved: int, assisting_count: int) -> float:
    """Return a small derived activity score for a performance record."""

    return round(items_moved + (assisting_count * 0.5), 2)


def attendance_presence_score(status: str) -> int:
    """Return a conservative binary presence score for attendance records."""

    return 1 if status == "present" else 0
