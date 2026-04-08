"""Helpers for normalizing and comparing staff identities."""

from __future__ import annotations

import re


def normalize_staff_name(value: str) -> str:
    """Return a comparison-safe normalized staff name."""

    lowered = value.casefold().strip()
    lowered = re.sub(r"[^a-z0-9 ]+", " ", lowered)
    return " ".join(lowered.split())


def duplicate_staff_names(names: list[str]) -> set[str]:
    """Return duplicate normalized names from the provided list."""

    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in names:
        normalized = normalize_staff_name(name)
        if not normalized:
            continue
        if normalized in seen:
            duplicates.add(normalized)
        seen.add(normalized)
    return duplicates
