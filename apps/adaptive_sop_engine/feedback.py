"""Operator and admin feedback strings for adaptive SOP learning."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from apps.adaptive_sop_engine.rules import preferred_format


def build_suggestion_message(source_label: str, mapped_to: str) -> str:
    """Return the admin review message for one suggested mapping."""

    return f"Observed repeated label '{source_label}' mapping to '{mapped_to}'. Review for approval."


def build_corrections_feedback(
    report_type: str,
    corrections: Sequence[dict[str, Any]],
) -> str | None:
    """Return the operator-facing adaptive correction message when needed."""

    if not corrections:
        return None

    lines = [
        "⚠️ ADAPTIVE SOP CORRECTIONS APPLIED",
        "Report processed successfully.",
        "Corrections:",
    ]
    for correction in corrections:
        source_label = str(correction.get("source_label") or "").strip()
        mapped_to = str(correction.get("mapped_to") or "").strip()
        if not source_label or not mapped_to:
            continue
        lines.append(f"- {source_label} → {mapped_to}")

    recommended = preferred_format(report_type)
    if recommended:
        lines.extend(["", "Recommended future format:", recommended])
    return "\n".join(lines)

