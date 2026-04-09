"""Deterministic feedback message formatting for rejected reports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def format_feedback_message(
    *,
    report_type: str,
    rejections: Sequence[Mapping[str, Any]],
    branch: str | None = None,
    report_date: str | None = None,
) -> str:
    """Return a deterministic WhatsApp-safe rejection feedback message."""

    lines = [
        "TOPTOWN OPS REJECTION FEEDBACK",
        f"Report Type: {_format_label(report_type)}",
    ]
    if branch:
        lines.append(f"Branch: {_format_label(branch)}")
    if report_date:
        lines.append(f"Report Date: {report_date}")

    lines.extend(
        [
            "",
            "This report was rejected. Please correct the following items:",
        ]
    )

    for index, rejection in enumerate(rejections, start=1):
        code = str(rejection.get("code") or "unknown_rejection").strip()
        message = str(rejection.get("message") or "No rejection detail was provided.").strip()
        field = str(rejection.get("field") or "").strip()
        if field:
            lines.append(f"{index}. {code} [{field}]: {message}")
        else:
            lines.append(f"{index}. {code}: {message}")

    lines.extend(
        [
            "",
            "Please resend one corrected report only.",
        ]
    )
    return "\n".join(lines)


def _format_label(value: str) -> str:
    """Return a compact display label for one slug-like value."""

    cleaned = value.strip().replace("_", " ")
    if not cleaned:
        return "Unknown"
    return " ".join(part.capitalize() for part in cleaned.split())
