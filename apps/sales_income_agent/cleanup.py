"""Text cleanup helpers for messy WhatsApp sales reports."""

from __future__ import annotations


def cleanup_text(raw_text: str) -> list[str]:
    """Return cleaned non-empty lines from a free-form sales report."""

    cleaned_lines: list[str] = []
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = line.lstrip("-* ")
        cleaned_lines.append(" ".join(line.split()))
    return cleaned_lines
