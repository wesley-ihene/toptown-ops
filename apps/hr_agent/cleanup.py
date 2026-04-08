"""Cleanup helpers for WhatsApp HR report text."""

from __future__ import annotations

import re

_WHITESPACE_PATTERN = re.compile(r"\s+")


def cleanup_text(raw_text: str) -> list[str]:
    """Return cleaned non-empty lines from free-form report text."""

    normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned_lines: list[str] = []
    for raw_line in normalized.split("\n"):
        line = _WHITESPACE_PATTERN.sub(" ", raw_line).strip(" \t-*")
        if line:
            cleaned_lines.append(line)
    return cleaned_lines
