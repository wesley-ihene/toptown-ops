"""Numbered block detection helpers for HR WhatsApp reports."""

from __future__ import annotations

from dataclasses import dataclass, field
import re

_NUMBERED_LINE_PATTERN = re.compile(r"^\s*(\d+)\s*(?:[.)\-:]+|\s)\s*(.*)$")


@dataclass(slots=True)
class DetectedBlock:
    """One numbered block extracted from a free-form HR report."""

    record_number: int
    header: str
    lines: list[str] = field(default_factory=list)


def split_numbered_blocks(lines: list[str]) -> tuple[list[DetectedBlock], list[str]]:
    """Split cleaned lines into numbered blocks and non-block remainder lines."""

    blocks: list[DetectedBlock] = []
    remainder: list[str] = []
    current: DetectedBlock | None = None

    for line in lines:
        numbered_match = _NUMBERED_LINE_PATTERN.match(line)
        if numbered_match is not None:
            if current is not None:
                blocks.append(current)
            current = DetectedBlock(
                record_number=int(numbered_match.group(1)),
                header=numbered_match.group(2).strip(),
            )
            continue

        if current is not None and _is_summary_boundary(line):
            blocks.append(current)
            current = None
            remainder.append(line)
            continue

        if current is not None:
            current.lines.append(line)
        else:
            remainder.append(line)

    if current is not None:
        blocks.append(current)

    return blocks, remainder


def _is_summary_boundary(line: str) -> bool:
    """Return whether a non-numbered line clearly starts a summary region."""

    normalized = line.casefold().strip()
    return normalized.startswith(
        (
            "grand total",
            "g/total",
            "attendance summary",
            "summary",
            "present:",
            "p:",
            "off:",
            "staff present",
            "staff off",
            "annual leave:",
            "anual leave:",
            "leave:",
            "absent:",
            "sick:",
            "suspend:",
            "suspended:",
            "total staff:",
        )
    )
