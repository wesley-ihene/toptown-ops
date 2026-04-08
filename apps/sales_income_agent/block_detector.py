"""Logical section detection for messy WhatsApp sales reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.sales_income_agent.field_mapper import canonical_section_name


@dataclass(slots=True)
class DetectedBlock:
    """A logical section extracted from a raw sales report."""

    block_type: str
    lines: list[str] = field(default_factory=list)


def detect_blocks(raw_text: str) -> list[DetectedBlock]:
    """Split a free-form sales report into coarse logical blocks."""

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    blocks: list[DetectedBlock] = []
    current = DetectedBlock(block_type="sales_header")

    for line in lines:
        block_type = _detect_heading(line)
        if block_type is not None:
            if current.lines:
                blocks.append(current)
            current = DetectedBlock(block_type=block_type, lines=[line])
            continue

        if _looks_like_totals_line(line) and current.block_type != "totals":
            if current.lines:
                blocks.append(current)
            current = DetectedBlock(block_type="totals", lines=[line])
            continue

        if _looks_like_customer_line(line) and current.block_type != "customer_count":
            if current.lines:
                blocks.append(current)
            current = DetectedBlock(block_type="customer_count", lines=[line])
            continue

        current.lines.append(line)

    if current.lines:
        blocks.append(current)

    return blocks


def _detect_heading(line: str) -> str | None:
    """Return a canonical block type when a line looks like a section heading."""

    cleaned = line.rstrip(":")
    return canonical_section_name(cleaned)


def _looks_like_totals_line(line: str) -> bool:
    """Return whether a line likely belongs to totals/reconciliation content."""

    normalized = line.casefold()
    return any(marker in normalized for marker in ("total", "gross", "cash", "eftpos", "deposit", "till"))


def _looks_like_customer_line(line: str) -> bool:
    """Return whether a line likely contains customer-count information."""

    normalized = line.casefold()
    return "customer" in normalized
