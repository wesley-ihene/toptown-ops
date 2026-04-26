"""Branch tolerance helpers for human-written WhatsApp reports."""

from __future__ import annotations

from dataclasses import dataclass
import re

from packages.normalization.branches import CANONICAL_BRANCHES, resolve_branch_alias

_BRANCH_LINE_PATTERN = re.compile(r"^\s*(?:branch|shop|location)\s*[:=-]\s*(.+?)\s*$", flags=re.IGNORECASE)


@dataclass(slots=True, frozen=True)
class BranchDetection:
    """One detected branch candidate from raw human input."""

    slug: str
    display_name: str
    raw_value: str
    line_index: int
    explicit_line: bool


def detect_branch(lines: list[str]) -> BranchDetection | None:
    """Return one canonical branch candidate from explicit or header text."""

    for index, line in enumerate(lines):
        match = _BRANCH_LINE_PATTERN.match(line)
        if match is None:
            continue
        resolved = resolve_branch_alias(match.group(1))
        if resolved is None:
            continue
        return BranchDetection(
            slug=resolved.slug,
            display_name=resolved.display_name,
            raw_value=match.group(1).strip(),
            line_index=index,
            explicit_line=True,
        )

    for index, line in enumerate(lines[:8]):
        resolved = resolve_branch_alias(line)
        if resolved is None:
            continue
        return BranchDetection(
            slug=resolved.slug,
            display_name=CANONICAL_BRANCHES.get(resolved.slug, resolved.display_name),
            raw_value=line.strip(),
            line_index=index,
            explicit_line=False,
        )
    return None
