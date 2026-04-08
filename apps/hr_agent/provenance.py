"""Provenance helpers for HR specialist output."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class HrProvenance:
    """Minimal provenance block for HR specialist parsing."""

    raw_branch: str | None = None
    raw_date: str | None = None
    detected_subtype: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-safe provenance payload."""

        return {
            "raw_branch": self.raw_branch,
            "raw_date": self.raw_date,
            "detected_subtype": self.detected_subtype,
            "notes": self.notes,
        }
