"""Provenance capture helpers for sales report context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class SalesProvenance:
    """Operational provenance extracted from a sales report."""

    cashier: str | None = None
    assistant: str | None = None
    balanced_by: str | None = None
    supervisor: str | None = None
    supervisor_confirmation: str | None = None
    notes: list[str] | None = None

    def to_payload(self) -> dict[str, str | list[str] | None]:
        """Return a JSON-safe provenance payload."""

        return {
            "cashier": self.cashier,
            "assistant": self.assistant,
            "balanced_by": self.balanced_by,
            "supervisor": self.supervisor,
            "supervisor_confirmation": self.supervisor_confirmation,
            "notes": self.notes or [],
        }
