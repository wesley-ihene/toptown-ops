"""Structured warning helpers for the HR agent."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class WarningEntry:
    """Structured warning object shared across the HR agent."""

    code: str
    severity: str
    message: str

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-safe warning payload."""

        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
        }


def make_warning(*, code: str, severity: str, message: str) -> WarningEntry:
    """Create a structured warning entry."""

    return WarningEntry(code=code, severity=severity, message=message)


def dedupe_warnings(warnings: list[WarningEntry]) -> list[WarningEntry]:
    """Return warnings de-duplicated by warning code."""

    unique: dict[str, WarningEntry] = {}
    for warning in warnings:
        if warning.code not in unique:
            unique[warning.code] = warning
    return list(unique.values())
