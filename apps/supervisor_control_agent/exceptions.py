"""Exception derivation helpers for supervisor control reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.supervisor_control_agent.parser import ParsedSupervisorControlReport


@dataclass(slots=True)
class ExceptionItem:
    """One normalized exception item for the output contract."""

    exception_type: str
    details: str
    action_taken: str
    escalated_by: str
    time: str
    supervisor_confirmed: str
    status: str

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-safe exception item payload."""

        return {
            "exception_type": self.exception_type,
            "details": self.details,
            "action_taken": self.action_taken,
            "escalated_by": self.escalated_by,
            "time": self.time,
            "supervisor_confirmed": self.supervisor_confirmed,
            "status": self.status,
        }


@dataclass(slots=True)
class ExceptionSummary:
    """Derived exception metrics and normalized items."""

    items: list[ExceptionItem] = field(default_factory=list)
    exception_count: int = 0
    open_exception_count: int = 0
    unknown_exception_type_count: int = 0


def derive_exceptions(parsed: ParsedSupervisorControlReport) -> ExceptionSummary:
    """Return normalized exception items and summary metrics."""

    items: list[ExceptionItem] = []
    open_exception_count = 0
    unknown_exception_type_count = 0

    for entry in parsed.exception_entries:
        exception_type = _normalize_exception_type(entry.exception_type)
        if exception_type == "UNKNOWN":
            unknown_exception_type_count += 1

        action_taken = (entry.action_taken or "").strip()
        supervisor_confirmed = _normalize_confirmation(entry.supervisor_confirmed)
        status = _normalize_status(action_taken)
        if status == "open":
            open_exception_count += 1

        items.append(
            ExceptionItem(
                exception_type=exception_type,
                details=(entry.details or "").strip(),
                action_taken=action_taken,
                escalated_by=(entry.escalated_by or "").strip(),
                time=(entry.time or "").strip(),
                supervisor_confirmed=supervisor_confirmed,
                status=status,
            )
        )

    return ExceptionSummary(
        items=items,
        exception_count=len(items),
        open_exception_count=open_exception_count,
        unknown_exception_type_count=unknown_exception_type_count,
    )


def _normalize_exception_type(raw_value: str | None) -> str:
    if not raw_value:
        return "UNKNOWN"

    normalized = "_".join(raw_value.strip().upper().replace("-", "_").split())
    known_types = {
        "STAFF_ISSUE",
        "SECURITY_ISSUE",
        "SYSTEM_ISSUE",
        "CASH_ISSUE",
        "STOCK_ISSUE",
        "CUSTOMER_ISSUE",
        "FACILITY_ISSUE",
    }
    if normalized in known_types:
        return normalized
    return "UNKNOWN"


def _normalize_confirmation(raw_value: str | None) -> str:
    lowered = (raw_value or "").strip().casefold()
    if lowered in {"yes", "y", "confirmed"}:
        return "YES"
    if lowered in {"no", "n", "not confirmed"}:
        return "NO"
    return "UNKNOWN"


def _normalize_status(action_taken: str) -> str:
    lowered = action_taken.casefold()
    if any(token in lowered for token in {"escalated", "escalate"}):
        return "escalated"
    if any(token in lowered for token in {"resolved", "closed", "cleared"}):
        return "resolved"
    if action_taken.strip():
        return "open"
    return "unknown"
