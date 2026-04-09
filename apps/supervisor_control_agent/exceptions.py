"""Exception derivation helpers for supervisor control reports."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.supervisor_control_agent.parser import ParsedSupervisorControlReport

_KNOWN_EXCEPTION_TYPES = {
    "STAFF_ISSUE",
    "SECURITY_ISSUE",
    "SYSTEM_ISSUE",
    "CASH_ISSUE",
    "STOCK_ISSUE",
    "CUSTOMER_ISSUE",
    "FACILITY_ISSUE",
    "CASH_CONTROL",
    "FLOOR_CONTROL",
    "STOCK_CONTROL",
    "PRICING_SYSTEM_CONTROL",
    "STAFFING_CONTROL",
}

_SEMANTIC_EXCEPTION_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "CASH_CONTROL",
        (
            "cashier reconciled",
            "cash variance",
            "till mismatch",
            "cash mismatch",
            "till variance",
            "cash control",
        ),
    ),
    (
        "FLOOR_CONTROL",
        (
            "floor check",
            "front door display",
            "door display checked",
            "display checked",
            "store locked",
        ),
    ),
    (
        "STOCK_CONTROL",
        (
            "stock issue",
            "empty rail",
            "out of stock",
            "stock out",
        ),
    ),
    (
        "PRICING_SYSTEM_CONTROL",
        (
            "pricing system issues",
            "pricing system issue",
            "pricing issues",
            "pricing issue",
            "printer down",
            "printer issue",
            "pos issue",
            "pos down",
            "system issue",
        ),
    ),
    (
        "STAFFING_CONTROL",
        (
            "staffing issue",
            "late staff",
            "absent staff",
            "staff absent",
            "staff late",
        ),
    ),
)


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
        details = (entry.details or "").strip()
        action_taken = _normalize_action_taken(entry.action_taken, details=details, exception_type=entry.exception_type or "")
        exception_type = _normalize_exception_type(
            entry.exception_type,
            details=details,
            action_taken=action_taken,
        )
        if exception_type == "UNKNOWN":
            unknown_exception_type_count += 1

        supervisor_confirmed = _normalize_confirmation(entry.supervisor_confirmed)
        status = _normalize_status(action_taken)
        if status == "open":
            open_exception_count += 1

        items.append(
            ExceptionItem(
                exception_type=exception_type,
                details=details,
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


def _normalize_exception_type(
    raw_value: str | None,
    *,
    details: str = "",
    action_taken: str = "",
) -> str:
    if raw_value:
        normalized = _normalize_exception_token(raw_value)
        if normalized in _KNOWN_EXCEPTION_TYPES:
            return normalized

    semantic_match = _classify_exception_type_from_text(raw_value=raw_value, details=details, action_taken=action_taken)
    if semantic_match is not None:
        return semantic_match
    return "UNKNOWN"


def _classify_exception_type_from_text(
    *,
    raw_value: str | None,
    details: str,
    action_taken: str,
) -> str | None:
    normalized_text = _normalize_text(" ".join(part for part in (raw_value or "", details, action_taken) if part))
    if not normalized_text:
        return None

    for canonical_type, phrases in _SEMANTIC_EXCEPTION_RULES:
        if any(phrase in normalized_text for phrase in phrases):
            return canonical_type
    return None


def _normalize_exception_token(value: str) -> str:
    return "_".join(value.strip().upper().replace("-", "_").replace("/", "_").split())


def _normalize_text(value: str) -> str:
    return " ".join(
        value.casefold()
        .replace("_", " ")
        .replace("-", " ")
        .replace("/", " ")
        .replace(":", " ")
        .split()
    )


def _normalize_action_taken(raw_value: str | None, *, details: str, exception_type: str) -> str:
    action_taken = (raw_value or "").strip()
    if action_taken:
        return action_taken
    if details:
        return details
    if exception_type != "UNKNOWN":
        return "Reported"
    return "Observed"


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
    if any(
        token in lowered
        for token in {
            "resolved",
            "closed",
            "cleared",
            "passed",
            "checked",
            "complete",
            "completed",
            "approved",
            "signed",
            "locked",
            "reconciled",
            "verified",
            "confirmed",
            "yes",
        }
    ):
        return "resolved"
    if action_taken.strip():
        return "open"
    return "unknown"
