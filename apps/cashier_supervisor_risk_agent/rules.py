"""Risk extraction rules for cashier and supervisor adjustment intelligence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from packages.validation.sales_reconciliation import TOLERANCE

RISK_TYPE_ITEM_RETURNS = "item_returns"
RISK_TYPE_CASH_OVER = "cash_over"
RISK_TYPE_CASH_DOWN = "cash_down"
RISK_TYPE_UNEXPLAINED_VARIANCE = "unexplained_variance"
RISK_TYPE_HIGH_ADJUSTMENT = "high_adjustment"
RISK_TYPE_REPEATED_ADJUSTMENT = "repeated_adjustment"
RISK_TYPE_BALANCED_BY_SAME_AS_CASHIER = "balanced_by_same_as_cashier"
RISK_TYPE_Z_READING_MISMATCH = "z_reading_mismatch"

PERSON_ROLE_CASHIER = "cashier"
PERSON_ROLE_SUPERVISOR = "supervisor"
PERSON_ROLE_BALANCED_BY = "balanced_by"

RISK_TYPES = frozenset(
    {
        RISK_TYPE_ITEM_RETURNS,
        RISK_TYPE_CASH_OVER,
        RISK_TYPE_CASH_DOWN,
        RISK_TYPE_UNEXPLAINED_VARIANCE,
        RISK_TYPE_HIGH_ADJUSTMENT,
        RISK_TYPE_REPEATED_ADJUSTMENT,
        RISK_TYPE_BALANCED_BY_SAME_AS_CASHIER,
        RISK_TYPE_Z_READING_MISMATCH,
    }
)
PERSON_ROLES = frozenset({PERSON_ROLE_CASHIER, PERSON_ROLE_SUPERVISOR, PERSON_ROLE_BALANCED_BY})


@dataclass(slots=True)
class AdjustmentEvent:
    """One structured sales record with adjustment-risk context."""

    branch: str
    report_date: str
    source_path: str
    cashier: str | None
    assistant: str | None
    supervisor: str | None
    balanced_by: str | None
    item_returns: float
    cash_over: float
    cash_down: float
    gross_sales: float
    net_sales: float
    unexplained_variance: float
    expected_z_reading: float
    declared_z_reading: float
    z_reading_mismatch: float
    variance_reason: str | None
    return_type: str | None
    warning_codes: tuple[str, ...] = ()
    review_flags: tuple[str, ...] = ()

    @property
    def has_adjustment(self) -> bool:
        return any(amount > 0.0 for amount in (self.item_returns, self.cash_over, self.cash_down))

    @property
    def has_unexplained_mismatch(self) -> bool:
        return self.unexplained_variance > TOLERANCE


@dataclass(slots=True)
class RiskObservation:
    """One base risk observation tied to one source record and person."""

    branch: str
    report_date: str
    source_path: str
    person_role: str
    person_name: str
    supervisor: str | None
    balanced_by: str | None
    risk_type: str
    amount: float
    review_flags: tuple[str, ...] = ()


@dataclass(slots=True)
class AdjustmentParticipation:
    """One adjustment event participation used for repeated-pattern windows."""

    branch: str
    report_date: str
    source_path: str
    person_role: str
    person_name: str
    supervisor: str | None
    balanced_by: str | None
    total_adjustment_amount: float
    review_flags: tuple[str, ...] = ()


@dataclass(slots=True)
class ExtractionResult:
    """Risk observations and repeated-pattern participants from one record."""

    observations: list[RiskObservation] = field(default_factory=list)
    participations: list[AdjustmentParticipation] = field(default_factory=list)


def extract_record_signals(payload: Mapping[str, Any], source_path: Path) -> ExtractionResult:
    """Return risk observations extracted from one structured sales record."""

    event = _extract_adjustment_event(payload, source_path)
    if event is None:
        return ExtractionResult()

    observations: list[RiskObservation] = []
    participations: list[AdjustmentParticipation] = []
    identities = _event_identities(event)
    total_adjustment_amount = round(
        max(event.item_returns, 0.0)
        + max(event.cash_over, 0.0)
        + max(event.cash_down, 0.0)
        + max(event.unexplained_variance, 0.0),
        2,
    )

    for person_role, person_name in identities:
        base_kwargs = {
            "branch": event.branch,
            "report_date": event.report_date,
            "source_path": event.source_path,
            "person_role": person_role,
            "person_name": person_name,
            "supervisor": event.supervisor,
            "balanced_by": event.balanced_by,
            "review_flags": event.review_flags,
        }
        if event.item_returns > 0.0:
            observations.append(
                RiskObservation(
                    risk_type=RISK_TYPE_ITEM_RETURNS,
                    amount=event.item_returns,
                    **base_kwargs,
                )
            )
        if event.cash_over > 0.0:
            observations.append(
                RiskObservation(
                    risk_type=RISK_TYPE_CASH_OVER,
                    amount=event.cash_over,
                    **base_kwargs,
                )
            )
        if event.cash_down > 0.0:
            observations.append(
                RiskObservation(
                    risk_type=RISK_TYPE_CASH_DOWN,
                    amount=event.cash_down,
                    **base_kwargs,
                )
            )
        if _is_high_adjustment(event):
            observations.append(
                RiskObservation(
                    risk_type=RISK_TYPE_HIGH_ADJUSTMENT,
                    amount=max(event.item_returns, event.cash_over, event.cash_down, event.unexplained_variance),
                    **base_kwargs,
                )
            )
        if _is_unexplained_adjustment(event):
            observations.append(
                RiskObservation(
                    risk_type=RISK_TYPE_UNEXPLAINED_VARIANCE,
                    amount=max(event.unexplained_variance, event.cash_over, event.cash_down),
                    **base_kwargs,
                )
            )
        if event.z_reading_mismatch > TOLERANCE:
            observations.append(
                RiskObservation(
                    risk_type=RISK_TYPE_Z_READING_MISMATCH,
                    amount=event.z_reading_mismatch,
                    **base_kwargs,
                )
            )
        if event.has_adjustment or event.has_unexplained_mismatch:
            participations.append(
                AdjustmentParticipation(
                    branch=event.branch,
                    report_date=event.report_date,
                    source_path=event.source_path,
                    person_role=person_role,
                    person_name=person_name,
                    supervisor=event.supervisor,
                    balanced_by=event.balanced_by,
                    total_adjustment_amount=total_adjustment_amount,
                    review_flags=event.review_flags,
                )
            )
    if _same_person(event.balanced_by, event.cashier) and event.cashier is not None:
        observations.append(
            RiskObservation(
                branch=event.branch,
                report_date=event.report_date,
                source_path=event.source_path,
                person_role=PERSON_ROLE_CASHIER,
                person_name=event.cashier,
                supervisor=event.supervisor,
                balanced_by=event.balanced_by,
                risk_type=RISK_TYPE_BALANCED_BY_SAME_AS_CASHIER,
                amount=0.0,
                review_flags=event.review_flags,
            )
        )

    return ExtractionResult(observations=observations, participations=participations)


def _extract_adjustment_event(payload: Mapping[str, Any], source_path: Path) -> AdjustmentEvent | None:
    branch = _string_or_none(payload.get("branch"))
    report_date = _string_or_none(payload.get("report_date"))
    if branch is None or report_date is None:
        return None

    metrics = payload.get("metrics")
    reconciliation = payload.get("reconciliation")
    provenance = payload.get("provenance")
    warnings = payload.get("warnings")
    if not isinstance(metrics, Mapping):
        metrics = {}
    if not isinstance(reconciliation, Mapping):
        reconciliation = {}
    if not isinstance(provenance, Mapping):
        provenance = {}

    warning_codes = tuple(_warning_codes(warnings))
    variance_reason = _first_non_empty(
        _string_or_none(metrics.get("variance_reason")),
        _string_or_none(reconciliation.get("variance_reason")),
        _note_value(provenance.get("notes"), "variance reason"),
    )
    return_type = _first_non_empty(
        _string_or_none(metrics.get("return_type")),
        _note_value(provenance.get("notes"), "return type"),
    )

    item_returns = _money(metrics.get("item_returns"), reconciliation.get("item_return_adjustment"))
    cash_over = _money(metrics.get("cash_over"), reconciliation.get("cash_over"))
    cash_down = _money(metrics.get("cash_down"), reconciliation.get("cash_down"))
    gross_sales = _money(metrics.get("gross_sales"))
    net_sales = _money(metrics.get("net_sales"))
    unexplained_variance = _money(reconciliation.get("unexplained_variance"))
    expected_z_reading_value = _number_or_none(reconciliation.get("expected_z_reading"))
    declared_z_reading_value = _number_or_none(
        reconciliation.get("declared_z_reading"),
        reconciliation.get("z_reading"),
        metrics.get("z_reading"),
        payload.get("z_reading"),
    )
    expected_z_reading = round(expected_z_reading_value or 0.0, 2)
    declared_z_reading = round(declared_z_reading_value or 0.0, 2)
    z_reading_mismatch = _difference(expected_z_reading_value, declared_z_reading_value)
    same_person_balancing = _same_person(
        _string_or_none(provenance.get("balanced_by")),
        _string_or_none(provenance.get("cashier")),
    )

    has_adjustment = item_returns > 0.0 or cash_over > 0.0 or cash_down > 0.0
    has_mismatch = unexplained_variance > TOLERANCE
    has_z_reading_mismatch = z_reading_mismatch > TOLERANCE
    if not has_adjustment and not has_mismatch and not has_z_reading_mismatch and not same_person_balancing:
        return None

    review_flags: list[str] = []
    if same_person_balancing or "balanced_by_same_as_cashier" in warning_codes:
        review_flags.append("balanced_by_same_as_cashier")
    if "missing_variance_reason" in warning_codes or (cash_over > 0.0 or cash_down > 0.0) and variance_reason is None:
        review_flags.append("missing_variance_reason")
    if has_z_reading_mismatch:
        review_flags.append("z_reading_mismatch")
    if has_mismatch:
        review_flags.append("unexplained_variance_present")
        review_flags.append("z_reading_mismatch_unreconciled")

    return AdjustmentEvent(
        branch=branch,
        report_date=report_date,
        source_path=str(source_path),
        cashier=_string_or_none(provenance.get("cashier")),
        assistant=_string_or_none(provenance.get("assistant")),
        supervisor=_string_or_none(provenance.get("supervisor")),
        balanced_by=_string_or_none(provenance.get("balanced_by")),
        item_returns=item_returns,
        cash_over=cash_over,
        cash_down=cash_down,
        gross_sales=gross_sales,
        net_sales=net_sales,
        unexplained_variance=unexplained_variance,
        expected_z_reading=expected_z_reading,
        declared_z_reading=declared_z_reading,
        z_reading_mismatch=z_reading_mismatch,
        variance_reason=variance_reason,
        return_type=return_type,
        warning_codes=warning_codes,
        review_flags=tuple(sorted(set(review_flags))),
    )


def _event_identities(event: AdjustmentEvent) -> list[tuple[str, str]]:
    identities: list[tuple[str, str]] = []
    for role, value in (
        (PERSON_ROLE_CASHIER, event.cashier),
        (PERSON_ROLE_SUPERVISOR, event.supervisor),
        (PERSON_ROLE_BALANCED_BY, event.balanced_by),
    ):
        name = _string_or_none(value)
        if name is None:
            continue
        identities.append((role, name))
    return identities


def _is_high_adjustment(event: AdjustmentEvent) -> bool:
    return (
        event.item_returns > 50.0
        or event.cash_over > 20.0
        or event.cash_down > 20.0
        or event.has_unexplained_mismatch
    )


def _is_unexplained_adjustment(event: AdjustmentEvent) -> bool:
    return (
        event.has_unexplained_mismatch
        or "missing_variance_reason" in event.review_flags
    )


def _warning_codes(warnings: object) -> list[str]:
    if not isinstance(warnings, list):
        return []
    codes: list[str] = []
    for warning in warnings:
        if isinstance(warning, Mapping):
            code = _string_or_none(warning.get("code"))
            if code is not None:
                codes.append(code)
    return codes


def _note_value(notes: object, prefix: str) -> str | None:
    if not isinstance(notes, list):
        return None
    normalized_prefix = f"{prefix.casefold().strip()}:"
    for note in notes:
        if not isinstance(note, str):
            continue
        stripped = note.strip()
        if not stripped:
            continue
        lowered = stripped.casefold()
        if lowered.startswith(normalized_prefix):
            return stripped.split(":", 1)[1].strip() or None
    return None


def _money(*values: object) -> float:
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return round(abs(float(value)), 2)
        if isinstance(value, str):
            stripped = value.strip().replace("K", "").replace(",", "")
            if not stripped:
                continue
            try:
                return round(abs(float(stripped)), 2)
            except ValueError:
                continue
    return 0.0


def _difference(left: float | None, right: float | None) -> float:
    if left is None or right is None:
        return 0.0
    return round(abs(left - right), 2)


def _number_or_none(*values: object) -> float | None:
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return round(float(value), 2)
        if isinstance(value, str):
            stripped = value.strip().replace("K", "").replace(",", "")
            if not stripped:
                continue
            try:
                return round(float(stripped), 2)
            except ValueError:
                continue
    return None


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value is not None:
            return value
    return None


def _same_person(left: str | None, right: str | None) -> bool:
    if left is None or right is None:
        return False
    return _normalize_person_name(left) == _normalize_person_name(right)


def _normalize_person_name(value: str) -> str:
    return " ".join(value.casefold().split())
