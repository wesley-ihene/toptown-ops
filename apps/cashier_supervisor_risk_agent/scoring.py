"""Aggregation and severity scoring for cashier and supervisor risks."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timedelta
from typing import Any

from apps.cashier_supervisor_risk_agent.feedback import recommended_action_for_entry
from apps.cashier_supervisor_risk_agent.rules import (
    PERSON_ROLE_BALANCED_BY,
    PERSON_ROLE_CASHIER,
    PERSON_ROLE_SUPERVISOR,
    RISK_TYPE_BALANCED_BY_SAME_AS_CASHIER,
    RISK_TYPE_CASH_DOWN,
    RISK_TYPE_CASH_OVER,
    RISK_TYPE_HIGH_ADJUSTMENT,
    RISK_TYPE_ITEM_RETURNS,
    RISK_TYPE_REPEATED_ADJUSTMENT,
    RISK_TYPE_UNEXPLAINED_VARIANCE,
    RISK_TYPE_Z_READING_MISMATCH,
    AdjustmentParticipation,
    RiskObservation,
)


def build_risk_entries(
    observations: Sequence[RiskObservation],
    participations: Sequence[AdjustmentParticipation],
) -> list[dict[str, Any]]:
    """Return aggregated persistent risk entries."""

    entries = _aggregate_base_observations(observations)
    entries.extend(_aggregate_repeated_adjustments(participations))
    return sorted(
        entries,
        key=lambda entry: (
            _severity_rank(entry["severity"]),
            entry["branch"],
            entry["person_role"],
            entry["person_name"],
            entry["risk_type"],
        ),
        reverse=True,
    )


def build_summary(entries: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Return a dashboard-ready summary from risk entries."""

    severity_counts = {"low": 0, "medium": 0, "high": 0}
    role_counts: dict[str, int] = defaultdict(int)
    risk_type_counts: dict[str, int] = defaultdict(int)
    branch_counts: dict[str, int] = defaultdict(int)

    for entry in entries:
        severity = str(entry.get("severity") or "low")
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        role_counts[str(entry.get("person_role") or "unknown")] += 1
        risk_type_counts[str(entry.get("risk_type") or "unknown")] += 1
        branch_counts[str(entry.get("branch") or "unknown")] += 1

    return {
        "entry_count": len(entries),
        "severity_counts": severity_counts,
        "role_counts": dict(sorted(role_counts.items())),
        "risk_type_counts": dict(sorted(risk_type_counts.items())),
        "branch_counts": dict(sorted(branch_counts.items())),
    }


def _aggregate_base_observations(observations: Sequence[RiskObservation]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[RiskObservation]] = defaultdict(list)
    for observation in observations:
        grouped[
            (
                observation.branch,
                observation.person_role,
                observation.person_name,
                observation.risk_type,
            )
        ].append(observation)

    entries: list[dict[str, Any]] = []
    for key, items in grouped.items():
        branch, person_role, person_name, risk_type = key
        latest = max(items, key=lambda item: item.report_date)
        dates = sorted({item.report_date for item in items})
        total_amount = round(sum(item.amount for item in items), 2)
        review_flags = sorted({flag for item in items for flag in item.review_flags})
        severity = severity_for_entry(
            risk_type=risk_type,
            person_role=person_role,
            total_amount=total_amount,
            occurrence_count=len(items),
            review_flags=review_flags,
        )
        entries.append(
            {
                "risk_id": _risk_id(person_role, person_name, branch, risk_type),
                "branch": branch,
                "person_role": person_role,
                "person_name": person_name,
                "supervisor": latest.supervisor,
                "balanced_by": latest.balanced_by,
                "risk_type": risk_type,
                "occurrence_count": len(items),
                "total_amount": total_amount,
                "distinct_dates": dates,
                "last_seen": dates[-1],
                "source_records": sorted({item.source_path for item in items}),
                "severity": severity,
                "recommended_action": recommended_action_for_entry(
                    risk_type=risk_type,
                    severity=severity,
                    person_role=person_role,
                    review_flags=review_flags,
                ),
                "review_flags": review_flags,
            }
        )
    return entries


def _aggregate_repeated_adjustments(participations: Sequence[AdjustmentParticipation]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[AdjustmentParticipation]] = defaultdict(list)
    for participation in participations:
        grouped[
            (
                participation.branch,
                participation.person_role,
                participation.person_name,
            )
        ].append(participation)

    entries: list[dict[str, Any]] = []
    for key, items in grouped.items():
        branch, person_role, person_name = key
        window_days, threshold = repeated_adjustment_policy(person_role)
        if window_days is None or threshold is None:
            continue
        window_items = _best_window(items, window_days=window_days, threshold=threshold)
        if not window_items:
            continue
        latest = max(window_items, key=lambda item: item.report_date)
        dates = sorted({item.report_date for item in window_items})
        review_flags = sorted({flag for item in window_items for flag in item.review_flags})
        total_amount = round(sum(item.total_adjustment_amount for item in window_items), 2)
        severity = severity_for_entry(
            risk_type=RISK_TYPE_REPEATED_ADJUSTMENT,
            person_role=person_role,
            total_amount=total_amount,
            occurrence_count=len(window_items),
            review_flags=review_flags,
        )
        entries.append(
            {
                "risk_id": _risk_id(person_role, person_name, branch, RISK_TYPE_REPEATED_ADJUSTMENT),
                "branch": branch,
                "person_role": person_role,
                "person_name": person_name,
                "supervisor": latest.supervisor,
                "balanced_by": latest.balanced_by,
                "risk_type": RISK_TYPE_REPEATED_ADJUSTMENT,
                "occurrence_count": len(window_items),
                "total_amount": total_amount,
                "distinct_dates": dates,
                "last_seen": dates[-1],
                "source_records": sorted({item.source_path for item in window_items}),
                "severity": severity,
                "recommended_action": recommended_action_for_entry(
                    risk_type=RISK_TYPE_REPEATED_ADJUSTMENT,
                    severity=severity,
                    person_role=person_role,
                    review_flags=review_flags,
                ),
                "review_flags": review_flags,
                "window_days": window_days,
            }
        )
    return entries


def severity_for_entry(
    *,
    risk_type: str,
    person_role: str,
    total_amount: float,
    occurrence_count: int,
    review_flags: Iterable[str] = (),
) -> str:
    """Return the severity for one aggregated entry."""

    flags = set(review_flags)
    if risk_type == RISK_TYPE_UNEXPLAINED_VARIANCE:
        return "high"
    if risk_type == RISK_TYPE_BALANCED_BY_SAME_AS_CASHIER:
        return "medium"
    if risk_type == RISK_TYPE_Z_READING_MISMATCH:
        severity = "high" if total_amount > 50.0 else "medium"
        if "unexplained_variance_present" in flags and severity == "medium":
            return "high"
        return severity
    if risk_type == RISK_TYPE_REPEATED_ADJUSTMENT:
        if person_role == PERSON_ROLE_CASHIER and occurrence_count >= 3:
            return "high"
        if person_role == PERSON_ROLE_BALANCED_BY and occurrence_count >= 5:
            return "high"
        if person_role == PERSON_ROLE_SUPERVISOR and occurrence_count >= 3:
            return "medium"
        if person_role == PERSON_ROLE_CASHIER and occurrence_count >= 2:
            return "medium"
        return "low"
    if risk_type == RISK_TYPE_HIGH_ADJUSTMENT:
        if "z_reading_mismatch_unreconciled" in flags or total_amount > 50.0:
            return "high"
        return "medium"
    if risk_type == RISK_TYPE_ITEM_RETURNS:
        return "medium" if total_amount > 50.0 else "low"
    if risk_type in {RISK_TYPE_CASH_OVER, RISK_TYPE_CASH_DOWN}:
        if total_amount > 50.0:
            return "high"
        if total_amount > 20.0:
            return "medium"
        return "low"
    return "low"


def repeated_adjustment_policy(person_role: str) -> tuple[int | None, int | None]:
    """Return the repeated-adjustment policy for one role."""

    if person_role == PERSON_ROLE_CASHIER:
        return 7, 2
    if person_role == PERSON_ROLE_SUPERVISOR:
        return 7, 3
    if person_role == PERSON_ROLE_BALANCED_BY:
        return 14, 5
    return None, None


def _best_window(
    participations: Sequence[AdjustmentParticipation],
    *,
    window_days: int,
    threshold: int,
) -> list[AdjustmentParticipation]:
    dated_items = sorted(participations, key=lambda item: (item.report_date, item.source_path))
    best: list[AdjustmentParticipation] = []
    for index, current in enumerate(dated_items):
        start = _parse_date(current.report_date)
        window_end = start + timedelta(days=window_days - 1)
        window_items = [
            item
            for item in dated_items[index:]
            if _parse_date(item.report_date) <= window_end
        ]
        if len(window_items) < threshold:
            continue
        if len(window_items) > len(best):
            best = window_items
    return best


def _risk_id(person_role: str, person_name: str, branch: str, risk_type: str) -> str:
    return f"{person_role}|{person_name}|{branch}|{risk_type}"


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _severity_rank(severity: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(severity, 0)
