"""Conservative autonomous control rules for approved structured records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
import hashlib
import json
from typing import Any

from apps.sales_income_agent.customer_metrics import LOW_CONVERSION_THRESHOLD

_ATTENDANCE_SHORTAGE_THRESHOLD = 0.25


def generate_control_actions(
    *,
    structured_payload: Mapping[str, Any] | None,
    governance_sidecar: Mapping[str, Any] | None,
    analytics_context: Mapping[str, Any] | None = None,
    replay: bool = False,
    allow_replay: bool = False,
    source_paths: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Return deterministic action decisions for one governed structured record."""

    del analytics_context

    payload = dict(structured_payload) if isinstance(structured_payload, Mapping) else {}
    governance = dict(governance_sidecar) if isinstance(governance_sidecar, Mapping) else {}
    normalized_source_paths = [path for path in source_paths or [] if isinstance(path, str) and path.strip()]

    if replay and not allow_replay:
        return {
            "status": "suppressed_replay",
            "reason": "replay_suppressed",
            "actions": [],
        }

    governance_status = _string_or_none(governance.get("status"))
    export_allowed = governance.get("export_allowed") is True or payload.get("export_allowed") is True
    if governance_status not in {"accepted", "accepted_with_warning"}:
        return {
            "status": "skipped",
            "reason": "governance_not_actionable",
            "actions": [],
        }
    if not export_allowed:
        return {
            "status": "skipped",
            "reason": "export_not_allowed",
            "actions": [],
        }

    branch = _string_or_none(payload.get("branch"))
    report_date = _string_or_none(payload.get("report_date"))
    signal_type = _string_or_none(payload.get("signal_type"))
    if branch is None or report_date is None or signal_type is None:
        return {
            "status": "skipped",
            "reason": "missing_scope",
            "actions": [],
        }

    actions: list[dict[str, Any]] = []
    actions.extend(
        _low_conversion_actions(
            payload=payload,
            branch=branch,
            report_date=report_date,
            signal_type=signal_type,
            source_paths=normalized_source_paths,
        )
    )
    actions.extend(
        _attendance_shortage_actions(
            payload=payload,
            branch=branch,
            report_date=report_date,
            signal_type=signal_type,
            source_paths=normalized_source_paths,
        )
    )
    actions.extend(
        _pricing_gap_actions(
            payload=payload,
            branch=branch,
            report_date=report_date,
            signal_type=signal_type,
            source_paths=normalized_source_paths,
        )
    )
    actions.extend(
        _supervisor_exception_actions(
            payload=payload,
            branch=branch,
            report_date=report_date,
            signal_type=signal_type,
            source_paths=normalized_source_paths,
        )
    )

    return {
        "status": "generated" if actions else "skipped",
        "reason": "actions_generated" if actions else "no_rule_triggered",
        "actions": actions,
    }


def _low_conversion_actions(
    *,
    payload: Mapping[str, Any],
    branch: str,
    report_date: str,
    signal_type: str,
    source_paths: list[str],
) -> list[dict[str, Any]]:
    if signal_type != "sales_income":
        return []

    metrics = _mapping(payload.get("metrics"))
    traffic = _int_or_none(metrics.get("traffic"))
    served = _int_or_none(metrics.get("served"))
    conversion_rate = _float_or_none(metrics.get("conversion_rate"))
    if traffic is None or traffic <= 0 or served is None or served <= 0 or conversion_rate is None:
        return []
    if conversion_rate >= LOW_CONVERSION_THRESHOLD:
        return []

    return [
        _build_action(
            rule_code="low_conversion_rate",
            branch=branch,
            report_date=report_date,
            signal_type=signal_type,
            scope_key="branch_conversion",
            severity="warning",
            priority="high",
            assigned_to="branch_supervisor",
            requires_ack=True,
            summary="Review floor engagement and stock availability due to low conversion.",
            evidence={
                "traffic": traffic,
                "served": served,
                "conversion_rate": round(conversion_rate, 4),
                "threshold": LOW_CONVERSION_THRESHOLD,
            },
            source_paths=source_paths,
        )
    ]


def _attendance_shortage_actions(
    *,
    payload: Mapping[str, Any],
    branch: str,
    report_date: str,
    signal_type: str,
    source_paths: list[str],
) -> list[dict[str, Any]]:
    if signal_type != "hr_attendance":
        return []

    metrics = _mapping(payload.get("metrics"))
    total_staff_listed = _int_or_none(metrics.get("total_staff_listed"))
    absent_count = _int_or_none(metrics.get("absent_count"))
    if total_staff_listed is None or total_staff_listed <= 0 or absent_count is None or absent_count <= 0:
        return []

    absence_ratio = round(absent_count / total_staff_listed, 4)
    if absence_ratio < _ATTENDANCE_SHORTAGE_THRESHOLD:
        return []

    evidence = {
        "absent_count": absent_count,
        "total_staff_listed": total_staff_listed,
        "absence_ratio": absence_ratio,
        "threshold": _ATTENDANCE_SHORTAGE_THRESHOLD,
    }
    coverage_ratio = _float_or_none(metrics.get("coverage_ratio"))
    if coverage_ratio is not None:
        evidence["coverage_ratio"] = round(coverage_ratio, 4)
    attendance_gap = _int_or_none(metrics.get("attendance_gap"))
    if attendance_gap is not None:
        evidence["attendance_gap"] = attendance_gap

    return [
        _build_action(
            rule_code="attendance_shortage",
            branch=branch,
            report_date=report_date,
            signal_type=signal_type,
            scope_key="branch_staffing",
            severity="warning",
            priority="high",
            assigned_to="branch_supervisor",
            requires_ack=True,
            summary="Confirm staffing coverage for the branch attendance shortfall.",
            evidence=evidence,
            source_paths=source_paths,
        )
    ]


def _pricing_gap_actions(
    *,
    payload: Mapping[str, Any],
    branch: str,
    report_date: str,
    signal_type: str,
    source_paths: list[str],
) -> list[dict[str, Any]]:
    if signal_type != "pricing_stock_release":
        return []

    items = payload.get("items")
    if not isinstance(items, list):
        return []

    actions: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        item_payload = _mapping(item)
        qty = _float_or_none(item_payload.get("qty"))
        price_per_piece = _float_or_none(item_payload.get("price_per_piece"))
        if qty is None or qty <= 0 or price_per_piece not in {None, 0.0}:
            continue
        bale_id = _string_or_none(item_payload.get("bale_id")) or f"item_{index + 1}"
        item_name = _string_or_none(item_payload.get("item_name")) or bale_id
        actions.append(
            _build_action(
                rule_code="pricing_gap_review",
                branch=branch,
                report_date=report_date,
                signal_type=signal_type,
                scope_key=bale_id,
                severity="warning",
                priority="medium",
                assigned_to="pricing_supervisor",
                requires_ack=False,
                summary=f"Verify pricing for {item_name} before manual release follow-up.",
                evidence={
                    "bale_id": bale_id,
                    "item_name": item_name,
                    "qty": qty,
                    "amount": _float_or_none(item_payload.get("amount")),
                    "price_per_piece": price_per_piece,
                },
                source_paths=source_paths,
            )
        )
    return actions


def _supervisor_exception_actions(
    *,
    payload: Mapping[str, Any],
    branch: str,
    report_date: str,
    signal_type: str,
    source_paths: list[str],
) -> list[dict[str, Any]]:
    if signal_type != "supervisor_control":
        return []

    metrics = _mapping(payload.get("metrics"))
    open_exception_count = _int_or_none(metrics.get("open_exception_count")) or 0
    escalated_count = _int_or_none(metrics.get("escalated_count")) or 0
    if open_exception_count <= 0 and escalated_count <= 0:
        return []

    return [
        _build_action(
            rule_code="unresolved_supervisor_exception",
            branch=branch,
            report_date=report_date,
            signal_type=signal_type,
            scope_key="open_supervisor_exceptions",
            severity="critical" if escalated_count > 0 else "warning",
            priority="high" if escalated_count > 0 else "medium",
            assigned_to="branch_supervisor",
            requires_ack=True,
            summary="Confirm open supervisor exceptions and close any unresolved controls.",
            evidence={
                "open_exception_count": open_exception_count,
                "escalated_count": escalated_count,
                "control_gap_count": _int_or_none(metrics.get("control_gap_count")) or 0,
            },
            source_paths=source_paths,
        )
    ]


def _build_action(
    *,
    rule_code: str,
    branch: str,
    report_date: str,
    signal_type: str,
    scope_key: str,
    severity: str,
    priority: str,
    assigned_to: str,
    requires_ack: bool,
    summary: str,
    evidence: Mapping[str, Any],
    source_paths: Sequence[str],
) -> dict[str, Any]:
    dedupe_key = f"{branch}:{report_date}:{rule_code}:{scope_key}"
    return {
        "action_id": _action_id(dedupe_key),
        "action_type": rule_code,
        "rule_code": rule_code,
        "branch": branch,
        "report_date": report_date,
        "signal_type": signal_type,
        "severity": severity,
        "priority": priority,
        "assigned_to": assigned_to,
        "requires_ack": requires_ack,
        "status": "pending",
        "expires_at": _default_expiry(report_date),
        "dedupe_key": dedupe_key,
        "scope_key": scope_key,
        "summary": summary,
        "evidence": dict(evidence),
        "source_paths": list(source_paths),
    }


def _action_id(dedupe_key: str) -> str:
    digest = hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()
    return digest[:16]


def _default_expiry(report_date: str) -> str:
    try:
        base_date = date.fromisoformat(report_date)
    except ValueError:
        return f"{report_date}T23:59:59Z"
    return datetime.combine(base_date + timedelta(days=1), datetime.min.time()).replace(hour=23, minute=59, second=59).isoformat() + "Z"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _float_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _int_or_none(value: object) -> int | None:
    number = _float_or_none(value)
    if number is None:
        return None
    return int(number)
