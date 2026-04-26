"""Deterministic executive assistant insights over existing TopTown data."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from analytics.phase5_executive import build_ceo_overview
from apps.proposals_store.store import list_proposals
from apps.supervisor_auth.worker import lookup_supervisor
from packages.common.analytics_loader import list_available_comparison_dates
from packages.common.executive_alerts import build_executive_alert_summary
from packages.learning_store import read_latest_learning_artifact

_QUERY_TYPES = {
    "executive_overview",
    "executive_alerts",
    "executive_learning",
    "executive_proposals",
}


def execute_executive_query(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Return one deterministic executive insight payload."""

    query_type = _clean_text(command.get("query_type"))
    sender_phone = _clean_text(command.get("sender_phone"))
    report_date = _clean_text(command.get("report_date"))
    if bool(command.get("is_replay") is True):
        return _result(
            status="failed",
            query_type=query_type,
            report_date=report_date,
            reason="replay_ignored",
            message="replay must not execute executive queries",
        )
    if query_type not in _QUERY_TYPES:
        return _result(
            status="failed",
            query_type=query_type,
            report_date=report_date,
            reason="unsupported_query_type",
            message="query type is not allowed",
        )

    supervisor = lookup_supervisor(sender_phone=sender_phone)
    if supervisor is None:
        return _result(
            status="unauthorized",
            query_type=query_type,
            report_date=report_date,
            reason="supervisor_not_found",
            message="sender is not authorized for executive queries",
        )

    effective_date = report_date or _latest_report_date(output_root)
    if effective_date is None:
        return _result(
            status="failed",
            query_type=query_type,
            report_date=report_date,
            reason="analytics_not_found",
            message="no executive analytics are available",
        )

    overview, overview_error = build_ceo_overview(effective_date, root=_root_str(output_root))
    if overview is None:
        return _result(
            status="failed",
            query_type=query_type,
            report_date=effective_date,
            reason=_clean_text((overview_error or {}).get("error")) or "analytics_not_found",
            message=_clean_text((overview_error or {}).get("message")) or "executive overview data unavailable",
        )

    alerts_summary, _ = build_executive_alert_summary(effective_date, root=output_root)
    learning = _learning_bundle(effective_date, output_root)
    proposals = _proposal_bundle(effective_date, output_root)

    if query_type == "executive_overview":
        insight = _overview_insight(overview, alerts_summary, learning, proposals)
    elif query_type == "executive_alerts":
        insight = _alerts_insight(alerts_summary, learning, proposals)
    elif query_type == "executive_learning":
        insight = _learning_insight(overview, learning, proposals)
    else:
        insight = _proposal_insight(alerts_summary, learning, proposals)

    return _result(
        status="completed",
        query_type=query_type,
        report_date=effective_date,
        reason="insight_ready",
        message="executive insight ready",
        insight=insight,
    )


def _overview_insight(
    overview: Mapping[str, Any],
    alerts_summary: Mapping[str, Any] | None,
    learning: Mapping[str, Any],
    proposals: Mapping[str, Any],
) -> dict[str, Any]:
    counts = dict(alerts_summary.get("counts_by_severity")) if isinstance(alerts_summary, Mapping) else {}
    return {
        "total_gross_sales": overview.get("total_gross_sales"),
        "branches_reporting_count": overview.get("branches_reporting_count"),
        "top_branch": _nested_text(overview, "top_branch_by_operational_score", "branch"),
        "weakest_branch": _nested_text(overview, "weakest_branch_by_operational_score", "branch"),
        "critical_alert_count": _int_or_zero(counts.get("critical")),
        "pending_proposals_count": _int_or_zero(proposals.get("pending_count")),
        "top_recurring_review_cause": _clean_text(learning.get("top_recurring_review_cause")),
    }


def _alerts_insight(
    alerts_summary: Mapping[str, Any] | None,
    learning: Mapping[str, Any],
    proposals: Mapping[str, Any],
) -> dict[str, Any]:
    counts = dict(alerts_summary.get("counts_by_severity")) if isinstance(alerts_summary, Mapping) else {}
    alerts = alerts_summary.get("alerts") if isinstance(alerts_summary, Mapping) else None
    highest_risk_branch = None
    if isinstance(alerts, list):
        for row in alerts:
            if not isinstance(row, Mapping):
                continue
            highest_risk_branch = _clean_text(row.get("branch"))
            if highest_risk_branch is not None:
                break
    return {
        "critical_alert_count": _int_or_zero(counts.get("critical")),
        "warning_alert_count": _int_or_zero(counts.get("warning")),
        "highest_risk_branch": highest_risk_branch,
        "pending_proposals_count": _int_or_zero(proposals.get("pending_count")),
        "top_recurring_review_cause": _clean_text(learning.get("top_recurring_review_cause")),
    }


def _learning_insight(
    overview: Mapping[str, Any],
    learning: Mapping[str, Any],
    proposals: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "top_recurring_review_cause": _clean_text(learning.get("top_recurring_review_cause")),
        "top_threshold_recommendation": _clean_text(learning.get("top_threshold_recommendation")),
        "noisy_rule": _clean_text(learning.get("noisy_rule")),
        "weakest_branch": _nested_text(overview, "weakest_branch_by_operational_score", "branch"),
        "pending_proposals_count": _int_or_zero(proposals.get("pending_count")),
    }


def _proposal_insight(
    alerts_summary: Mapping[str, Any] | None,
    learning: Mapping[str, Any],
    proposals: Mapping[str, Any],
) -> dict[str, Any]:
    counts = dict(alerts_summary.get("counts_by_severity")) if isinstance(alerts_summary, Mapping) else {}
    return {
        "pending_proposals_count": _int_or_zero(proposals.get("pending_count")),
        "approved_not_applied_count": _int_or_zero(proposals.get("approved_not_applied_count")),
        "leading_pending_branch": _clean_text(proposals.get("leading_pending_branch")),
        "top_recurring_review_cause": _clean_text(learning.get("top_recurring_review_cause")),
        "critical_alert_count": _int_or_zero(counts.get("critical")),
    }


def _learning_bundle(report_date: str, output_root: str | Path | None) -> dict[str, Any]:
    review = _latest_learning_before_or_on("review_summary", report_date, output_root)
    recommendations = _latest_learning_before_or_on("threshold_recommendations", report_date, output_root)
    actions = _latest_learning_before_or_on("action_effectiveness", report_date, output_root)

    recurring = None
    if isinstance(review, Mapping):
        recurring_rows = review.get("recurring_review_causes")
        if isinstance(recurring_rows, list) and recurring_rows:
            first = recurring_rows[0]
            if isinstance(first, Mapping):
                recurring = _clean_text(first.get("reason"))

    recommendation = None
    if isinstance(recommendations, Mapping):
        rows = recommendations.get("recommendations")
        if isinstance(rows, list) and rows:
            first = rows[0]
            if isinstance(first, Mapping):
                recommendation = _clean_text(first.get("recommendation_id")) or _clean_text(first.get("target_area"))

    noisy_rule = None
    if isinstance(actions, Mapping):
        rows = actions.get("noisy_rules_candidates")
        if isinstance(rows, list) and rows:
            first = rows[0]
            if isinstance(first, Mapping):
                noisy_rule = _clean_text(first.get("rule_code"))

    return {
        "top_recurring_review_cause": recurring,
        "top_threshold_recommendation": recommendation,
        "noisy_rule": noisy_rule,
    }


def _proposal_bundle(report_date: str, output_root: str | Path | None) -> dict[str, Any]:
    visible = [
        proposal
        for proposal in list_proposals(output_root=output_root)
        if _clean_text(proposal.get("generated_date")) is not None
        and _clean_text(proposal.get("generated_date")) <= report_date
    ]
    pending = [proposal for proposal in visible if _clean_text(proposal.get("approval_status")) == "pending"]
    approved_not_applied = [
        proposal
        for proposal in visible
        if _clean_text(proposal.get("approval_status")) == "approved"
        and _clean_text(proposal.get("apply_status")) != "applied"
    ]
    pending_by_branch = Counter(
        _clean_text(item.get("branch"))
        for item in pending
        if _clean_text(item.get("branch")) is not None
    )
    leading_pending_branch = None
    if pending_by_branch:
        leading_pending_branch = sorted(
            pending_by_branch.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
    return {
        "pending_count": len(pending),
        "approved_not_applied_count": len(approved_not_applied),
        "leading_pending_branch": leading_pending_branch,
    }


def _latest_learning_before_or_on(
    category: str,
    report_date: str,
    output_root: str | Path | None,
) -> dict[str, Any] | None:
    category_root = _records_root(output_root) / "learning" / category
    if not category_root.exists():
        return None
    candidates = sorted(path for path in category_root.glob("*.json") if path.stem <= report_date)
    if not candidates:
        return None
    return _read_json(candidates[-1])


def _latest_report_date(output_root: str | Path | None) -> str | None:
    available = list_available_comparison_dates(root=output_root)
    if not available:
        return None
    return available[0]


def _root_str(output_root: str | Path | None) -> str | None:
    if output_root is None:
        return None
    return str(output_root)


def _records_root(output_root: str | Path | None) -> Path:
    return (Path(output_root) if output_root is not None else Path(".")) / "records"


def _read_json(path: Path) -> dict[str, Any] | None:
    import json

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, dict) else None


def _result(
    *,
    status: str,
    query_type: str | None,
    report_date: str | None,
    reason: str,
    message: str,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "status": status,
        "query_type": query_type,
        "report_date": report_date,
        "reason": reason,
        "message": message,
    }
    payload.update(extra)
    return payload


def _nested_text(payload: Mapping[str, Any], *path: str) -> str | None:
    current: Any = payload
    for part in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return _clean_text(current)


def _nested_int(payload: Mapping[str, Any], *path: str) -> int:
    current: Any = payload
    for part in path:
        if not isinstance(current, Mapping):
            return 0
        current = current.get(part)
    return _int_or_zero(current)


def _int_or_zero(value: object) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return 0


def _normalize_date(value: str) -> str:
    return date.fromisoformat(value.strip()).isoformat()


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
