"""Deterministic cross-branch analytics queries over existing analytics outputs."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from apps.supervisor_auth.worker import authorize_supervisor
from packages.common.analytics_loader import load_branch_comparison, list_available_comparison_dates

_QUERY_SPECS = {
    "branch_sales_rank": {
        "ranking_key": "ranked_branches_by_sales",
        "value_key": "gross_sales",
    },
    "branch_conversion_rank": {
        "ranking_key": "ranked_branches_by_conversion",
        "value_key": "conversion_rate",
    },
    "branch_productivity_rank": {
        "ranking_key": "ranked_branches_by_staff_productivity",
        "value_key": "staff_productivity_index",
    },
    "branch_operational_rank": {
        "ranking_key": "ranked_branches_by_operational_score",
        "value_key": "operational_score",
    },
}


def execute_cross_branch_query(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Return one deterministic cross-branch query result from analytics files only."""

    query_type = _clean_text(command.get("query_type"))
    branch = _clean_text(command.get("branch"))
    sender_phone = _clean_text(command.get("sender_phone"))
    report_date = _clean_text(command.get("report_date"))
    if bool(command.get("is_replay") is True):
        return _result(
            status="failed",
            query_type=query_type,
            branch=branch,
            report_date=report_date,
            reason="replay_ignored",
            message="replay must not execute cross-branch queries",
        )

    spec = _QUERY_SPECS.get(query_type or "")
    if spec is None:
        return _result(
            status="failed",
            query_type=query_type,
            branch=branch,
            report_date=report_date,
            reason="unsupported_query_type",
            message="query type is not allowed",
        )
    authorization = authorize_supervisor(sender_phone=sender_phone, branch=branch)
    if authorization["authorized"] is not True:
        return _result(
            status="unauthorized",
            query_type=query_type,
            branch=branch,
            report_date=report_date,
            reason=str(authorization["reason"]),
            message="sender is not authorized for this branch",
        )

    effective_date = report_date or _latest_report_date(output_root)
    if effective_date is None:
        return _result(
            status="failed",
            query_type=query_type,
            branch=branch,
            report_date=report_date,
            reason="analytics_not_found",
            message="no branch comparison analytics are available",
        )

    comparison_payload, not_found = load_branch_comparison(report_date=effective_date, root=output_root)
    if comparison_payload is None:
        return _result(
            status="failed",
            query_type=query_type,
            branch=branch,
            report_date=effective_date,
            reason="analytics_not_found",
            message="branch comparison analytics were not found",
            expected_path=None if not_found is None else not_found.expected_path,
        )

    ranked_rows = _ranking_rows(comparison_payload.get(spec["ranking_key"]))
    target_row = _row_for_branch(ranked_rows, branch)
    if target_row is None:
        return _result(
            status="failed",
            query_type=query_type,
            branch=branch,
            report_date=effective_date,
            reason="branch_not_present",
            message="branch is not present in the branch comparison ranking",
        )

    top_row = ranked_rows[0] if ranked_rows else None
    return _result(
        status="completed",
        query_type=query_type,
        branch=branch,
        report_date=effective_date,
        reason="query_completed",
        message="query completed",
        rank=int(target_row.get("rank") or 0),
        total_branches=len(ranked_rows),
        branch_value=target_row.get(spec["value_key"]),
        top_branch=_clean_text(None if top_row is None else top_row.get("branch")),
        top_value=None if top_row is None else top_row.get(spec["value_key"]),
    )


def _latest_report_date(output_root: str | Path | None) -> str | None:
    available = list_available_comparison_dates(root=output_root)
    if not available:
        return None
    return available[0]


def _ranking_rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            rows.append(dict(item))
    return rows


def _row_for_branch(rows: list[dict[str, Any]], branch: str | None) -> dict[str, Any] | None:
    if branch is None:
        return None
    for row in rows:
        if _clean_text(row.get("branch")) == branch:
            return row
    return None


def _result(
    *,
    status: str,
    query_type: str | None,
    branch: str | None,
    report_date: str | None,
    reason: str,
    message: str,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "status": status,
        "query_type": query_type,
        "branch": branch,
        "report_date": report_date,
        "reason": reason,
        "message": message,
    }
    payload.update(extra)
    return payload


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
