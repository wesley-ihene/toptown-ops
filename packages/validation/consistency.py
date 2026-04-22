"""Helpers for daily consistency aggregation."""

from __future__ import annotations

from typing import Any

from .types import utc_now_iso


def merge_consistency_snapshot(
    payload: dict[str, Any] | None,
    *,
    report_date: str,
    branch: str,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    """Merge one branch-level consistency snapshot into the daily artifact."""

    document = payload if isinstance(payload, dict) else _empty_payload(report_date)
    document.setdefault("branches", {})[branch] = dict(snapshot)
    issue_count = 0
    for branch_payload in document["branches"].values():
        issues = branch_payload.get("issues")
        if isinstance(issues, list):
            issue_count += len(issues)
    document["generated_at_utc"] = utc_now_iso()
    document["summary"] = {
        "branch_count": len(document["branches"]),
        "issue_count": issue_count,
        "status": "pass" if issue_count == 0 else "warn",
    }
    return document


def _empty_payload(report_date: str) -> dict[str, Any]:
    return {
        "report_date": report_date,
        "generated_at_utc": utc_now_iso(),
        "branches": {},
        "summary": {"branch_count": 0, "issue_count": 0, "status": "pass"},
    }
