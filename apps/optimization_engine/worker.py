"""Deterministic optimization proposal generation from learning and analytics."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
import hashlib
from pathlib import Path
from typing import Any

from apps.proposals_store.store import write_optimization_proposal
from packages.common.analytics_loader import load_branch_comparison
from packages.learning_store import read_latest_learning_artifact

AGENT_NAME = "optimization_engine"
SIGNAL_TYPE = "optimization_proposals"
_MIN_BRANCH_REVIEW_COUNT = 2


def build_optimization_proposals(
    report_date: str,
    *,
    output_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return deterministic proposals without persisting anything."""

    normalized_date = _normalize_date(report_date)
    review_artifact = _load_artifact("review_summary", normalized_date, output_root)
    action_artifact = _load_artifact("action_effectiveness", normalized_date, output_root)
    comparison_artifact = _load_branch_comparison(normalized_date, output_root)
    if review_artifact is None or comparison_artifact is None:
        return []

    review_payload = review_artifact["payload"]
    action_payload = action_artifact["payload"] if action_artifact is not None else None
    comparison_payload = comparison_artifact["payload"]

    branch_heatmap = review_payload.get("branch_review_heatmap")
    scorecards = _scorecards_by_branch(comparison_payload.get("branch_scorecards"))
    action_by_branch = _action_by_branch(action_payload)
    source_paths = _source_paths(review_artifact, action_artifact, comparison_artifact)

    proposals: list[dict[str, Any]] = []
    if not isinstance(branch_heatmap, list):
        return proposals

    for branch_entry in branch_heatmap:
        if not isinstance(branch_entry, Mapping):
            continue
        branch = _text_or_none(branch_entry.get("branch"))
        total_reviews = _int_or_none(branch_entry.get("total_reviews"))
        if branch is None or total_reviews is None or total_reviews < _MIN_BRANCH_REVIEW_COUNT:
            continue

        leading_reason = _leading_counter_value(branch_entry.get("by_reason"), "reason")
        leading_report_type = _leading_counter_value(branch_entry.get("by_report_type"), "report_type")
        if leading_reason is None or leading_report_type is None:
            continue

        scorecard = scorecards.get(branch, {})
        action_context = action_by_branch.get(branch, {})
        proposal_id = _proposal_id(normalized_date, branch, leading_reason)
        priority = _priority(total_reviews=total_reviews, operational_score=_float_or_none(scorecard.get("operational_score")))
        proposed_action = _proposed_action(
            proposal_id=proposal_id,
            branch=branch,
            report_date=normalized_date,
            leading_reason=leading_reason,
            priority=priority,
            evidence={
                "review_count": total_reviews,
                "leading_reason": leading_reason,
                "report_type": leading_report_type,
                "operational_score": _float_or_none(scorecard.get("operational_score")),
                "conversion_rate": _float_or_none(scorecard.get("conversion_rate")),
                "warning_count": _int_or_none(scorecard.get("warning_count")),
                "branch_action_dismissed_rate": _float_or_none(action_context.get("dismissed_rate")),
                "branch_action_stale_pending_rate": _float_or_none(action_context.get("stale_pending_rate")),
            },
            source_paths=source_paths,
        )
        proposals.append(
            {
                "proposal_id": proposal_id,
                "generated_date": normalized_date,
                "proposal_type": "branch_optimization_action",
                "status": "pending_review",
                "approval_status": "pending",
                "apply_status": "not_applied",
                "branch": branch,
                "report_type": leading_report_type,
                "summary": f"Create a branch follow-up action for repeated review cause `{leading_reason}`.",
                "priority": priority,
                "requires_human_approval": True,
                "evidence": {
                    "review_count": total_reviews,
                    "leading_reason": leading_reason,
                    "report_type": leading_report_type,
                    "analytics_snapshot": {
                        "operational_score": _float_or_none(scorecard.get("operational_score")),
                        "conversion_rate": _float_or_none(scorecard.get("conversion_rate")),
                        "warning_count": _int_or_none(scorecard.get("warning_count")),
                    },
                    "learning_snapshot": {
                        "branch_action_dismissed_rate": _float_or_none(action_context.get("dismissed_rate")),
                        "branch_action_stale_pending_rate": _float_or_none(action_context.get("stale_pending_rate")),
                    },
                },
                "proposed_action": proposed_action,
                "source_paths": source_paths,
            }
        )

    proposals.sort(key=lambda item: (item["branch"], item["proposal_id"]))
    return proposals


def generate_optimization_proposals(
    report_date: str,
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Generate and persist deterministic optimization proposals."""

    proposals = build_optimization_proposals(report_date, output_root=output_root)
    output_paths = [write_optimization_proposal(proposal, output_root=output_root) for proposal in proposals]
    return {
        "status": "written",
        "proposal_count": len(proposals),
        "output_paths": output_paths,
        "proposals": proposals,
        "output_path": output_paths[0] if output_paths else None,
    }


def _load_artifact(
    category: str,
    report_date: str,
    output_root: str | Path | None,
) -> dict[str, Any] | None:
    root = _learning_root(output_root) / category
    if not root.exists():
        return None
    candidates = sorted(path for path in root.glob("*.json") if path.stem <= report_date)
    if not candidates:
        payload = read_latest_learning_artifact(category, output_root=output_root)
        if payload is None:
            return None
        latest_path = root / f"{payload['date']}.json"
        return {"path": str(latest_path), "payload": payload}
    target = candidates[-1]
    payload = _read_json(target)
    if payload is None:
        return None
    return {"path": str(target), "payload": payload}


def _load_branch_comparison(report_date: str, output_root: str | Path | None) -> dict[str, Any] | None:
    comparison_root = _analytics_root(output_root) / "branch_comparison"
    if not comparison_root.exists():
        return None
    candidates = sorted(path for path in comparison_root.glob("*.json") if path.stem <= report_date)
    target_date = candidates[-1].stem if candidates else report_date
    payload, _ = load_branch_comparison(report_date=target_date, root=output_root)
    if payload is None:
        return None
    return {
        "path": str(comparison_root / f"{target_date}.json"),
        "payload": payload,
    }


def _scorecards_by_branch(value: object) -> dict[str, dict[str, Any]]:
    if not isinstance(value, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in value:
        if not isinstance(item, Mapping):
            continue
        branch = _text_or_none(item.get("branch"))
        if branch is None:
            continue
        result[branch] = dict(item)
    return result


def _action_by_branch(action_payload: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    rows = action_payload.get("action_response_by_branch") if isinstance(action_payload, Mapping) else None
    if not isinstance(rows, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        branch = _text_or_none(item.get("branch"))
        if branch is None:
            continue
        result[branch] = dict(item)
    return result


def _source_paths(*artifacts: dict[str, Any] | None) -> list[str]:
    paths = []
    for artifact in artifacts:
        if artifact is None:
            continue
        path = _text_or_none(artifact.get("path"))
        if path is not None:
            paths.append(path)
        payload = artifact.get("payload")
        if isinstance(payload, Mapping):
            source_paths = payload.get("source_paths")
            if isinstance(source_paths, list):
                paths.extend(item for item in source_paths if isinstance(item, str) and item.strip())
    return sorted(set(paths))


def _leading_counter_value(rows: object, field_name: str) -> str | None:
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    if not isinstance(first, Mapping):
        return None
    return _text_or_none(first.get(field_name))


def _proposal_id(report_date: str, branch: str, leading_reason: str) -> str:
    base = f"{report_date}:{branch}:{leading_reason}:branch_optimization_action"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:12]
    return f"{branch}__{leading_reason}__{digest}"


def _proposed_action(
    *,
    proposal_id: str,
    branch: str,
    report_date: str,
    leading_reason: str,
    priority: str,
    evidence: dict[str, Any],
    source_paths: list[str],
) -> dict[str, Any]:
    summary = f"Review branch report quality controls for repeated issue `{leading_reason}`."
    return {
        "action_id": hashlib.sha256(f"action:{proposal_id}".encode("utf-8")).hexdigest()[:16],
        "action_type": "report_quality_coaching",
        "rule_code": "report_quality_coaching",
        "branch": branch,
        "report_date": report_date,
        "signal_type": "optimization_proposal",
        "severity": "warning",
        "priority": priority,
        "assigned_to": "branch_supervisor",
        "requires_ack": True,
        "status": "pending",
        "expires_at": _expires_at(report_date),
        "dedupe_key": f"{branch}:{report_date}:report_quality_coaching:{leading_reason}",
        "scope_key": leading_reason,
        "summary": summary,
        "evidence": evidence,
        "source_paths": list(source_paths),
    }


def _priority(*, total_reviews: int, operational_score: float | None) -> str:
    if total_reviews >= 3:
        return "high"
    if operational_score is not None and operational_score < 80:
        return "high"
    return "medium"


def _expires_at(report_date: str) -> str:
    day = datetime.strptime(report_date, "%Y-%m-%d").date() + timedelta(days=1)
    return f"{day.isoformat()}T23:59:59Z"


def _normalize_date(value: str) -> str:
    return date.fromisoformat(value.strip()).isoformat()


def _learning_root(output_root: str | Path | None) -> Path:
    return (Path(output_root) if output_root is not None else Path(".")) / "records" / "learning"


def _analytics_root(output_root: str | Path | None) -> Path:
    return (Path(output_root) if output_root is not None else Path(".")) / "analytics"


def _read_json(path: Path) -> dict[str, Any] | None:
    import json

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, dict) else None


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _float_or_none(value: object) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None
