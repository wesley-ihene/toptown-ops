"""Deterministic threshold recommendation summaries."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from packages.learning_store import write_threshold_recommendations
from packages.report_policy import get_report_policy
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "threshold_recommendation_engine"
SIGNAL_TYPE = "threshold_recommendations"


@dataclass(slots=True)
class ThresholdRecommendationEngineWorker:
    """Generate one daily threshold recommendation artifact."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    report_date = payload.get("report_date") or payload.get("date")
    output_root = payload.get("root") or payload.get("output_root")
    generated_at = payload.get("generated_at")

    if not isinstance(report_date, str) or not report_date.strip():
        return _failure_result("report_date is required for threshold recommendation analysis.")

    result = generate_threshold_recommendations(
        report_date.strip(),
        output_root=_path_or_none(output_root),
        generated_at=generated_at if isinstance(generated_at, str) else None,
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "report_date": report_date.strip(),
            "status": "written",
            "output_path": result["output_path"],
            "threshold_recommendations": result["summary"],
        },
    )


def generate_threshold_recommendations(
    report_date: str,
    *,
    output_root: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Generate threshold recommendations without mutating live policy."""

    normalized_report_date = _normalize_date(report_date)
    review_summary = _load_latest_learning_summary("review_summary", normalized_report_date, output_root)
    action_summary = _load_latest_learning_summary("action_effectiveness", normalized_report_date, output_root)

    review_payload = review_summary["payload"] if review_summary is not None else None
    action_payload = action_summary["payload"] if action_summary is not None else None

    recommendations = _build_recommendations(
        report_date=normalized_report_date,
        review_summary=review_payload,
        action_summary=action_payload,
    )
    summary_payload = {
        "artifact_type": SIGNAL_TYPE,
        "report_date": normalized_report_date,
        "recommendation_count": len(recommendations),
        "recommendations": recommendations,
        "source_summary_status": {
            "review_summary_path": review_summary["path"] if review_summary is not None else None,
            "action_effectiveness_path": action_summary["path"] if action_summary is not None else None,
        },
    }

    output_path = write_threshold_recommendations(
        normalized_report_date,
        summary_payload,
        generated_at=generated_at or f"{normalized_report_date}T00:00:00Z",
        source_paths=[
            path
            for path in (
                review_summary["path"] if review_summary is not None else None,
                action_summary["path"] if action_summary is not None else None,
            )
            if isinstance(path, str) and path.strip()
        ],
        analysis_window=_analysis_window(review_payload, action_payload, normalized_report_date),
        output_root=output_root,
    )
    return {
        "status": "written",
        "output_path": output_path,
        "summary": json.loads(Path(output_path).read_text(encoding="utf-8")),
    }


def _build_recommendations(
    *,
    report_date: str,
    review_summary: Mapping[str, Any] | None,
    action_summary: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(review_summary, Mapping):
        return []

    review_volume = _review_volume_by_report_type(review_summary)
    false_low = _candidate_counts(
        review_summary,
        "false_low_confidence_candidates",
    )
    false_high = _candidate_counts(
        review_summary,
        "false_high_confidence_candidates",
    )
    action_context = _action_context(action_summary)

    recommendations: list[dict[str, Any]] = []
    for report_type in sorted(set(false_low) | set(false_high)):
        volume = review_volume.get(report_type, 0)
        low_count = false_low.get(report_type, 0)
        high_count = false_high.get(report_type, 0)

        if low_count >= 2 and low_count >= high_count:
            recommendation = _false_low_recommendation(
                report_date=report_date,
                report_type=report_type,
                review_volume=volume,
                evidence_count=low_count,
                action_context=action_context,
            )
            if recommendation is not None:
                recommendations.append(recommendation)

        if high_count >= 2 and high_count > low_count:
            recommendation = _false_high_recommendation(
                report_date=report_date,
                report_type=report_type,
                review_volume=volume,
                evidence_count=high_count,
                action_context=action_context,
            )
            if recommendation is not None:
                recommendations.append(recommendation)

    return sorted(
        recommendations,
        key=lambda item: (
            _priority_rank(item["priority"]),
            item["target_area"],
            item["recommendation_id"],
        ),
    )


def _false_low_recommendation(
    *,
    report_date: str,
    report_type: str,
    review_volume: int,
    evidence_count: int,
    action_context: Mapping[str, Any],
) -> dict[str, Any] | None:
    try:
        policy = get_report_policy(report_type)
    except ValueError:
        return None

    share = _share(evidence_count, review_volume)
    step = _threshold_step(evidence_count, share)
    current = round(policy.confidence_thresholds.auto_accept_min, 2)
    proposed = round(max(policy.confidence_thresholds.review_min, current - step), 2)
    if proposed >= current:
        return None

    confidence = _recommendation_confidence(evidence_count, share, action_context)
    impact_score = _impact_score(evidence_count, share, action_context)
    return {
        "recommendation_id": f"{policy.report_type}__auto_accept_min__decrease",
        "target_area": f"report_policy.{policy.report_type}.confidence_thresholds.auto_accept_min",
        "current_threshold": current,
        "proposed_threshold": proposed,
        "reason": "Repeated low-confidence review items were later acceptable after manual review.",
        "evidence": {
            "report_type": policy.report_type,
            "review_volume": review_volume,
            "false_low_confidence_count": evidence_count,
            "false_low_confidence_share": share,
            "action_context": dict(action_context),
        },
        "confidence": confidence,
        "impact_score": impact_score,
        "priority": _priority_from_impact(impact_score),
        "requires_human_approval": True,
        "simulation_fields": {
            "report_date": report_date,
            "comparison_mode": "confidence_threshold_delta",
            "threshold_field": "auto_accept_min",
        },
    }


def _false_high_recommendation(
    *,
    report_date: str,
    report_type: str,
    review_volume: int,
    evidence_count: int,
    action_context: Mapping[str, Any],
) -> dict[str, Any] | None:
    try:
        policy = get_report_policy(report_type)
    except ValueError:
        return None

    share = _share(evidence_count, review_volume)
    step = _threshold_step(evidence_count, share)
    current = round(policy.confidence_thresholds.auto_accept_min, 2)
    proposed = round(min(0.99, current + step), 2)
    if proposed <= current:
        return None

    confidence = _recommendation_confidence(evidence_count, share, action_context)
    impact_score = _impact_score(evidence_count, share, action_context)
    return {
        "recommendation_id": f"{policy.report_type}__auto_accept_min__increase",
        "target_area": f"report_policy.{policy.report_type}.confidence_thresholds.auto_accept_min",
        "current_threshold": current,
        "proposed_threshold": proposed,
        "reason": "Multiple reviewed items appeared too acceptable initially but were later rejected or conflict-blocked.",
        "evidence": {
            "report_type": policy.report_type,
            "review_volume": review_volume,
            "false_high_confidence_count": evidence_count,
            "false_high_confidence_share": share,
            "action_context": dict(action_context),
        },
        "confidence": confidence,
        "impact_score": impact_score,
        "priority": _priority_from_impact(impact_score),
        "requires_human_approval": True,
        "simulation_fields": {
            "report_date": report_date,
            "comparison_mode": "confidence_threshold_delta",
            "threshold_field": "auto_accept_min",
        },
    }


def _analysis_window(
    review_summary: Mapping[str, Any] | None,
    action_summary: Mapping[str, Any] | None,
    report_date: str,
) -> dict[str, Any]:
    start_candidates = [
        _window_start(review_summary),
        _window_start(action_summary),
    ]
    starts = [value for value in start_candidates if value is not None]
    return {
        "start_date": min(starts) if starts else report_date,
        "end_date": report_date,
    }


def _window_start(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    analysis_window = payload.get("analysis_window")
    if isinstance(analysis_window, Mapping):
        value = analysis_window.get("start_date")
        if isinstance(value, str) and value.strip():
            return value.strip()
    analysis_scope = payload.get("analysis_scope")
    if isinstance(analysis_scope, Mapping):
        value = analysis_scope.get("start_date")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _review_volume_by_report_type(review_summary: Mapping[str, Any]) -> dict[str, int]:
    volume_rows = review_summary.get("review_volume_by_report_type")
    output: dict[str, int] = {}
    if not isinstance(volume_rows, list):
        return output
    for row in volume_rows:
        if not isinstance(row, Mapping):
            continue
        report_type = _canonical_report_type(row.get("report_type"))
        count = _int_or_zero(row.get("count"))
        if report_type is not None:
            output[report_type] = count
    return output


def _candidate_counts(review_summary: Mapping[str, Any], bucket_name: str) -> dict[str, int]:
    confidence = review_summary.get("confidence_accuracy_analysis")
    if not isinstance(confidence, Mapping):
        return {}
    bucket = confidence.get(bucket_name)
    if not isinstance(bucket, Mapping):
        return {}
    items = bucket.get("items")
    counts: Counter[str] = Counter()
    if not isinstance(items, list):
        return {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        report_type = _canonical_report_type(item.get("report_type"))
        if report_type is not None:
            counts[report_type] += 1
    return dict(counts)


def _action_context(action_summary: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(action_summary, Mapping):
        return {
            "available": False,
            "dismissed_rate": 0.0,
            "stale_pending_rate": 0.0,
            "noisy_rule_count": 0,
        }

    summary = action_summary.get("summary")
    noisy_rules = action_summary.get("noisy_rules_candidates")
    return {
        "available": True,
        "dismissed_rate": _float_or_zero(summary.get("dismissed_rate")) if isinstance(summary, Mapping) else 0.0,
        "stale_pending_rate": _float_or_zero(summary.get("stale_pending_rate")) if isinstance(summary, Mapping) else 0.0,
        "noisy_rule_count": len(noisy_rules) if isinstance(noisy_rules, list) else 0,
    }


def _impact_score(evidence_count: int, share: float, action_context: Mapping[str, Any]) -> float:
    score = 0.23 + min(evidence_count, 5) * 0.07 + share * 0.35
    score += _float_or_zero(action_context.get("dismissed_rate")) * 0.1
    score += _float_or_zero(action_context.get("stale_pending_rate")) * 0.1
    if _int_or_zero(action_context.get("noisy_rule_count")) > 0:
        score += 0.03
    return round(min(score, 1.0), 2)


def _recommendation_confidence(evidence_count: int, share: float, action_context: Mapping[str, Any]) -> float:
    confidence = 0.3 + min(evidence_count, 5) * 0.1 + share * 0.4
    if action_context.get("available") is True:
        confidence += 0.05
    return round(min(confidence, 0.95), 2)


def _priority_from_impact(impact_score: float) -> str:
    if impact_score >= 0.75:
        return "high"
    if impact_score >= 0.5:
        return "medium"
    return "low"


def _priority_rank(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(priority, 3)


def _threshold_step(evidence_count: int, share: float) -> float:
    if evidence_count >= 5 or share >= 0.6:
        return 0.05
    if evidence_count >= 3 or share >= 0.55:
        return 0.03
    return 0.02


def _share(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total, 4)


def _canonical_report_type(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return get_report_policy(value.strip()).report_type
    except ValueError:
        return None


def _load_latest_learning_summary(
    category: str,
    report_date: str,
    output_root: str | Path | None,
) -> dict[str, Any] | None:
    category_dir = _learning_root(output_root) / category
    if not category_dir.exists():
        return None

    candidate_paths = sorted(
        (
            path
            for path in category_dir.glob("*.json")
            if path.is_file() and path.stem <= report_date
        ),
        key=lambda path: path.stem,
    )
    if not candidate_paths:
        return None
    path = candidate_paths[-1]
    payload = _read_json(path)
    if payload is None:
        return None
    return {
        "path": str(path),
        "payload": payload,
    }


def _learning_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return Path("records") / "learning"
    return Path(output_root) / "records" / "learning"


def _normalize_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")


def _int_or_zero(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _float_or_zero(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _path_or_none(value: object) -> Path | None:
    if isinstance(value, (str, Path)):
        return Path(value)
    return None


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _failure_result(message: str) -> AgentResult:
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "report_date": None,
            "output_path": None,
            "status": "invalid_input",
            "warnings": [
                {
                    "code": "missing_fields",
                    "severity": "error",
                    "message": message,
                }
            ],
        },
    )
