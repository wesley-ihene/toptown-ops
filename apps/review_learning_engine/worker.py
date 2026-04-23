"""Deterministic review-queue learning summaries."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any

from packages.learning_store import write_review_summary
import packages.record_store.paths as record_paths
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "review_learning_engine"
SIGNAL_TYPE = "review_summary"
_CONFIDENCE_REVIEW_REASON = "confidence_between_review_and_accept_thresholds"


@dataclass(slots=True)
class ReviewLearningEngineWorker:
    """Generate one daily review-learning summary artifact."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    report_date = payload.get("report_date") or payload.get("date")
    output_root = payload.get("root") or payload.get("output_root")
    generated_at = payload.get("generated_at")
    window_days = payload.get("window_days", 7)

    if not isinstance(report_date, str) or not report_date.strip():
        return _failure_result("report_date is required for review learning analysis.")

    if not isinstance(window_days, int) or isinstance(window_days, bool) or window_days <= 0:
        return _failure_result("window_days must be a positive integer.")

    result = analyze_review_queue(
        report_date.strip(),
        window_days=window_days,
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
            "review_summary": result["summary"],
        },
    )


def analyze_review_queue(
    report_date: str,
    *,
    window_days: int = 7,
    output_root: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Analyze recent review items and persist one deterministic learning summary."""

    normalized_report_date = _normalize_date(report_date)
    normalized_window_days = _normalized_window_days(window_days)
    start_date = _window_start_date(normalized_report_date, normalized_window_days)
    review_items = _load_review_items(
        start_date=start_date,
        end_date=normalized_report_date,
        output_root=output_root,
    )
    summary_payload = _build_summary_payload(
        report_date=normalized_report_date,
        start_date=start_date,
        window_days=normalized_window_days,
        review_items=review_items,
    )
    output_path = write_review_summary(
        normalized_report_date,
        summary_payload,
        generated_at=generated_at or f"{normalized_report_date}T00:00:00Z",
        source_paths=[item["path"] for item in review_items],
        analysis_window={
            "start_date": start_date,
            "end_date": normalized_report_date,
            "window_days": normalized_window_days,
        },
        output_root=output_root,
    )
    return {
        "status": "written",
        "output_path": output_path,
        "summary": json.loads(Path(output_path).read_text(encoding="utf-8")),
    }


def _build_summary_payload(
    *,
    report_date: str,
    start_date: str,
    window_days: int,
    review_items: list[dict[str, Any]],
) -> dict[str, Any]:
    by_report_type = Counter(item["report_type"] for item in review_items)
    by_branch = Counter(item["branch"] for item in review_items)
    by_reason = Counter(item["reason"] for item in review_items)
    by_date = Counter(item["date"] for item in review_items)

    branch_report_type_counts: dict[str, Counter[str]] = defaultdict(Counter)
    branch_reason_counts: dict[str, Counter[str]] = defaultdict(Counter)
    reason_report_types: dict[str, set[str]] = defaultdict(set)
    reason_branches: dict[str, set[str]] = defaultdict(set)
    reason_dates: dict[str, set[str]] = defaultdict(set)

    for item in review_items:
        branch_report_type_counts[item["branch"]][item["report_type"]] += 1
        branch_reason_counts[item["branch"]][item["reason"]] += 1
        reason_report_types[item["reason"]].add(item["report_type"])
        reason_branches[item["reason"]].add(item["branch"])
        reason_dates[item["reason"]].add(item["date"])

    review_reasons_summary = [
        {
            "reason": reason,
            "count": count,
            "report_types": sorted(reason_report_types[reason]),
            "branches": sorted(reason_branches[reason]),
            "dates": sorted(reason_dates[reason]),
        }
        for reason, count in _sorted_counter_items(by_reason)
    ]
    recurring_review_causes = [item for item in review_reasons_summary if item["count"] >= 2]

    accepted_after_review = sum(1 for item in review_items if item["review_outcome"] == "accepted_after_review")
    rejected_after_review = sum(1 for item in review_items if item["review_outcome"] == "rejected_after_review")
    still_pending = sum(1 for item in review_items if item["review_outcome"] == "still_pending")

    false_low_confidence_items = [
        _confidence_candidate_row(item)
        for item in review_items
        if item["reason"] == _CONFIDENCE_REVIEW_REASON and item["review_outcome"] == "accepted_after_review"
    ]
    false_high_confidence_items = [
        _confidence_candidate_row(item)
        for item in review_items
        if item["confidence"] is not None and item["review_outcome"] == "rejected_after_review"
    ]

    return {
        "artifact_type": SIGNAL_TYPE,
        "report_date": report_date,
        "total_review_items": len(review_items),
        "grouped_review_counts": {
            "by_report_type": _counter_rows("report_type", by_report_type),
            "by_branch": _counter_rows("branch", by_branch),
            "by_reason": _counter_rows("reason", by_reason),
            "by_date": _counter_rows("date", by_date),
        },
        "review_volume_by_report_type": _counter_rows("report_type", by_report_type),
        "review_reasons_summary": review_reasons_summary,
        "branch_review_heatmap": [
            {
                "branch": branch,
                "total_reviews": by_branch[branch],
                "by_report_type": _counter_rows("report_type", branch_report_type_counts[branch]),
                "by_reason": _counter_rows("reason", branch_reason_counts[branch]),
            }
            for branch in sorted(branch_report_type_counts)
        ],
        "recurring_review_causes": recurring_review_causes,
        "review_outcomes": {
            "accepted_after_review": accepted_after_review,
            "rejected_after_review": rejected_after_review,
            "still_pending": still_pending,
        },
        "confidence_accuracy_analysis": {
            "false_low_confidence_candidates": {
                "count": len(false_low_confidence_items),
                "items": false_low_confidence_items,
            },
            "false_high_confidence_candidates": {
                "count": len(false_high_confidence_items),
                "items": false_high_confidence_items,
            },
        },
        "analysis_scope": {
            "start_date": start_date,
            "end_date": report_date,
            "window_days": window_days,
        },
    }


def _load_review_items(
    *,
    start_date: str,
    end_date: str,
    output_root: str | Path | None,
) -> list[dict[str, Any]]:
    review_root = _review_root(output_root)
    if not review_root.exists():
        return []

    loaded: list[dict[str, Any]] = []
    for path in sorted(review_root.rglob("*.json")):
        payload = _read_json(path)
        if payload is None:
            continue
        item = _normalized_review_item(path, payload)
        if item is None:
            continue
        if not (start_date <= item["date"] <= end_date):
            continue
        loaded.append(item)
    return loaded


def _normalized_review_item(path: Path, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    item_date = _string_or_none(payload.get("date"))
    if item_date is None:
        return None

    try:
        normalized_date = _normalize_date(item_date)
    except ValueError:
        return None

    report_type = _string_or_none(payload.get("report_type")) or "unknown"
    branch = _string_or_none(payload.get("branch")) or "unknown"
    reason = _primary_reason(payload)
    confidence = _float_or_none(payload.get("confidence"))

    return {
        "path": str(path),
        "date": normalized_date,
        "report_type": report_type,
        "branch": branch,
        "reason": reason,
        "confidence": confidence,
        "review_outcome": _review_outcome(payload),
    }


def _primary_reason(payload: Mapping[str, Any]) -> str:
    reason = _string_or_none(payload.get("reason"))
    if reason is not None:
        return reason

    governance = payload.get("governance")
    if isinstance(governance, Mapping):
        reasons = governance.get("reasons")
        if isinstance(reasons, list):
            for item in reasons:
                cleaned = _string_or_none(item)
                if cleaned is not None:
                    return cleaned

    acceptance = payload.get("acceptance")
    if isinstance(acceptance, Mapping):
        reason = _string_or_none(acceptance.get("reason"))
        if reason is not None:
            return reason

    return "unknown"


def _review_outcome(payload: Mapping[str, Any]) -> str:
    resolution_status = _string_or_none(payload.get("resolution_status"))
    acceptance = payload.get("acceptance")
    acceptance_status = None
    if isinstance(acceptance, Mapping):
        acceptance_status = _string_or_none(acceptance.get("decision")) or _string_or_none(acceptance.get("status"))

    if acceptance_status == "accept" or resolution_status in {"accepted", "accepted_after_review"}:
        return "accepted_after_review"
    if acceptance_status == "reject" or resolution_status in {"rejected", "rejected_after_review"}:
        return "rejected_after_review"
    return "still_pending"


def _counter_rows(field_name: str, counts: Counter[str]) -> list[dict[str, Any]]:
    return [
        {
            field_name: key,
            "count": count,
        }
        for key, count in _sorted_counter_items(counts)
    ]


def _sorted_counter_items(counts: Counter[str]) -> list[tuple[str, int]]:
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def _confidence_candidate_row(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "path": str(item["path"]),
        "date": str(item["date"]),
        "report_type": str(item["report_type"]),
        "branch": str(item["branch"]),
        "reason": str(item["reason"]),
        "confidence": item["confidence"],
    }


def _review_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.REVIEW_DIR
    return Path(output_root) / "records" / "review"


def _normalize_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")


def _window_start_date(report_date: str, window_days: int) -> str:
    anchor = datetime.strptime(report_date, "%Y-%m-%d").date()
    return (anchor - timedelta(days=window_days - 1)).strftime("%Y-%m-%d")


def _normalized_window_days(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("window_days must be a positive integer")
    return value


def _float_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


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
