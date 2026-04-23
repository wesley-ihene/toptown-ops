"""Deterministic action-effectiveness summaries."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
import json
from pathlib import Path
from typing import Any

from packages.learning_store import write_action_effectiveness
import packages.record_store.paths as record_paths
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "action_effectiveness_engine"
SIGNAL_TYPE = "action_effectiveness"
NON_TERMINAL_STATUSES = {"pending", "acknowledged", "in_progress"}


@dataclass(slots=True)
class ActionEffectivenessEngineWorker:
    """Generate one daily action-effectiveness summary artifact."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    report_date = payload.get("report_date") or payload.get("date")
    output_root = payload.get("root") or payload.get("output_root")
    generated_at = payload.get("generated_at")
    now_utc = payload.get("now_utc")
    window_days = payload.get("window_days", 7)

    if not isinstance(report_date, str) or not report_date.strip():
        return _failure_result("report_date is required for action effectiveness analysis.")
    if not isinstance(window_days, int) or isinstance(window_days, bool) or window_days <= 0:
        return _failure_result("window_days must be a positive integer.")

    result = analyze_action_effectiveness(
        report_date.strip(),
        window_days=window_days,
        output_root=_path_or_none(output_root),
        generated_at=generated_at if isinstance(generated_at, str) else None,
        now_utc=now_utc if isinstance(now_utc, str) else None,
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "report_date": report_date.strip(),
            "status": "written",
            "output_path": result["output_path"],
            "action_effectiveness": result["summary"],
        },
    )


def analyze_action_effectiveness(
    report_date: str,
    *,
    window_days: int = 7,
    output_root: str | Path | None = None,
    generated_at: str | None = None,
    now_utc: str | None = None,
) -> dict[str, Any]:
    """Analyze recent actions and feedback artifacts, then persist one summary."""

    normalized_report_date = _normalize_date(report_date)
    normalized_window_days = _normalized_window_days(window_days)
    start_date = _window_start_date(normalized_report_date, normalized_window_days)
    reference_time = _reference_time(normalized_report_date, now_utc)

    feedback_by_key = _load_feedback_by_key(
        start_date=start_date,
        end_date=normalized_report_date,
        output_root=output_root,
    )
    actions = _load_actions(
        start_date=start_date,
        end_date=normalized_report_date,
        output_root=output_root,
        feedback_by_key=feedback_by_key,
        reference_time=reference_time,
    )
    summary_payload = _build_summary_payload(
        report_date=normalized_report_date,
        start_date=start_date,
        window_days=normalized_window_days,
        actions=actions,
    )
    output_path = write_action_effectiveness(
        normalized_report_date,
        summary_payload,
        generated_at=generated_at or f"{normalized_report_date}T00:00:00Z",
        source_paths=_source_paths(actions),
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
    actions: list[dict[str, Any]],
) -> dict[str, Any]:
    by_rule: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_branch: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_priority: dict[str, list[dict[str, Any]]] = defaultdict(list)
    stale_actions = [action for action in actions if action["stale_pending"]]

    for action in actions:
        by_rule[action["rule_code"]].append(action)
        by_branch[action["branch"]].append(action)
        by_priority[action["priority"]].append(action)

    action_effectiveness_by_rule = [
        {
            "rule_code": rule_code,
            **_metrics_payload(grouped_actions),
            "branches": sorted({action["branch"] for action in grouped_actions}),
            "priorities": sorted({action["priority"] for action in grouped_actions}),
        }
        for rule_code, grouped_actions in _sorted_groups(by_rule)
    ]
    action_response_by_branch = [
        {
            "branch": branch,
            **_metrics_payload(grouped_actions),
            "rule_counts": _counter_rows("rule_code", Counter(action["rule_code"] for action in grouped_actions)),
            "priority_counts": _counter_rows("priority", Counter(action["priority"] for action in grouped_actions)),
        }
        for branch, grouped_actions in _sorted_groups(by_branch)
    ]
    action_effectiveness_by_priority = [
        {
            "priority": priority,
            **_metrics_payload(grouped_actions),
        }
        for priority, grouped_actions in _sorted_groups(by_priority)
    ]

    stale_by_rule = Counter(action["rule_code"] for action in stale_actions)
    stale_by_branch = Counter(action["branch"] for action in stale_actions)
    stale_actions_summary = {
        "count": len(stale_actions),
        "by_rule": _counter_rows("rule_code", stale_by_rule),
        "by_branch": _counter_rows("branch", stale_by_branch),
        "items": [
            {
                "action_id": action["action_id"],
                "rule_code": action["rule_code"],
                "branch": action["branch"],
                "priority": action["priority"],
                "report_date": action["report_date"],
                "expires_at": action["expires_at"],
                "effective_status": action["effective_status"],
            }
            for action in sorted(
                stale_actions,
                key=lambda item: (
                    item["report_date"],
                    item["branch"],
                    item["rule_code"],
                    item["action_id"],
                ),
            )
        ],
    }

    noisy_rules_candidates = [
        {
            "rule_code": rule_summary["rule_code"],
            "total_actions": rule_summary["total_actions"],
            "dismissed_rate": rule_summary["dismissed_rate"],
            "dismissed_actions": rule_summary["dismissed_actions"],
            "stale_pending_rate": rule_summary["stale_pending_rate"],
            "stale_pending_actions": rule_summary["stale_pending_actions"],
            "branches": rule_summary["branches"],
            "priorities": rule_summary["priorities"],
        }
        for rule_summary in action_effectiveness_by_rule
        if rule_summary["total_actions"] >= 2
        and (
            rule_summary["dismissed_rate"] >= 0.5
            or rule_summary["stale_pending_rate"] >= 0.5
        )
    ]

    return {
        "artifact_type": SIGNAL_TYPE,
        "report_date": report_date,
        "total_actions": len(actions),
        "summary": _metrics_payload(actions),
        "action_effectiveness_by_rule": action_effectiveness_by_rule,
        "action_response_by_branch": action_response_by_branch,
        "action_effectiveness_by_priority": action_effectiveness_by_priority,
        "stale_actions_summary": stale_actions_summary,
        "noisy_rules_candidates": noisy_rules_candidates,
        "analysis_scope": {
            "start_date": start_date,
            "end_date": report_date,
            "window_days": window_days,
        },
    }


def _load_actions(
    *,
    start_date: str,
    end_date: str,
    output_root: str | Path | None,
    feedback_by_key: dict[tuple[str, str, str], dict[str, Any]],
    reference_time: datetime,
) -> list[dict[str, Any]]:
    actions_root = _actions_root(output_root)
    if not actions_root.exists():
        return []

    loaded: list[dict[str, Any]] = []
    for path in sorted(actions_root.rglob("*.json")):
        payload = _read_json(path)
        if payload is None:
            continue
        action = _normalized_action(
            path=path,
            payload=payload,
            feedback_by_key=feedback_by_key,
            reference_time=reference_time,
        )
        if action is None:
            continue
        if not (start_date <= action["report_date"] <= end_date):
            continue
        loaded.append(action)
    return loaded


def _normalized_action(
    *,
    path: Path,
    payload: Mapping[str, Any],
    feedback_by_key: Mapping[tuple[str, str, str], dict[str, Any]],
    reference_time: datetime,
) -> dict[str, Any] | None:
    action_id = _string_or_none(payload.get("action_id"))
    report_date = _string_or_none(payload.get("report_date"))
    branch = _string_or_none(payload.get("branch"))
    if action_id is None or report_date is None or branch is None:
        return None

    try:
        normalized_report_date = _normalize_date(report_date)
    except ValueError:
        return None

    rule_code = _string_or_none(payload.get("rule_code")) or _string_or_none(payload.get("action_type")) or "unknown"
    priority = _string_or_none(payload.get("priority")) or "unknown"
    feedback = feedback_by_key.get((normalized_report_date, branch, action_id))
    effective_status = _effective_status(payload, feedback)
    action_timestamp = _first_timestamp(payload.get("created_at"), payload.get("generated_at"), payload.get("issued_at"))
    first_response_timestamp = _first_feedback_timestamp(feedback)

    return {
        "action_id": action_id,
        "report_date": normalized_report_date,
        "branch": branch,
        "rule_code": rule_code,
        "priority": priority,
        "effective_status": effective_status,
        "stale_pending": _is_stale_pending(effective_status, _string_or_none(payload.get("expires_at")), reference_time),
        "response_delay_seconds": _response_delay_seconds(action_timestamp, first_response_timestamp),
        "action_path": str(path),
        "feedback_path": feedback.get("path") if isinstance(feedback, Mapping) else None,
        "expires_at": _string_or_none(payload.get("expires_at")),
    }


def _load_feedback_by_key(
    *,
    start_date: str,
    end_date: str,
    output_root: str | Path | None,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    feedback_root = _feedback_root(output_root)
    if not feedback_root.exists():
        return {}

    loaded: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in sorted(feedback_root.rglob("*.json")):
        payload = _read_json(path)
        if payload is None:
            continue
        item = _normalized_feedback(path, payload)
        if item is None:
            continue
        if not (start_date <= item["report_date"] <= end_date):
            continue
        loaded[(item["report_date"], item["branch"], item["action_id"])] = item
    return loaded


def _normalized_feedback(path: Path, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    action_id = _string_or_none(payload.get("action_id"))
    branch = _string_or_none(payload.get("branch"))
    report_date = _string_or_none(payload.get("report_date"))
    if action_id is None or branch is None or report_date is None:
        return None

    try:
        normalized_report_date = _normalize_date(report_date)
    except ValueError:
        return None

    history_payload = payload.get("history")
    history: list[dict[str, Any]] = []
    if isinstance(history_payload, list):
        for entry in history_payload:
            if isinstance(entry, Mapping):
                history.append(dict(entry))

    return {
        "path": str(path),
        "action_id": action_id,
        "branch": branch,
        "report_date": normalized_report_date,
        "status": _string_or_none(payload.get("status")),
        "acknowledged_at": _string_or_none(payload.get("acknowledged_at")),
        "history": history,
    }


def _metrics_payload(actions: list[dict[str, Any]]) -> dict[str, Any]:
    total_actions = len(actions)
    acknowledged_actions = sum(1 for action in actions if action["effective_status"] != "pending")
    resolved_actions = sum(1 for action in actions if action["effective_status"] == "resolved")
    dismissed_actions = sum(1 for action in actions if action["effective_status"] == "dismissed")
    stale_pending_actions = sum(1 for action in actions if action["stale_pending"])
    delays = [delay for delay in (action["response_delay_seconds"] for action in actions) if delay is not None]

    return {
        "total_actions": total_actions,
        "acknowledged_actions": acknowledged_actions,
        "resolved_actions": resolved_actions,
        "dismissed_actions": dismissed_actions,
        "stale_pending_actions": stale_pending_actions,
        "acknowledged_rate": _rate(acknowledged_actions, total_actions),
        "resolved_rate": _rate(resolved_actions, total_actions),
        "dismissed_rate": _rate(dismissed_actions, total_actions),
        "stale_pending_rate": _rate(stale_pending_actions, total_actions),
        "average_response_delay": round(sum(delays) / len(delays), 2) if delays else None,
        "response_delay_sample_count": len(delays),
    }


def _effective_status(action_payload: Mapping[str, Any], feedback: Mapping[str, Any] | None) -> str:
    if isinstance(feedback, Mapping):
        status = _string_or_none(feedback.get("status"))
        if status is not None:
            return status
    return _string_or_none(action_payload.get("status")) or "pending"


def _first_feedback_timestamp(feedback: Mapping[str, Any] | None) -> datetime | None:
    if not isinstance(feedback, Mapping):
        return None

    candidates: list[datetime] = []
    direct_ack = _parse_timestamp(feedback.get("acknowledged_at"))
    if direct_ack is not None:
        candidates.append(direct_ack)

    history = feedback.get("history")
    if isinstance(history, list):
        for entry in history:
            if not isinstance(entry, Mapping):
                continue
            parsed = _parse_timestamp(entry.get("acknowledged_at"))
            if parsed is not None:
                candidates.append(parsed)
    if not candidates:
        return None
    return min(candidates)


def _response_delay_seconds(
    action_timestamp: datetime | None,
    first_response_timestamp: datetime | None,
) -> float | None:
    if action_timestamp is None or first_response_timestamp is None:
        return None
    delay = (first_response_timestamp - action_timestamp).total_seconds()
    if delay < 0:
        return None
    return round(delay, 2)


def _is_stale_pending(effective_status: str, expires_at: str | None, reference_time: datetime) -> bool:
    if effective_status not in NON_TERMINAL_STATUSES:
        return False
    expiry = _parse_timestamp(expires_at)
    if expiry is None:
        return False
    return expiry < reference_time


def _source_paths(actions: list[dict[str, Any]]) -> list[str]:
    paths = {
        action["action_path"]
        for action in actions
        if isinstance(action.get("action_path"), str) and action["action_path"].strip()
    }
    paths.update(
        action["feedback_path"]
        for action in actions
        if isinstance(action.get("feedback_path"), str) and action["feedback_path"] and action["feedback_path"].strip()
    )
    return sorted(paths)


def _counter_rows(field_name: str, counts: Counter[str]) -> list[dict[str, Any]]:
    return [
        {
            field_name: key,
            "count": count,
        }
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _sorted_groups(groups: Mapping[str, list[dict[str, Any]]]) -> list[tuple[str, list[dict[str, Any]]]]:
    return sorted(
        groups.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )


def _actions_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.ACTIONS_DIR
    return Path(output_root) / "records" / "actions"


def _feedback_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.FEEDBACK_DIR
    return Path(output_root) / "records" / "feedback"


def _normalize_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")


def _window_start_date(report_date: str, window_days: int) -> str:
    anchor = datetime.strptime(report_date, "%Y-%m-%d").date()
    return (anchor - timedelta(days=window_days - 1)).strftime("%Y-%m-%d")


def _normalized_window_days(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("window_days must be a positive integer")
    return value


def _reference_time(report_date: str, now_utc: str | None) -> datetime:
    if isinstance(now_utc, str) and now_utc.strip():
        parsed = _parse_timestamp(now_utc)
        if parsed is not None:
            return parsed
    anchor = datetime.strptime(report_date, "%Y-%m-%d").date()
    return datetime.combine(anchor, time(23, 59, 59), tzinfo=timezone.utc)


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _first_timestamp(*values: object) -> datetime | None:
    for value in values:
        parsed = _parse_timestamp(value)
        if parsed is not None:
            return parsed
    return None


def _rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total, 4)


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
