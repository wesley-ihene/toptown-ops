"""Append-only file-backed operator feedback helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.record_store.naming import safe_segment
from packages.record_store.paths import get_action_preview_path, get_feedback_path
from packages.record_store.writer import write_json_file

VALID_FEEDBACK_STATUSES = {"acknowledged", "in_progress", "resolved", "dismissed"}
NON_TERMINAL_ACTION_STATUSES = {"pending", "acknowledged", "in_progress"}
VERSION = "v1"


def record_action_feedback(
    *,
    action_id: str,
    branch: str,
    report_date: str,
    status: str,
    acknowledged_by: str,
    acknowledged_at: str | None = None,
    resolution_note: str | None = None,
    evidence_paths: list[str] | None = None,
    source_action_path: str,
    linked_review_queue_path: str | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Append one operator feedback transition for an action."""

    normalized_action_id = _required_text(action_id, field_name="action_id")
    normalized_branch = _required_text(branch, field_name="branch")
    normalized_date = _required_text(report_date, field_name="report_date")
    normalized_status = _normalized_status(status)
    normalized_by = _required_text(acknowledged_by, field_name="acknowledged_by")
    normalized_action_path = _required_text(source_action_path, field_name="source_action_path")
    normalized_note = _optional_text(resolution_note)
    normalized_review_path = _optional_text(linked_review_queue_path)
    normalized_evidence_paths = _string_list(evidence_paths)

    feedback_path = get_feedback_path(
        normalized_date,
        normalized_branch,
        normalized_action_id,
        output_root=output_root,
    )
    existing = read_action_feedback(
        normalized_date,
        normalized_branch,
        normalized_action_id,
        output_root=output_root,
    )
    history = list(existing.get("history", [])) if isinstance(existing, Mapping) else []
    current_status = _optional_text(existing.get("status")) if isinstance(existing, Mapping) else "pending"

    idempotent_history = _matching_history_entry(
        history,
        status=normalized_status,
        acknowledged_by=normalized_by,
        resolution_note=normalized_note,
        evidence_paths=normalized_evidence_paths,
        source_action_path=normalized_action_path,
        linked_review_queue_path=normalized_review_path,
    )
    if idempotent_history is not None:
        payload = dict(existing) if isinstance(existing, Mapping) else _feedback_payload(
            action_id=normalized_action_id,
            branch=normalized_branch,
            report_date=normalized_date,
            feedback_id=idempotent_history["feedback_id"],
            status=normalized_status,
            acknowledged_by=normalized_by,
            acknowledged_at=idempotent_history["acknowledged_at"],
            resolution_note=normalized_note,
            evidence_paths=normalized_evidence_paths,
            source_action_path=normalized_action_path,
            linked_review_queue_path=normalized_review_path,
            history=history,
        )
        return {
            "path": str(feedback_path),
            "payload": payload,
            "transition_status": "unchanged",
        }

    _validate_status_transition(current_status, normalized_status)
    recorded_at = _normalize_timestamp(acknowledged_at)
    feedback_id = _feedback_id(normalized_action_id, len(history) + 1)
    history_entry = {
        "feedback_id": feedback_id,
        "status": normalized_status,
        "acknowledged_by": normalized_by,
        "acknowledged_at": recorded_at,
        "resolution_note": normalized_note,
        "evidence_paths": normalized_evidence_paths,
        "source_action_path": normalized_action_path,
        "linked_review_queue_path": normalized_review_path,
    }
    history.append(history_entry)

    payload = _feedback_payload(
        action_id=normalized_action_id,
        branch=normalized_branch,
        report_date=normalized_date,
        feedback_id=feedback_id,
        status=normalized_status,
        acknowledged_by=normalized_by,
        acknowledged_at=recorded_at,
        resolution_note=normalized_note,
        evidence_paths=normalized_evidence_paths,
        source_action_path=normalized_action_path,
        linked_review_queue_path=normalized_review_path,
        history=history,
    )
    write_json_file(feedback_path, payload)
    return {
        "path": str(feedback_path),
        "payload": payload,
        "transition_status": "recorded",
    }


def read_action_feedback(
    report_date: str,
    branch: str,
    action_id: str,
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Read one operator feedback artifact when present."""

    path = get_feedback_path(report_date, branch, action_id, output_root=output_root)
    return _read_json(path)


def list_feedback_for_date(
    report_date: str,
    *,
    branch: str | None = None,
    output_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """List all valid feedback artifacts for one date, optionally for one branch."""

    feedback_root = _feedback_root(output_root) / report_date
    if not feedback_root.exists():
        return []

    payloads: list[dict[str, Any]] = []
    branch_dirs = [feedback_root / safe_segment(branch)] if branch else sorted(path for path in feedback_root.iterdir() if path.is_dir())
    for branch_dir in branch_dirs:
        if not branch_dir.exists():
            continue
        for path in sorted(branch_dir.glob("*.json")):
            payload = _read_json(path)
            if payload is None:
                continue
            payloads.append(payload)
    return payloads


def build_action_feedback_state(
    report_date: str,
    *,
    branch: str | None = None,
    output_root: str | Path | None = None,
    now_utc: str | None = None,
) -> dict[str, Any]:
    """Build one read-only action and feedback state snapshot for dashboard use."""

    normalized_date = _required_text(report_date, field_name="report_date")
    reference_time = _parse_or_now(now_utc)
    actions = list_actions_for_date(normalized_date, branch=branch, output_root=output_root, now_utc=reference_time)
    pending_actions = [action for action in actions if action["effective_status"] in NON_TERMINAL_ACTION_STATUSES]
    feedback_items = list_feedback_for_date(normalized_date, branch=branch, output_root=output_root)
    return {
        "report_date": normalized_date,
        "branch": _optional_text(branch),
        "summary": {
            "total_actions": len(actions),
            "pending_actions": len(pending_actions),
            "actions_acknowledged": sum(1 for action in actions if action["effective_status"] == "acknowledged"),
            "actions_in_progress": sum(1 for action in actions if action["effective_status"] == "in_progress"),
            "actions_resolved": sum(1 for action in actions if action["effective_status"] == "resolved"),
            "actions_dismissed": sum(1 for action in actions if action["effective_status"] == "dismissed"),
            "review_linked_actions": sum(1 for action in actions if action["linked_review_queue_path"] is not None),
            "stale_pending_actions": sum(1 for action in actions if action["stale_pending"]),
            "feedback_records": len(feedback_items),
            "feedback_history_events": sum(len(item.get("history", [])) for item in feedback_items if isinstance(item.get("history"), list)),
        },
        "pending_actions": pending_actions,
        "actions": actions,
        "feedback_items": feedback_items,
    }


def list_actions_for_date(
    report_date: str,
    *,
    branch: str | None = None,
    output_root: str | Path | None = None,
    now_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    """List actions with derived effective feedback state for one date."""

    action_root = _actions_root(output_root) / report_date
    if not action_root.exists():
        return []

    reference_time = now_utc or datetime.now(timezone.utc)
    action_rows: list[dict[str, Any]] = []
    branch_dirs = [action_root / safe_segment(branch)] if branch else sorted(path for path in action_root.iterdir() if path.is_dir())
    for branch_dir in branch_dirs:
        if not branch_dir.exists():
            continue
        for action_type_dir in sorted(path for path in branch_dir.iterdir() if path.is_dir()):
            for action_path in sorted(action_type_dir.glob("*.json")):
                action_payload = _read_json(action_path)
                if action_payload is None:
                    continue
                action_row = _action_state_row(
                    action_payload=action_payload,
                    action_path=action_path,
                    output_root=output_root,
                    report_date=report_date,
                    fallback_branch=branch_dir.name,
                    now_utc=reference_time,
                )
                if action_row is not None:
                    action_rows.append(action_row)
    return sorted(
        action_rows,
        key=lambda row: (
            _priority_rank(row.get("priority")),
            row.get("branch") or "",
            row.get("action_type") or "",
            row.get("action_id") or "",
        ),
    )


def _action_state_row(
    *,
    action_payload: Mapping[str, Any],
    action_path: Path,
    output_root: str | Path | None,
    report_date: str,
    fallback_branch: str,
    now_utc: datetime,
) -> dict[str, Any] | None:
    action_id = _optional_text(action_payload.get("action_id"))
    if action_id is None:
        return None
    branch = _optional_text(action_payload.get("branch")) or fallback_branch
    payload_report_date = _optional_text(action_payload.get("report_date")) or report_date
    feedback = read_action_feedback(payload_report_date, branch, action_id, output_root=output_root)
    effective_status = _optional_text(feedback.get("status")) if isinstance(feedback, Mapping) else None
    if effective_status is None:
        effective_status = _optional_text(action_payload.get("status")) or "pending"
    linked_review_queue_path = _optional_text(feedback.get("linked_review_queue_path")) if isinstance(feedback, Mapping) else None
    if linked_review_queue_path is None:
        linked_review_queue_path = _find_linked_review_queue_path(
            report_date=payload_report_date,
            branch=branch,
            action_id=action_id,
            output_root=output_root,
        )
    action_type = _optional_text(action_payload.get("action_type")) or _optional_text(action_payload.get("rule_code"))
    preview_path = None
    if action_type is not None:
        preview_path = str(
            get_action_preview_path(
                payload_report_date,
                branch,
                action_type,
                action_id,
                output_root=output_root,
            )
        )
    return {
        "action_id": action_id,
        "action_type": action_type,
        "rule_code": _optional_text(action_payload.get("rule_code")),
        "branch": branch,
        "report_date": payload_report_date,
        "signal_type": _optional_text(action_payload.get("signal_type")),
        "priority": _optional_text(action_payload.get("priority")),
        "severity": _optional_text(action_payload.get("severity")),
        "summary": _optional_text(action_payload.get("summary")),
        "assigned_to": _optional_text(action_payload.get("assigned_to")),
        "requires_ack": action_payload.get("requires_ack") is True,
        "effective_status": effective_status,
        "source_action_path": str(action_path),
        "preview_path": preview_path,
        "feedback_path": str(get_feedback_path(payload_report_date, branch, action_id, output_root=output_root))
        if feedback is not None
        else None,
        "linked_review_queue_path": linked_review_queue_path,
        "feedback_history_count": len(feedback.get("history", [])) if isinstance(feedback, Mapping) and isinstance(feedback.get("history"), list) else 0,
        "feedback_acknowledged_by": _optional_text(feedback.get("acknowledged_by")) if isinstance(feedback, Mapping) else None,
        "feedback_acknowledged_at": _optional_text(feedback.get("acknowledged_at")) if isinstance(feedback, Mapping) else None,
        "expires_at": _optional_text(action_payload.get("expires_at")),
        "delivery_status": _optional_text(action_payload.get("delivery_status")),
        "stale_pending": _is_stale_pending(
            effective_status=effective_status,
            expires_at=_optional_text(action_payload.get("expires_at")),
            now_utc=now_utc,
        ),
    }


def _find_linked_review_queue_path(
    *,
    report_date: str,
    branch: str,
    action_id: str,
    output_root: str | Path | None = None,
) -> str | None:
    review_root = _review_root(output_root) / safe_segment(report_date) / safe_segment(branch)
    if not review_root.exists():
        return None
    for path in sorted(review_root.glob("*/*.json")):
        payload = _read_json(path)
        if payload is None:
            continue
        if _optional_text(payload.get("linked_action_id")) == action_id:
            return str(path)
    return None


def _feedback_payload(
    *,
    action_id: str,
    branch: str,
    report_date: str,
    feedback_id: str,
    status: str,
    acknowledged_by: str,
    acknowledged_at: str,
    resolution_note: str | None,
    evidence_paths: list[str],
    source_action_path: str,
    linked_review_queue_path: str | None,
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "feedback_id": feedback_id,
        "action_id": action_id,
        "branch": branch,
        "report_date": report_date,
        "status": status,
        "acknowledged_by": acknowledged_by,
        "acknowledged_at": acknowledged_at,
        "resolution_note": resolution_note,
        "evidence_paths": evidence_paths,
        "source_action_path": source_action_path,
        "linked_review_queue_path": linked_review_queue_path,
        "version": VERSION,
        "history": history,
    }


def _validate_status_transition(current_status: str | None, next_status: str) -> None:
    if current_status is None or current_status == "pending":
        return
    if current_status == next_status:
        return
    allowed_next = {
        "acknowledged": {"in_progress", "resolved", "dismissed"},
        "in_progress": {"resolved", "dismissed"},
        "resolved": set(),
        "dismissed": set(),
    }
    if next_status not in allowed_next.get(current_status, set()):
        raise ValueError(f"invalid feedback status transition: {current_status} -> {next_status}")


def _matching_history_entry(
    history: list[dict[str, Any]],
    *,
    status: str,
    acknowledged_by: str,
    resolution_note: str | None,
    evidence_paths: list[str],
    source_action_path: str,
    linked_review_queue_path: str | None,
) -> dict[str, Any] | None:
    if not history:
        return None
    latest = history[-1]
    if not isinstance(latest, Mapping):
        return None
    if _optional_text(latest.get("status")) != status:
        return None
    if _optional_text(latest.get("acknowledged_by")) != acknowledged_by:
        return None
    if _optional_text(latest.get("resolution_note")) != resolution_note:
        return None
    if _string_list(latest.get("evidence_paths")) != evidence_paths:
        return None
    if _optional_text(latest.get("source_action_path")) != source_action_path:
        return None
    if _optional_text(latest.get("linked_review_queue_path")) != linked_review_queue_path:
        return None
    return dict(latest)


def _normalize_timestamp(value: str | None) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalized_status(value: str) -> str:
    cleaned = _required_text(value, field_name="status")
    if cleaned not in VALID_FEEDBACK_STATUSES:
        raise ValueError(f"unsupported feedback status: {cleaned}")
    return cleaned


def _is_stale_pending(*, effective_status: str, expires_at: str | None, now_utc: datetime) -> bool:
    if effective_status not in NON_TERMINAL_ACTION_STATUSES:
        return False
    if expires_at is None:
        return False
    try:
        expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    return expiry < now_utc


def _priority_rank(value: object) -> int:
    if value == "high":
        return 0
    if value == "medium":
        return 1
    if value == "low":
        return 2
    return 3


def _feedback_root(output_root: str | Path | None) -> Path:
    return record_paths.FEEDBACK_DIR if output_root is None else Path(output_root) / "records" / "feedback"


def _actions_root(output_root: str | Path | None) -> Path:
    return record_paths.ACTIONS_DIR if output_root is None else Path(output_root) / "records" / "actions"


def _review_root(output_root: str | Path | None) -> Path:
    return record_paths.REVIEW_DIR if output_root is None else Path(output_root) / "records" / "review"


def _feedback_id(action_id: str, position: int) -> str:
    return f"{safe_segment(action_id)}_{position:03d}"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_or_now(value: str | None) -> datetime:
    if isinstance(value, str) and value.strip():
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return datetime.now(timezone.utc)


def _required_text(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"feedback field `{field_name}` must be a non-empty string")


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]
