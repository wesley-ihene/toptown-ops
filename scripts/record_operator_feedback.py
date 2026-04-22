"""Manual CLI for recording operator feedback against one action artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from packages.feedback_store import list_actions_for_date, record_action_feedback
from packages.observability import refresh_feedback_summary


def main(argv: list[str] | None = None) -> int:
    """Record one feedback transition without mutating the source action."""

    parser = argparse.ArgumentParser(description="Record operator feedback for one action artifact.")
    parser.add_argument("--action-path", required=True, help="Path to one action JSON artifact under records/actions.")
    parser.add_argument(
        "--status",
        required=True,
        choices=["acknowledged", "in_progress", "resolved", "dismissed"],
        help="Feedback lifecycle status to record.",
    )
    parser.add_argument("--by", required=True, help="Operator name or identifier.")
    parser.add_argument("--note", default=None, help="Optional resolution or handoff note.")
    parser.add_argument("--evidence", action="append", default=[], help="Optional evidence path. Repeatable.")
    args = parser.parse_args(argv)

    action_path = Path(args.action_path).expanduser().resolve()
    if not action_path.exists():
        raise FileNotFoundError(f"action artifact not found: {action_path}")

    payload = json.loads(action_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"action artifact must contain a JSON object: {action_path}")

    output_root = _derive_output_root(action_path)
    action_id = _required_text(payload.get("action_id"), field_name="action_id")
    branch = _required_text(payload.get("branch"), field_name="branch")
    report_date = _required_text(payload.get("report_date"), field_name="report_date")
    linked_review_queue_path = _linked_review_queue_path(
        report_date=report_date,
        branch=branch,
        action_id=action_id,
        output_root=output_root,
    )

    result = record_action_feedback(
        action_id=action_id,
        branch=branch,
        report_date=report_date,
        status=args.status,
        acknowledged_by=args.by,
        resolution_note=args.note,
        evidence_paths=list(args.evidence),
        source_action_path=str(action_path),
        linked_review_queue_path=linked_review_queue_path,
        output_root=output_root,
    )
    refresh_feedback_summary(report_date=report_date, branch=branch, output_root=output_root)
    feedback_payload = result["payload"]
    print(f"feedback_path={result['path']}")
    print(
        "summary="
        + json.dumps(
            {
                "action_id": feedback_payload["action_id"],
                "branch": feedback_payload["branch"],
                "report_date": feedback_payload["report_date"],
                "status": feedback_payload["status"],
                "acknowledged_by": feedback_payload["acknowledged_by"],
                "history_count": len(feedback_payload.get("history", [])),
                "linked_review_queue_path": feedback_payload.get("linked_review_queue_path"),
            },
            sort_keys=True,
        )
    )
    return 0


def _linked_review_queue_path(
    *,
    report_date: str,
    branch: str,
    action_id: str,
    output_root: Path,
) -> str | None:
    actions = list_actions_for_date(report_date, branch=branch, output_root=output_root)
    for action in actions:
        if action.get("action_id") == action_id:
            linked = action.get("linked_review_queue_path")
            if isinstance(linked, str) and linked.strip():
                return linked.strip()
            return None
    return None


def _derive_output_root(action_path: Path) -> Path:
    parts = list(action_path.parts)
    try:
        records_index = parts.index("records")
    except ValueError as exc:
        raise ValueError(f"action path must live under records/actions: {action_path}") from exc
    if len(parts) <= records_index + 2 or parts[records_index + 1] != "actions":
        raise ValueError(f"action path must live under records/actions: {action_path}")
    return Path(*parts[:records_index]) if records_index > 0 else Path("/")


def _required_text(value: Any, *, field_name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"action field `{field_name}` must be a non-empty string")


if __name__ == "__main__":
    raise SystemExit(main())
