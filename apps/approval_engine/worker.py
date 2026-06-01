"""Supervisor approval, rejection, and replay workflow for review items."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import io
import json
from pathlib import Path
from typing import Any

import apps.hr_agent.record_store as hr_record_store
import apps.pricing_stock_release_agent.record_store as pricing_record_store
import apps.sales_income_agent.record_store as sales_record_store
import apps.supervisor_control_agent.record_store as supervisor_record_store
from apps.supervisor_auth.worker import authorize_supervisor
from packages.branch_registry import canonical_branch_slug_or_none
import packages.record_store.paths as record_paths
from packages.record_store.naming import build_rejected_filename, safe_segment
from packages.record_store.paths import get_rejected_path
from packages.record_store.writer import write_json_file, write_text_file
from scripts import replay_records

_OPEN_RESOLUTION_STATUSES = {"open", "pending", "needs_review", None}
_ACCEPTED_STATUSES = {"accepted", "accepted_with_warning"}
_REPORT_TYPE_TO_WRITER = {
    "sales": sales_record_store.write_structured_record,
    "staff_attendance": hr_record_store.write_structured_record,
    "hr_attendance": hr_record_store.write_structured_record,
    "staff_performance": hr_record_store.write_structured_record,
    "hr_performance": hr_record_store.write_structured_record,
    "bale_summary": pricing_record_store.write_structured_record,
    "supervisor_control": supervisor_record_store.write_structured_record,
}


def process_supervisor_action(
    command: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Execute one authorized supervisor action against a review item."""

    action = _required_text(command.get("command_name"), field_name="command_name")
    record_id = _required_text(
        command.get("record_id") or command.get("command_argument"),
        field_name="record_id",
    )
    sender_phone = _text_or_none(command.get("sender_phone"))
    timestamp = _utc_timestamp()
    root = Path(output_root) if output_root is not None else record_paths.RECORDS_DIR.parent

    review_resolution = _resolve_review_item(record_id=record_id, output_root=root)
    if review_resolution["status"] != "resolved":
        audit = _write_audit_log(
            action=action,
            record_id=record_id,
            sender_phone=sender_phone,
            branch=None,
            authorized=False,
            status="failed",
            reason=review_resolution["reason"],
            review_queue_path=None,
            output_root=root,
            occurred_at=timestamp,
        )
        return _result(
            action=action,
            record_id=record_id,
            status="failed",
            reason=review_resolution["reason"],
            message="review item not found",
            audit_path=str(audit),
        )

    review_path = review_resolution["path"]
    review_payload = review_resolution["payload"]
    branch = _text_or_none(review_payload.get("branch"))
    report_type = _text_or_none(review_payload.get("report_type"))
    report_date = _text_or_none(review_payload.get("date"))
    authorization = authorize_supervisor(sender_phone=sender_phone, branch=branch)
    if authorization["authorized"] is not True:
        audit = _write_audit_log(
            action=action,
            record_id=record_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=False,
            status="unauthorized",
            reason=str(authorization["reason"]),
            review_queue_path=str(review_path),
            output_root=root,
            occurred_at=timestamp,
        )
        return _result(
            action=action,
            record_id=record_id,
            status="unauthorized",
            branch=branch,
            report_type=report_type,
            report_date=report_date,
            reason=str(authorization["reason"]),
            message="sender is not authorized for this branch",
            audit_path=str(audit),
        )

    resolution_status = _text_or_none(review_payload.get("resolution_status"))
    if resolution_status not in _OPEN_RESOLUTION_STATUSES:
        audit = _write_audit_log(
            action=action,
            record_id=record_id,
            sender_phone=sender_phone,
            branch=branch,
            authorized=True,
            status="failed",
            reason="review_item_already_resolved",
            review_queue_path=str(review_path),
            output_root=root,
            occurred_at=timestamp,
        )
        return _result(
            action=action,
            record_id=record_id,
            status="failed",
            branch=branch,
            report_type=report_type,
            report_date=report_date,
            reason="review_item_already_resolved",
            message="review item is already resolved",
            audit_path=str(audit),
        )

    if action == "approve":
        action_result = _approve_review_item(
            review_path=review_path,
            review_payload=review_payload,
            output_root=root,
        )
    elif action == "reject":
        action_result = _reject_review_item(review_path=review_path, review_payload=review_payload, output_root=root)
    elif action == "replay":
        action_result = _replay_review_item(
            review_path=review_path,
            review_payload=review_payload,
            output_root=root,
        )
    else:
        action_result = {
            "status": "failed",
            "reason": "unsupported_supervisor_action",
            "message": "unsupported supervisor action",
        }

    audit = _write_audit_log(
        action=action,
        record_id=record_id,
        sender_phone=sender_phone,
        branch=branch,
        authorized=True,
        status=str(action_result["status"]),
        reason=str(action_result["reason"]),
        review_queue_path=str(review_path),
        output_root=root,
        occurred_at=timestamp,
        extra=action_result,
    )
    if action_result["status"] == "completed":
        _update_review_item(
            review_path=review_path,
            review_payload=review_payload,
            action=action,
            actor=authorization["supervisor"],
            audit_path=str(audit),
            action_result=action_result,
            occurred_at=timestamp,
        )
    return _result(
        action=action,
        record_id=record_id,
        status=str(action_result["status"]),
        branch=branch,
        report_type=report_type,
        report_date=report_date,
        governance_status=_text_or_none(action_result.get("governance_status")),
        reason=str(action_result["reason"]),
        message=str(action_result["message"]),
        audit_path=str(audit),
    )


def _approve_review_item(
    *,
    review_path: Path,
    review_payload: dict[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    candidate_payload = review_payload.get("candidate_payload")
    if not isinstance(candidate_payload, Mapping):
        return {
            "status": "failed",
            "reason": "missing_candidate_payload",
            "message": "review item has no candidate payload to approve",
        }

    report_type = _text_or_none(review_payload.get("report_type"))
    writer = _REPORT_TYPE_TO_WRITER.get(report_type or "")
    if writer is None:
        return {
            "status": "failed",
            "reason": "unsupported_report_type",
            "message": "review item report type is not supported for approval",
        }

    payload = dict(candidate_payload)
    payload["status"] = "accepted_with_warning" if _has_warnings(payload) else "accepted"
    payload.pop("export_allowed", None)
    payload.pop("governance", None)
    metadata = _approval_metadata(
        review_path=review_path,
        review_payload=review_payload,
        output_root=output_root,
    )
    metadata["acceptance"] = {"decision": "accept", "reason": "supervisor_approved"}

    write_result = writer(payload, metadata=metadata)
    governance = getattr(write_result, "governance", None) if write_result is not None else None
    governance_status = _text_or_none(getattr(governance, "status", None))
    if write_result is None or governance is None or governance_status not in _ACCEPTED_STATUSES:
        return {
            "status": "failed",
            "reason": governance_status or "approval_write_failed",
            "message": "governance did not accept the approved review item",
            "governance_status": governance_status,
        }

    return {
        "status": "completed",
        "reason": "approved_after_review",
        "message": "approved successfully",
        "governance_status": governance_status,
        "structured_output_path": str(write_result.path),
    }


def _reject_review_item(
    *,
    review_path: Path,
    review_payload: dict[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    source_text_path = _source_text_path(review_payload, output_root=output_root)
    if source_text_path is None or not source_text_path.exists():
        return {
            "status": "failed",
            "reason": "missing_source_record",
            "message": "review item has no source record for rejection copy",
        }

    report_type = _text_or_none(review_payload.get("report_type")) or "unknown"
    rejection_dir = (
        get_rejected_path(report_type)
        if output_root == record_paths.RECORDS_DIR.parent
        else output_root / "records" / "rejected" / "whatsapp" / safe_segment(report_type)
    )
    filename = build_rejected_filename(report_type, "supervisor_rejected")
    text_path = rejection_dir / filename
    meta_path = text_path.with_suffix(".meta.json")
    text = source_text_path.read_text(encoding="utf-8")
    write_text_file(text_path, text)
    write_json_file(
        meta_path,
        {
            "review_queue_path": str(review_path),
            "report_type": report_type,
            "branch": _text_or_none(review_payload.get("branch")),
            "report_date": _text_or_none(review_payload.get("date")),
            "rejection_reason": "supervisor_rejected",
            "source_record_path": str(source_text_path),
            "raw_paths": dict(review_payload.get("raw_paths")) if isinstance(review_payload.get("raw_paths"), Mapping) else {},
        },
    )
    return {
        "status": "completed",
        "reason": "rejected_after_review",
        "message": "rejected successfully",
        "rejected_output_path": str(text_path),
    }


def _replay_review_item(
    *,
    review_path: Path,
    review_payload: dict[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    source_text_path = _source_text_path(review_payload, output_root=output_root)
    if source_text_path is None or not source_text_path.exists():
        return {
            "status": "failed",
            "reason": "missing_source_record",
            "message": "review item has no source record for replay",
        }

    source_kind = _replay_source_kind(source_text_path)
    if source_kind is None:
        return {
            "status": "failed",
            "reason": "unsupported_replay_source",
            "message": "review source must be an archived raw or rejected record",
        }

    buffer = io.StringIO()
    with redirect_stdout(buffer), redirect_stderr(buffer):
        exit_code = replay_records.main(
            [
                "--source",
                source_kind,
                "--mode",
                "orchestrator",
                "--path",
                str(source_text_path),
            ]
        )
    if exit_code != 0:
        return {
            "status": "failed",
            "reason": "replay_failed",
            "message": "replay returned a non-zero exit status",
            "replay_output": buffer.getvalue(),
        }
    return {
        "status": "completed",
        "reason": "replay_requested",
        "message": "replay completed safely",
        "replay_output": buffer.getvalue(),
    }


def _approval_metadata(
    *,
    review_path: Path,
    review_payload: dict[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "validation": dict(review_payload.get("validation")) if isinstance(review_payload.get("validation"), Mapping) else {},
        "review_queue_path": str(review_path),
    }
    governance_context = _governance_context_from_review(review_payload, output_root=output_root)
    if governance_context:
        metadata["governance_context"] = governance_context
    return metadata


def _governance_context_from_review(
    review_payload: dict[str, Any],
    *,
    output_root: Path,
) -> dict[str, Any]:
    raw_paths = review_payload.get("raw_paths")
    raw_meta_path = None
    if isinstance(raw_paths, Mapping):
        raw_meta_path = _path_or_none(raw_paths.get("raw_meta_path"), output_root=output_root)
        raw_sha256 = _text_or_none(raw_paths.get("raw_sha256"))
    else:
        raw_sha256 = None
    raw_meta = _read_json(raw_meta_path) if raw_meta_path is not None else None
    context = {
        "message_id": _text_or_none(raw_meta.get("message_id")) if isinstance(raw_meta, Mapping) else None,
        "raw_sha256": raw_sha256
        or (_text_or_none(raw_meta.get("raw_sha256")) if isinstance(raw_meta, Mapping) else None),
        "raw_meta_path": str(raw_meta_path) if raw_meta_path is not None else None,
        "classified_report_type": _text_or_none(review_payload.get("report_type")),
    }
    return {key: value for key, value in context.items() if value is not None}


def _resolve_review_item(*, record_id: str, output_root: Path) -> dict[str, Any]:
    review_root = output_root / "records" / "review"
    matches = sorted(review_root.glob(f"**/{record_id}.json"))
    if not matches:
        return {"status": "missing", "reason": "review_item_not_found"}
    if len(matches) > 1:
        return {"status": "ambiguous", "reason": "multiple_review_items_found"}
    payload = _read_json(matches[0])
    if payload is None:
        return {"status": "invalid", "reason": "review_item_invalid_json"}
    return {"status": "resolved", "path": matches[0], "payload": payload, "reason": "review_item_resolved"}


def _update_review_item(
    *,
    review_path: Path,
    review_payload: dict[str, Any],
    action: str,
    actor: Mapping[str, Any],
    audit_path: str,
    action_result: Mapping[str, Any],
    occurred_at: str,
) -> None:
    updated = dict(review_payload)
    if action == "approve":
        updated["resolution_status"] = "accepted_after_review"
        updated["acceptance"] = {"decision": "accept", "reason": "supervisor_approved"}
    elif action == "reject":
        updated["resolution_status"] = "rejected_after_review"
        updated["acceptance"] = {"decision": "reject", "reason": "supervisor_rejected"}
    updated["last_supervisor_action"] = {
        "action": action,
        "actor_name": _text_or_none(actor.get("name")) if isinstance(actor, Mapping) else None,
        "sender_phone": _text_or_none(actor.get("sender_phone")) if isinstance(actor, Mapping) else None,
        "occurred_at": occurred_at,
        "audit_path": audit_path,
        "result": dict(action_result),
    }
    write_json_file(review_path, updated)


def _write_audit_log(
    *,
    action: str,
    record_id: str,
    sender_phone: str | None,
    branch: str | None,
    authorized: bool,
    status: str,
    reason: str,
    review_queue_path: str | None,
    output_root: Path,
    occurred_at: str,
    extra: Mapping[str, Any] | None = None,
) -> Path:
    audit_dir = output_root / "records" / "audit" / "supervisor_actions" / occurred_at[:10]
    filename = f"{safe_segment(occurred_at)}__{safe_segment(action)}__{safe_segment(record_id)}.json"
    payload = {
        "occurred_at": occurred_at,
        "action": action,
        "record_id": record_id,
        "sender_phone": sender_phone,
        "branch": _branch_or_none(branch),
        "authorized": authorized,
        "status": status,
        "reason": reason,
        "review_queue_path": review_queue_path,
        "details": dict(extra) if isinstance(extra, Mapping) else {},
    }
    path = audit_dir / filename
    write_json_file(path, payload)
    return path


def _source_text_path(review_payload: dict[str, Any], *, output_root: Path) -> Path | None:
    raw_paths = review_payload.get("raw_paths")
    if isinstance(raw_paths, Mapping):
        for field_name in ("raw_txt_path", "raw_text_path"):
            path = _path_or_none(raw_paths.get(field_name), output_root=output_root)
            if path is not None:
                return path
    provenance = review_payload.get("provenance")
    if isinstance(provenance, Mapping):
        path = _path_or_none(provenance.get("source_record_path"), output_root=output_root)
        if path is not None:
            return path
    return None


def _replay_source_kind(path: Path) -> str | None:
    path_text = str(path)
    if "/records/raw/" in path_text:
        return "raw"
    if "/records/rejected/" in path_text:
        return "rejected"
    return None


def _has_warnings(payload: Mapping[str, Any]) -> bool:
    warnings = payload.get("warnings")
    return isinstance(warnings, list) and bool(warnings)


def _path_or_none(value: object, *, output_root: Path | None = None) -> Path | None:
    if not isinstance(value, (str, Path)):
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    root = output_root if output_root is not None else record_paths.RECORDS_DIR.parent
    return root / path


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _result(
    *,
    action: str,
    record_id: str,
    status: str,
    reason: str,
    message: str,
    audit_path: str,
    branch: str | None = None,
    report_type: str | None = None,
    report_date: str | None = None,
    governance_status: str | None = None,
) -> dict[str, Any]:
    return {
        "action": action,
        "record_id": record_id,
        "status": status,
        "reason": reason,
        "message": message,
        "audit_path": audit_path,
        "branch": branch,
        "report_type": report_type,
        "report_date": report_date,
        "governance_status": governance_status,
    }


def _branch_or_none(value: object) -> str | None:
    text = _text_or_none(value)
    if text is None:
        return None
    return canonical_branch_slug_or_none(text)


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _required_text(value: object, *, field_name: str) -> str:
    cleaned = _text_or_none(value)
    if cleaned is None:
        raise ValueError(f"supervisor action field `{field_name}` must be a non-empty string")
    return cleaned


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
