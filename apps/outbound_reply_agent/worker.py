"""Dispatch persisted WhatsApp replies through the controlled outbound sender."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any

from packages.common.paths import REPO_ROOT
from packages.response_store import load_response_artifact, update_response_artifact_dispatch
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.whatsapp_outbound import send_whatsapp_text

AGENT_NAME = "outbound_reply_agent"
LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class OutboundReplyAgentWorker:
    """Send one persisted response artifact back to WhatsApp."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        """Dispatch one outbound reply work item."""

        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Process one outbound reply work item into one dispatch result."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    result = dispatch_outbound_reply(payload, output_root=REPO_ROOT)
    status = _string_or_none(result.get("status")) or _string_or_none(result.get("dispatch_status")) or "failed"
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "status": status,
            **result,
        },
    )


def dispatch_outbound_reply(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Send one generated WhatsApp reply and persist dispatch metadata."""

    source_root = Path(output_root) if output_root is not None else Path(REPO_ROOT)
    normalized, validation_error = _normalized_payload(payload)
    if validation_error is not None:
        _log_event(
            "error",
            "outbound_send_failed",
            response_id=_string_or_none(payload.get("response_id")),
            response_type=_string_or_none(payload.get("response_type")),
            source_message_id=_string_or_none(payload.get("source_message_id")),
            sender_phone=_string_or_none(payload.get("sender_phone")),
            error=validation_error,
        )
        return {
            "status": "invalid_input",
            "dispatch_status": "failed",
            "provider_message_id": None,
            "dispatch_error": validation_error,
            "http_status": None,
            "dispatched_at": None,
            "artifact_updated": False,
            "json_path": None,
            "text_path": None,
        }

    artifact = load_response_artifact(normalized["response_id"], output_root=source_root)
    if artifact is None:
        error_message = "response_artifact_not_found"
        _log_event(
            "error",
            "outbound_send_failed",
            response_id=normalized["response_id"],
            response_type=normalized["response_type"],
            source_message_id=normalized["source_message_id"],
            sender_phone=normalized["sender_phone"],
            error=error_message,
        )
        return {
            **normalized,
            "status": "failed",
            "dispatch_status": "failed",
            "provider_message_id": None,
            "dispatch_error": error_message,
            "http_status": None,
            "dispatched_at": None,
            "artifact_updated": False,
            "json_path": None,
            "text_path": None,
        }

    _log_event(
        "info",
        "outbound_send_attempted",
        response_id=normalized["response_id"],
        response_type=normalized["response_type"],
        source_message_id=normalized["source_message_id"],
        sender_phone=normalized["sender_phone"],
    )

    try:
        dispatch_result = send_whatsapp_text(
            to=normalized["sender_phone"],
            body=normalized["response_text"],
            source_message_id=normalized["source_message_id"],
            response_id=normalized["response_id"],
            response_type=normalized["response_type"],
            is_replay=payload.get("is_replay") is True,
        )
    except Exception as exc:  # pragma: no cover - defensive branch
        dispatch_result = {
            "dispatch_status": "failed",
            "provider_message_id": None,
            "error": str(exc),
            "http_status": None,
        }

    dispatch_status = _normalized_dispatch_status(dispatch_result)
    provider_message_id = _string_or_none(dispatch_result.get("provider_message_id"))
    dispatch_error = _string_or_none(dispatch_result.get("dispatch_error") or dispatch_result.get("error"))
    http_status = _int_or_none(dispatch_result.get("http_status"))
    dispatched_at = (
        _utc_timestamp()
        if dispatch_status in {"sent", "failed"}
        else _artifact_dispatched_at(artifact)
    )

    updated_artifact = update_response_artifact_dispatch(
        normalized["response_id"],
        dispatch_status=dispatch_status,
        provider_message_id=provider_message_id,
        dispatch_error=dispatch_error,
        http_status=http_status,
        dispatched_at=dispatched_at,
        output_root=source_root,
    )

    log_level = "error" if dispatch_status == "failed" else "info"
    log_event = "outbound_send_failed" if dispatch_status == "failed" else "outbound_send_success"
    _log_event(
        log_level,
        log_event,
        response_id=normalized["response_id"],
        response_type=normalized["response_type"],
        source_message_id=normalized["source_message_id"],
        sender_phone=normalized["sender_phone"],
        dispatch_status=dispatch_status,
        provider_message_id=provider_message_id,
        http_status=http_status,
        error=dispatch_error,
    )

    return {
        **normalized,
        "status": dispatch_status,
        "dispatch_status": dispatch_status,
        "provider_message_id": provider_message_id,
        "dispatch_error": dispatch_error,
        "http_status": http_status,
        "dispatched_at": dispatched_at,
        "artifact_updated": True,
        "json_path": updated_artifact["json_path"],
        "text_path": updated_artifact["text_path"],
    }


def _normalized_payload(payload: Mapping[str, Any]) -> tuple[dict[str, str], str | None]:
    sender_phone = _required_text(payload.get("sender_phone"))
    response_text = _required_text(payload.get("response_text"))
    response_type = _required_text(payload.get("response_type"))
    source_message_id = _required_text(payload.get("source_message_id"))
    response_id = _required_text(payload.get("response_id"))
    if None in {sender_phone, response_text, response_type, source_message_id, response_id}:
        return {}, "missing_required_outbound_reply_fields"
    return (
        {
            "sender_phone": sender_phone,
            "response_text": response_text,
            "response_type": response_type,
            "source_message_id": source_message_id,
            "response_id": response_id,
        },
        None,
    )


def _artifact_dispatched_at(artifact: Mapping[str, Any]) -> str | None:
    payload = artifact.get("payload")
    if not isinstance(payload, Mapping):
        return None
    return _string_or_none(payload.get("dispatched_at"))


def _normalized_dispatch_status(result: Mapping[str, Any]) -> str:
    status = _string_or_none(result.get("dispatch_status"))
    if status in {"sent", "failed", "dry_run", "suppressed", "duplicate", "generated", "skipped"}:
        return status
    return "sent"


def _log_event(level: str, event: str, **fields: Any) -> None:
    message = json.dumps({"event": event, **fields}, sort_keys=True, ensure_ascii=True)
    getattr(LOGGER, level)(message)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _required_text(value: object) -> str | None:
    text = _string_or_none(value)
    return text


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None
