"""Production-safe WhatsApp ingress bridge for raw-first orchestration.

The repo already routes upstream work through `WorkItem(kind="raw_message", payload=...)`.
This bridge keeps that contract and adds two explicit payload blocks:

- `payload["ingress_envelope"]` carries the stable WhatsApp handoff fields.
- `payload["raw_record"]` tells Orchestra when the bridge already wrote the raw audit.

The bridge stays transport + envelope + dispatch only. Classification and routing
remain in `apps.orchestrator_agent.worker`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import apps.orchestrator_agent.worker as orchestrator_worker
from dotenv import load_dotenv
from packages.common.paths import REPO_ROOT
from packages.record_store.naming import safe_segment
from packages.record_store.paths import get_raw_path, get_structured_path
from packages.record_store.writer import write_json_file, write_text_file
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

load_dotenv(REPO_ROOT / ".env.whatsapp_bridge")

LOGGER = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
SERVICE_NAME = "whatsapp_webhook_bridge"
INGRESS_NAME = "whatsapp"
WEBHOOK_SOURCE = "meta_webhook"
WEBHOOK_ROUTE = "/webhook"
SUPPORTED_WEBHOOK_ROUTES = {"/", WEBHOOK_ROUTE, "/webhooks/whatsapp"}
HEALTH_ROUTE = "/health"
UNKNOWN_BUCKET = "unknown"


@dataclass(slots=True)
class InboundMessageEnvelope:
    """Normalized inbound message envelope for the bridge."""

    text: str
    received_at: str
    message_id: str | None
    sender_name: str | None
    sender_phone: str | None
    group_name: str | None
    chat_id: str | None
    entry_id: str | None
    phone_number_id: str | None
    display_phone_number: str | None
    replay: dict[str, Any]
    verify_context: dict[str, Any]
    raw_txt_path: str | None = None
    raw_meta_path: str | None = None
    payload_kind: str = "text"
    channel: str = INGRESS_NAME
    source: str = WEBHOOK_SOURCE


@dataclass(slots=True)
class BridgeHttpResponse:
    """Simple HTTP response object used by tests and the live handler."""

    status_code: int
    body: bytes
    content_type: str = "application/json"


def dispatch_http_request(
    *,
    method: str,
    target: str,
    body: bytes = b"",
    headers: Mapping[str, str] | None = None,
) -> BridgeHttpResponse:
    """Dispatch one HTTP request to the bridge without starting a server."""

    parsed = urlparse(target)
    path = parsed.path or "/"

    if method == "GET" and path == HEALTH_ROUTE:
        return _json_response(HTTPStatus.OK, _health_payload())
    if method == "GET" and path in SUPPORTED_WEBHOOK_ROUTES:
        return _handle_verification(parsed.query)
    if method == "POST" and path in SUPPORTED_WEBHOOK_ROUTES:
        return _handle_webhook_post(body=body, headers=headers or {})
    return _json_response(
        HTTPStatus.NOT_FOUND,
        {
            "ok": False,
            "service": SERVICE_NAME,
            "error_stage": "routing",
            "error": f"unsupported route: {path}",
        },
    )


def _handle_verification(query: str) -> BridgeHttpResponse:
    """Handle Meta webhook verification without touching ingress state."""

    params = parse_qs(query, keep_blank_values=True)
    mode = _first_query_value(params, "hub.mode")
    challenge = _first_query_value(params, "hub.challenge")
    supplied_token = _first_query_value(params, "hub.verify_token")
    expected_token = os.getenv("WHATSAPP_VERIFY_TOKEN")

    if mode != "subscribe" or challenge is None:
        return _json_response(
            HTTPStatus.BAD_REQUEST,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error_stage": "verification",
                "error": "missing Meta verification parameters",
            },
        )
    if not expected_token:
        return _json_response(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error_stage": "verification",
                "error": "WHATSAPP_VERIFY_TOKEN is not configured",
            },
        )
    if supplied_token != expected_token:
        return _json_response(
            HTTPStatus.FORBIDDEN,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error_stage": "verification",
                "error": "verify token mismatch",
            },
        )

    return BridgeHttpResponse(
        status_code=HTTPStatus.OK,
        body=challenge.encode("utf-8"),
        content_type="text/plain; charset=utf-8",
    )


def _handle_webhook_post(*, body: bytes, headers: Mapping[str, str]) -> BridgeHttpResponse:
    """Process one POST payload and return a structured JSON acknowledgement."""

    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        LOGGER.exception("whatsapp webhook payload decode failed")
        return _json_response(
            HTTPStatus.BAD_REQUEST,
            {
                "ok": False,
                "ingress": INGRESS_NAME,
                "error_stage": "payload_extraction",
                "error": str(exc),
            },
        )

    try:
        messages = extract_inbound_messages(payload=payload, headers=headers)
    except Exception as exc:
        LOGGER.exception("whatsapp webhook payload extraction failed")
        return _json_response(
            HTTPStatus.BAD_REQUEST,
            {
                "ok": False,
                "ingress": INGRESS_NAME,
                "error_stage": "payload_extraction",
                "error": str(exc),
            },
        )

    if not messages:
        return _json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "ingress": INGRESS_NAME,
                "message_count": 0,
                "results": [],
                "reason": "no_supported_messages",
            },
        )

    results = [_process_envelope(envelope) for envelope in messages]
    return _json_response(HTTPStatus.OK, _response_payload(results))


def extract_inbound_messages(
    *,
    payload: Mapping[str, Any],
    headers: Mapping[str, str] | None = None,
) -> list[InboundMessageEnvelope]:
    """Extract supported inbound text messages from bridge-native or Meta payloads."""

    direct_message = _extract_direct_envelope(payload=payload, headers=headers or {})
    if direct_message is not None:
        return [direct_message]

    return _extract_meta_messages(payload=payload, headers=headers or {})


def _extract_direct_envelope(
    *,
    payload: Mapping[str, Any],
    headers: Mapping[str, str],
) -> InboundMessageEnvelope | None:
    """Return one explicitly shaped bridge envelope when provided."""

    payload_block = payload.get("payload")
    candidate = payload_block if isinstance(payload_block, Mapping) else payload
    raw_message = payload.get("raw_message")
    text = _clean_text(
        candidate.get("text")
        if isinstance(candidate, Mapping)
        else None
    )
    if text is None and isinstance(raw_message, Mapping):
        text = _clean_text(raw_message.get("text"))
    replay = _sanitize_replay(payload.get("replay"))
    if text is None:
        if replay.get("is_replay") is True:
            raise ValueError("explicit replay payload requires `text` or `raw_message.text`")
        return None

    metadata = payload.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}
    received_at = _clean_text(metadata_map.get("received_at")) or _utc_timestamp()
    verify_context = _verify_context_from_headers(headers)
    if payload.get("verify_context") and isinstance(payload.get("verify_context"), Mapping):
        verify_context.update(dict(payload["verify_context"]))
    chat_id = _clean_text(metadata_map.get("chat_id"))
    if chat_id is None and isinstance(candidate, Mapping):
        chat_id = _clean_text(candidate.get("chat_id"))

    return InboundMessageEnvelope(
        text=text,
        received_at=received_at,
        message_id=_clean_text(candidate.get("message_id") if isinstance(candidate, Mapping) else None),
        sender_name=_clean_text(candidate.get("sender_name") if isinstance(candidate, Mapping) else None),
        sender_phone=_clean_text(candidate.get("sender_phone") if isinstance(candidate, Mapping) else None),
        group_name=_clean_text(candidate.get("group_name") if isinstance(candidate, Mapping) else None),
        chat_id=chat_id,
        entry_id=_clean_text(metadata_map.get("entry_id")),
        phone_number_id=_clean_text(metadata_map.get("phone_number_id")),
        display_phone_number=_clean_text(metadata_map.get("display_phone_number")),
        replay=replay,
        verify_context=verify_context,
        raw_txt_path=_clean_text(candidate.get("raw_txt_path") if isinstance(candidate, Mapping) else None)
        or _clean_text(payload.get("raw_txt_path")),
        raw_meta_path=_clean_text(candidate.get("raw_meta_path") if isinstance(candidate, Mapping) else None)
        or _clean_text(payload.get("raw_meta_path")),
    )


def _extract_meta_messages(
    *,
    payload: Mapping[str, Any],
    headers: Mapping[str, str],
) -> list[InboundMessageEnvelope]:
    """Extract text messages from the standard Meta webhook payload."""

    entries = payload.get("entry")
    if not isinstance(entries, list):
        return []

    envelopes: list[InboundMessageEnvelope] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        entry_id = _clean_text(entry.get("id"))
        changes = entry.get("changes")
        if not isinstance(changes, list):
            continue
        for change in changes:
            if not isinstance(change, Mapping):
                continue
            value = change.get("value")
            if not isinstance(value, Mapping):
                continue
            messages = value.get("messages")
            if not isinstance(messages, list):
                continue

            metadata = value.get("metadata")
            metadata_map = metadata if isinstance(metadata, Mapping) else {}
            contacts = value.get("contacts")
            contacts_list = contacts if isinstance(contacts, list) else []
            contact = contacts_list[0] if contacts_list and isinstance(contacts_list[0], Mapping) else {}
            profile = contact.get("profile") if isinstance(contact, Mapping) and isinstance(contact.get("profile"), Mapping) else {}
            verify_context = _verify_context_from_headers(headers)
            verify_context["object"] = payload.get("object")

            for message in messages:
                if not isinstance(message, Mapping):
                    continue
                text = _extract_meta_text(message)
                if text is None:
                    continue
                envelopes.append(
                    InboundMessageEnvelope(
                        text=text,
                        received_at=_timestamp_to_iso(_clean_text(message.get("timestamp"))),
                        message_id=_clean_text(message.get("id")),
                        sender_name=_clean_text(profile.get("name")),
                        sender_phone=_clean_text(message.get("from")) or _clean_text(contact.get("wa_id")),
                        group_name=_clean_text(value.get("group_name")),
                        chat_id=_clean_text(message.get("from")),
                        entry_id=entry_id,
                        phone_number_id=_clean_text(metadata_map.get("phone_number_id")),
                        display_phone_number=_clean_text(metadata_map.get("display_phone_number")),
                        replay={"is_replay": False},
                        verify_context=verify_context,
                    )
                )
    return envelopes


def _extract_meta_text(message: Mapping[str, Any]) -> str | None:
    """Return plain text from a supported Meta message payload."""

    text_payload = message.get("text")
    if not isinstance(text_payload, Mapping):
        return None
    return _clean_text(text_payload.get("body"))


def _process_envelope(envelope: InboundMessageEnvelope) -> dict[str, Any]:
    """Run raw-first ingest and orchestration for one envelope."""

    replay = envelope.replay
    raw_sha256 = hashlib.sha256(envelope.text.encode("utf-8")).hexdigest()
    message_sha256 = _message_sha256(envelope)

    try:
        raw_record = (
            _prepare_replay_raw_record(envelope=envelope, raw_sha256=raw_sha256)
            if replay.get("is_replay") is True
            else _write_live_raw_record(
                envelope=envelope,
                raw_sha256=raw_sha256,
                message_sha256=message_sha256,
            )
        )
    except DuplicateLiveMessage as exc:
        LOGGER.info("duplicate live WhatsApp message skipped: %s", exc.message_id or exc.raw_sha256)
        return {
            "ok": True,
            "ingress": INGRESS_NAME,
            "workspace_root": str(REPO_ROOT),
            "raw_written": False,
            "duplicate": True,
            "duplicate_reason": exc.reason,
            "replay": False,
            "orchestrator_status": "skipped",
            "route": None,
            "agent": None,
            "message_id": exc.message_id,
            "message_sha256": message_sha256,
            "raw_sha256": exc.raw_sha256,
            "raw_txt_path": exc.raw_txt_path,
            "raw_meta_path": exc.raw_meta_path,
            "outputs": [],
        }
    except Exception as exc:
        LOGGER.exception("whatsapp raw write failed")
        return {
            "ok": False,
            "ingress": INGRESS_NAME,
            "workspace_root": str(REPO_ROOT),
            "raw_written": False,
            "replay": replay.get("is_replay") is True,
            "error_stage": "raw_write",
            "error": str(exc),
            "message_id": envelope.message_id,
            "message_sha256": message_sha256,
            "raw_sha256": raw_sha256,
            "outputs": [],
        }

    try:
        work_item = build_work_item(
            envelope=envelope,
            raw_record=raw_record,
            raw_sha256=raw_sha256,
            message_sha256=message_sha256,
        )
    except Exception as exc:
        LOGGER.exception("whatsapp work item construction failed")
        return {
            "ok": False,
            "ingress": INGRESS_NAME,
            "workspace_root": str(REPO_ROOT),
            "raw_written": raw_record["raw_written"],
            "replay": replay.get("is_replay") is True,
            "error_stage": "work_item_construction",
            "error": str(exc),
            "message_id": envelope.message_id,
            "message_sha256": message_sha256,
            "raw_sha256": raw_sha256,
            "raw_txt_path": raw_record.get("raw_txt_path"),
            "raw_meta_path": raw_record.get("raw_meta_path"),
            "outputs": [],
        }

    try:
        result = orchestrator_worker.process_work_item(work_item)
    except Exception as exc:
        LOGGER.exception("whatsapp orchestrator dispatch failed")
        return {
            "ok": False,
            "ingress": INGRESS_NAME,
            "workspace_root": str(REPO_ROOT),
            "raw_written": raw_record["raw_written"],
            "replay": replay.get("is_replay") is True,
            "orchestrator_status": "failed",
            "error_stage": "orchestrator",
            "error": str(exc),
            "route": None,
            "agent": None,
            "message_id": envelope.message_id,
            "message_sha256": message_sha256,
            "raw_sha256": raw_sha256,
            "raw_txt_path": raw_record.get("raw_txt_path"),
            "raw_meta_path": raw_record.get("raw_meta_path"),
            "outputs": [],
        }

    return _success_response(
        envelope=envelope,
        raw_record=raw_record,
        raw_sha256=raw_sha256,
        message_sha256=message_sha256,
        result=result,
    )


def build_work_item(
    *,
    envelope: InboundMessageEnvelope,
    raw_record: dict[str, Any],
    raw_sha256: str,
    message_sha256: str,
) -> WorkItem:
    """Build the existing raw-message work item with a stable ingress envelope."""

    sender = envelope.sender_name or envelope.sender_phone
    metadata: dict[str, Any] = {
        "received_at": envelope.received_at,
        "sender": sender,
    }
    if envelope.group_name:
        metadata["branch_hint"] = envelope.group_name

    ingress_envelope = {
        "signal_type": "whatsapp_ingress",
        "source_agent": SERVICE_NAME,
        "received_at": envelope.received_at,
        "payload": {
            "text": envelope.text,
            "sender_name": envelope.sender_name,
            "sender_phone": envelope.sender_phone,
            "group_name": envelope.group_name,
            "message_id": envelope.message_id,
            "message_sha256": message_sha256,
            "raw_sha256": raw_sha256,
            "raw_txt_path": raw_record.get("raw_txt_path"),
            "raw_meta_path": raw_record.get("raw_meta_path"),
            "channel": envelope.channel,
            "payload_kind": envelope.payload_kind,
            "replay": envelope.replay,
        },
        "metadata": {
            "source": envelope.source,
            "entry_id": envelope.entry_id,
            "phone_number_id": envelope.phone_number_id,
            "display_phone_number": envelope.display_phone_number,
            "chat_id": envelope.chat_id,
            "verify_context": dict(envelope.verify_context),
        },
    }

    return WorkItem(
        kind="raw_message",
        payload={
            "source": envelope.source,
            "raw_message": {"text": envelope.text},
            "metadata": metadata,
            "replay": dict(envelope.replay),
            "raw_record": dict(raw_record),
            "ingress_envelope": ingress_envelope,
        },
    )


def _write_live_raw_record(
    *,
    envelope: InboundMessageEnvelope,
    raw_sha256: str,
    message_sha256: str,
) -> dict[str, Any]:
    """Write the immutable raw text and companion metadata before orchestration."""

    raw_txt_path = _raw_txt_path(envelope=envelope, raw_sha256=raw_sha256)
    raw_meta_path = raw_txt_path.with_suffix(".meta.json")
    duplicate = _detect_duplicate(
        envelope=envelope,
        raw_sha256=raw_sha256,
        raw_txt_path=raw_txt_path,
        raw_meta_path=raw_meta_path,
    )
    if duplicate is not None:
        raise duplicate

    if raw_txt_path.exists() or raw_meta_path.exists():
        raise FileExistsError(f"raw audit collision at {raw_txt_path}")

    write_text_file(raw_txt_path, envelope.text)
    write_json_file(
        raw_meta_path,
        _raw_metadata_payload(
            envelope=envelope,
            raw_sha256=raw_sha256,
            message_sha256=message_sha256,
            raw_txt_path=raw_txt_path,
            raw_meta_path=raw_meta_path,
            processing_status="received",
        ),
    )
    return {
        "raw_written": True,
        "raw_txt_path": str(raw_txt_path),
        "raw_meta_path": str(raw_meta_path),
        "raw_sha256": raw_sha256,
    }


def _prepare_replay_raw_record(
    *,
    envelope: InboundMessageEnvelope,
    raw_sha256: str,
) -> dict[str, Any]:
    """Return the existing raw-record references for explicit replay dispatch."""

    replay = envelope.replay
    original_path = _clean_text(replay.get("original_path"))
    raw_txt_path = envelope.raw_txt_path or original_path
    raw_meta_path = envelope.raw_meta_path
    if raw_meta_path is None and raw_txt_path is not None and raw_txt_path.endswith(".txt"):
        raw_meta_path = f"{raw_txt_path[:-4]}.meta.json"

    return {
        "raw_written": False,
        "raw_txt_path": raw_txt_path,
        "raw_meta_path": raw_meta_path,
        "raw_sha256": raw_sha256,
    }


def _raw_metadata_payload(
    *,
    envelope: InboundMessageEnvelope,
    raw_sha256: str,
    message_sha256: str,
    raw_txt_path: Path,
    raw_meta_path: Path,
    processing_status: str,
) -> dict[str, Any]:
    """Return the bridge audit metadata persisted before orchestration."""

    sender = envelope.sender_name or envelope.sender_phone
    return {
        "source": envelope.source,
        "channel": envelope.channel,
        "payload_kind": envelope.payload_kind,
        "received_at": envelope.received_at,
        "sender": sender,
        "sender_name": envelope.sender_name,
        "sender_phone": envelope.sender_phone,
        "group_name": envelope.group_name,
        "branch_hint": envelope.group_name,
        "message_id": envelope.message_id,
        "message_sha256": message_sha256,
        "raw_sha256": raw_sha256,
        "raw_txt_path": str(raw_txt_path),
        "raw_meta_path": str(raw_meta_path),
        "entry_id": envelope.entry_id,
        "phone_number_id": envelope.phone_number_id,
        "display_phone_number": envelope.display_phone_number,
        "chat_id": envelope.chat_id,
        "verify_context": dict(envelope.verify_context),
        "replay": dict(envelope.replay),
        "processing_status": processing_status,
        "ingress_agent": SERVICE_NAME,
    }


def _detect_duplicate(
    *,
    envelope: InboundMessageEnvelope,
    raw_sha256: str,
    raw_txt_path: Path,
    raw_meta_path: Path,
) -> DuplicateLiveMessage | None:
    """Return a duplicate marker when a live raw audit already exists."""

    if not raw_txt_path.exists() and not raw_meta_path.exists():
        return None

    existing_meta = _read_json_file(raw_meta_path)
    existing_message_id = _clean_text(existing_meta.get("message_id"))
    existing_raw_sha256 = _clean_text(existing_meta.get("raw_sha256"))
    existing_received_at = _clean_text(existing_meta.get("received_at"))

    if envelope.message_id and existing_message_id == envelope.message_id:
        return DuplicateLiveMessage(
            reason="message_id",
            message_id=envelope.message_id,
            raw_sha256=raw_sha256,
            raw_txt_path=str(raw_txt_path),
            raw_meta_path=str(raw_meta_path),
        )
    if (
        envelope.message_id is None
        and existing_raw_sha256 == raw_sha256
        and existing_received_at == envelope.received_at
    ):
        return DuplicateLiveMessage(
            reason="raw_sha256_and_received_at",
            message_id=None,
            raw_sha256=raw_sha256,
            raw_txt_path=str(raw_txt_path),
            raw_meta_path=str(raw_meta_path),
        )
    return None


def _success_response(
    *,
    envelope: InboundMessageEnvelope,
    raw_record: dict[str, Any],
    raw_sha256: str,
    message_sha256: str,
    result: AgentResult,
) -> dict[str, Any]:
    """Build the structured success JSON returned to the webhook caller."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    status = payload.get("status")
    orchestrator_status = status if isinstance(status, str) else "ok"
    outputs = _outputs_from_result(result)

    return {
        "ok": True,
        "ingress": INGRESS_NAME,
        "workspace_root": str(REPO_ROOT),
        "raw_written": raw_record["raw_written"],
        "replay": envelope.replay.get("is_replay") is True,
        "orchestrator_status": orchestrator_status,
        "route": _route_from_result(result),
        "agent": result.agent_name,
        "message_id": envelope.message_id,
        "message_sha256": message_sha256,
        "raw_sha256": raw_sha256,
        "raw_txt_path": raw_record.get("raw_txt_path"),
        "raw_meta_path": raw_record.get("raw_meta_path"),
        "outputs": outputs,
    }


def _outputs_from_result(result: AgentResult) -> list[str]:
    """Infer structured output paths from the downstream result when available."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    explicit_outputs = payload.get("outputs")
    if isinstance(explicit_outputs, list) and all(isinstance(path, str) for path in explicit_outputs):
        return list(explicit_outputs)

    status = payload.get("status")
    if status == "invalid_input":
        return []

    branch = payload.get("branch")
    report_date = payload.get("report_date")
    if not isinstance(branch, str) or not isinstance(report_date, str):
        return []

    if result.agent_name == "sales_income_agent":
        return [str(get_structured_path("sales_income", branch, report_date))]
    if result.agent_name == "pricing_stock_release_agent":
        return [str(get_structured_path("pricing_stock_release", branch, report_date))]
    if result.agent_name == "hr_agent":
        subtype = payload.get("signal_subtype")
        if subtype == "staff_attendance":
            signal_type = "hr_attendance"
        elif subtype == "staff_performance":
            signal_type = "hr_performance"
        else:
            return []
        return [str(get_structured_path(signal_type, branch, report_date))]
    if result.agent_name == "staff_performance_agent":
        return [str(get_structured_path("hr_performance", branch, report_date))]
    if result.agent_name == "supervisor_control_agent":
        return [str(get_structured_path("supervisor_control", branch, report_date))]
    return []


def _route_from_result(result: AgentResult) -> str | None:
    """Return one stable route string from the orchestrator or specialist result."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    routing = payload.get("routing")
    if isinstance(routing, Mapping):
        route = _clean_text(routing.get("classification"))
        if route is not None:
            return route

    classification = payload.get("classification")
    if isinstance(classification, Mapping):
        report_type = _clean_text(classification.get("report_type"))
        if report_type is not None:
            return report_type

    if result.agent_name == "sales_income_agent":
        return "sales"
    if result.agent_name == "pricing_stock_release_agent":
        return "bale_summary"
    if result.agent_name == "hr_agent":
        subtype = _clean_text(payload.get("signal_subtype"))
        if subtype is not None:
            return subtype
    if result.agent_name == "staff_performance_agent":
        return "staff_performance"
    if result.agent_name == "supervisor_control_agent":
        return "supervisor_control"
    return None


def _response_payload(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a single-message or multi-message webhook response payload."""

    if len(results) == 1:
        return results[0]

    ok = all(result.get("ok") is True for result in results)
    return {
        "ok": ok,
        "ingress": INGRESS_NAME,
        "workspace_root": str(REPO_ROOT),
        "message_count": len(results),
        "results": results,
    }


def _health_payload() -> dict[str, Any]:
    """Return the live bridge health payload."""

    return {
        "ok": True,
        "service": SERVICE_NAME,
        "workspace_root": str(REPO_ROOT),
        "raw_root": str(get_raw_path(UNKNOWN_BUCKET).parent),
        "orchestrator_enabled": True,
    }


def _json_response(status_code: int, payload: dict[str, Any]) -> BridgeHttpResponse:
    """Build one JSON response."""

    body = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return BridgeHttpResponse(
        status_code=status_code,
        body=body + b"\n",
        content_type="application/json; charset=utf-8",
    )


def _raw_txt_path(*, envelope: InboundMessageEnvelope, raw_sha256: str) -> Path:
    """Return the canonical raw path for one live inbound message."""

    date_segment = envelope.received_at[:10] if len(envelope.received_at) >= 10 else _utc_timestamp()[:10]
    branch_segment = safe_segment(envelope.group_name or UNKNOWN_BUCKET)
    filename = f"{date_segment}__{branch_segment}__{raw_sha256[:12]}.txt"
    return get_raw_path(UNKNOWN_BUCKET) / filename


def _message_sha256(envelope: InboundMessageEnvelope) -> str:
    """Return a stable hash of the normalized inbound message envelope."""

    canonical = json.dumps(
        {
            "text": envelope.text,
            "received_at": envelope.received_at,
            "message_id": envelope.message_id,
            "sender_name": envelope.sender_name,
            "sender_phone": envelope.sender_phone,
            "group_name": envelope.group_name,
            "chat_id": envelope.chat_id,
            "entry_id": envelope.entry_id,
            "phone_number_id": envelope.phone_number_id,
            "display_phone_number": envelope.display_phone_number,
            "replay": envelope.replay,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sanitize_replay(replay: object) -> dict[str, Any]:
    """Keep only explicit replay markers supported by the bridge contract."""

    if not isinstance(replay, Mapping):
        return {"is_replay": False}
    if replay.get("is_replay") is not True:
        return {"is_replay": False}

    safe_replay: dict[str, Any] = {"is_replay": True}
    for field_name in ("source", "original_path", "replayed_at"):
        value = replay.get(field_name)
        if isinstance(value, str) and value.strip():
            safe_replay[field_name] = value.strip()
    if "replayed_at" not in safe_replay:
        safe_replay["replayed_at"] = _utc_timestamp()
    return safe_replay


def _read_json_file(path: Path) -> dict[str, Any]:
    """Read one JSON file when present, otherwise return an empty mapping."""

    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _verify_context_from_headers(headers: Mapping[str, str]) -> dict[str, Any]:
    """Return a small verification context block captured in raw metadata."""

    context: dict[str, Any] = {}
    for header_name in ("X-Hub-Signature-256", "X-Forwarded-For", "User-Agent"):
        value = _header_value(headers, header_name)
        if value is not None:
            context[header_name.lower().replace("-", "_")] = value
    return context


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    """Return one header value case-insensitively."""

    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _timestamp_to_iso(timestamp: str | None) -> str:
    """Convert Meta Unix timestamps to ISO-8601 UTC."""

    if not timestamp:
        return _utc_timestamp()
    try:
        dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return _utc_timestamp()
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_timestamp() -> str:
    """Return a stable UTC timestamp."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_text(value: object) -> str | None:
    """Return one stripped string value when present."""

    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _first_query_value(params: Mapping[str, list[str]], key: str) -> str | None:
    """Return the first query value for one key."""

    values = params.get(key)
    if not values:
        return None
    return values[0]


class DuplicateLiveMessage(Exception):
    """Explicit duplicate signal used to suppress duplicate raw writes."""

    def __init__(
        self,
        *,
        reason: str,
        message_id: str | None,
        raw_sha256: str,
        raw_txt_path: str,
        raw_meta_path: str,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.message_id = message_id
        self.raw_sha256 = raw_sha256
        self.raw_txt_path = raw_txt_path
        self.raw_meta_path = raw_meta_path


class WhatsAppWebhookHandler(BaseHTTPRequestHandler):
    """Threaded HTTP handler for live Meta webhook delivery."""

    server_version = "TopTownWhatsAppBridge/1.0"

    def do_GET(self) -> None:  # noqa: N802
        self._respond(dispatch_http_request(method="GET", target=self.path, headers=self.headers))

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_length)
        self._respond(
            dispatch_http_request(
                method="POST",
                target=self.path,
                body=body,
                headers=self.headers,
            )
        )

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("%s - %s", self.address_string(), format % args)

    def _respond(self, response: BridgeHttpResponse) -> None:
        self.send_response(response.status_code)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(response.body)))
        self.end_headers()
        self.wfile.write(response.body)


def main() -> int:
    """Run the live webhook bridge server."""

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    host = os.getenv("WHATSAPP_WEBHOOK_HOST", DEFAULT_HOST)
    port = int(os.getenv("WHATSAPP_WEBHOOK_PORT", os.getenv("PORT", str(DEFAULT_PORT))))
    server = ThreadingHTTPServer((host, port), WhatsAppWebhookHandler)
    LOGGER.info("listening on %s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("shutting down")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
