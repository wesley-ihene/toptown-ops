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
import hmac
import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from apps.command_handler.worker import handle_whatsapp_command
from apps.conversation_context import store_sender_interaction
from apps.command_router.worker import route_whatsapp_command
from apps.ceo_router.worker import handle_ceo_query
from apps.cross_branch_router.worker import handle_cross_branch_query
from apps.nl_intent_router.worker import route_natural_language_command
from apps.outbound_reply_agent import dispatch_outbound_reply
from apps.pre_ingestion_validator import validate_inbound_text
from apps.supervisor_commands.worker import handle_supervisor_command
import apps.orchestrator_agent.worker as orchestrator_worker
from dotenv import load_dotenv
from packages.common.paths import REPO_ROOT
from packages.human_tolerance import analyze_human_whatsapp_text
from packages.observability import record_conversation_reply_event, record_pre_ingestion_validation_event
from packages.record_store.automation import dispatch_whatsapp_response, generate_whatsapp_conversation_reply
from packages.record_store.duplicate_archive import archive_duplicate_record
from packages.record_store.naming import safe_segment
from packages.record_store.paths import get_raw_path, get_structured_path
from packages.record_store.writer import write_json_file, write_text_file
from packages.response_store import load_response_artifact, write_response_artifacts
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.taop_feedback import build_operational_query_response, detect_message_intent
from scripts.whatsapp_webhook_signature import verify_meta_signature

load_dotenv(REPO_ROOT / ".env.whatsapp_bridge", override=False)

LOGGER = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
SERVICE_NAME = "whatsapp_webhook_bridge"
RUNTIME_STATUS = "LIVE_RUNTIME"
RUNTIME_OWNER = "whatsapp_webhook_bridge"
RUNTIME_NOTE = (
    "Live report-ingestion owner; legacy apps.orchestra is not wired into the "
    "current ingress runtime."
)
INGRESS_NAME = "whatsapp"
WEBHOOK_SOURCE = "meta_webhook"
WEBHOOK_ROUTE = "/webhook"
SUPPORTED_WEBHOOK_ROUTES = {"/", WEBHOOK_ROUTE, "/webhooks/whatsapp"}
HEALTH_ROUTE = "/health"
UNKNOWN_BUCKET = "unknown"
DUPLICATE_NOTICE_RESPONSE_TEXT = "\n".join(
    [
        "ℹ️ TAOP DUPLICATE REPORT DETECTED",
        "",
        "This report was already received and processed earlier.",
        "No new processing was applied.",
    ]
)


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
    request_headers = headers or {}

    LOGGER.info(
        "bridge request received: method=%s path=%s body_bytes=%s user_agent=%s forwarded_for=%s",
        method,
        path,
        len(body),
        _header_value(request_headers, "User-Agent"),
        _header_value(request_headers, "X-Forwarded-For"),
    )

    if method == "GET" and path == HEALTH_ROUTE:
        return _json_response(HTTPStatus.OK, _health_payload())
    if method == "GET" and path in SUPPORTED_WEBHOOK_ROUTES:
        return _handle_verification(parsed.query)
    if method == "POST" and path in SUPPORTED_WEBHOOK_ROUTES:
        return _handle_webhook_post(body=body, headers=request_headers)
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
    if not hmac.compare_digest(supplied_token or "", expected_token or ""):
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

    if not verify_meta_signature(body, _header_value(headers, "X-Hub-Signature-256")):
        return _json_response(
            HTTPStatus.FORBIDDEN,
            {
                "ok": False,
                "ingress": INGRESS_NAME,
                "error_stage": "signature_verification",
                "error": "invalid webhook signature",
            },
        )

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

    payload_summary = _summarize_payload(payload)
    LOGGER.info(
        "whatsapp payload summary: object=%s messages=%s statuses=%s unsupported_changes=%s unsupported_message_types=%s",
        payload_summary["object"],
        payload_summary["message_count"],
        payload_summary["status_count"],
        ",".join(payload_summary["unsupported_change_fields"]) or "none",
        ",".join(payload_summary["unsupported_message_types"]) or "none",
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
        LOGGER.info(
            "whatsapp payload ignored: reason=no_supported_messages messages=%s statuses=%s unsupported_changes=%s unsupported_message_types=%s",
            payload_summary["message_count"],
            payload_summary["status_count"],
            ",".join(payload_summary["unsupported_change_fields"]) or "none",
            ",".join(payload_summary["unsupported_message_types"]) or "none",
        )
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
    payload_kind = _clean_text(
        candidate.get("payload_kind")
        if isinstance(candidate, Mapping)
        else None
    ) or _clean_text(candidate.get("type") if isinstance(candidate, Mapping) else None) or "text"
    text = _text_value(
        candidate.get("text")
        if isinstance(candidate, Mapping)
        else None
    )
    if text is None and isinstance(raw_message, Mapping):
        text = _text_value(raw_message.get("text"))
    replay = _sanitize_replay(payload.get("replay"))
    if text is None:
        if payload_kind != "text":
            text = ""
        elif replay.get("is_replay") is True:
            raise ValueError("explicit replay payload requires `text` or `raw_message.text`")
        else:
            return None
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
        payload_kind=payload_kind,
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
                payload_kind = _clean_text(message.get("type")) or "text"
                text = _extract_meta_text(message)
                if text is None and payload_kind == "text":
                    text = ""
                if text is None and payload_kind != "text":
                    text = ""
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
                        payload_kind=payload_kind,
                    )
                )
    return envelopes


def _extract_meta_text(message: Mapping[str, Any]) -> str | None:
    """Return plain text from a supported Meta message payload."""

    text_payload = message.get("text")
    if not isinstance(text_payload, Mapping):
        return None
    return _text_value(text_payload.get("body"))


def _process_envelope(envelope: InboundMessageEnvelope) -> dict[str, Any]:
    """Run raw-first ingest and orchestration for one envelope."""

    replay = envelope.replay
    human_tolerance = analyze_human_whatsapp_text(envelope.text)
    normalized_text = human_tolerance.normalized_text or envelope.text
    raw_sha256 = hashlib.sha256(envelope.text.encode("utf-8")).hexdigest()
    message_sha256 = _message_sha256(envelope)
    LOGGER.info(
        "processing inbound envelope: message_id=%s replay=%s received_at=%s raw_sha256=%s human_tolerance=%s",
        envelope.message_id,
        replay.get("is_replay") is True,
        envelope.received_at,
        raw_sha256,
        human_tolerance.applied,
    )

    try:
        raw_record = (
            _prepare_replay_raw_record(envelope=envelope, raw_sha256=raw_sha256)
            if replay.get("is_replay") is True
            else _write_live_raw_record(
                envelope=envelope,
                raw_sha256=raw_sha256,
                message_sha256=message_sha256,
                human_tolerance=human_tolerance.to_payload(),
            )
        )
    except DuplicateLiveMessage as exc:
        duplicate_reason = _bridge_duplicate_reason(exc.reason)
        archive_duplicate_record(
            source_message_id=envelope.message_id,
            sender_phone=envelope.sender_phone,
            branch=envelope.group_name,
            report_type=None,
            report_date=None,
            raw_txt_path=exc.raw_txt_path,
            raw_meta_path=exc.raw_meta_path,
            duplicate_reason=duplicate_reason,
            duplicate_basis=f"bridge_live_raw:{exc.reason}",
            original_or_duplicate_of=exc.raw_meta_path,
            output_root=REPO_ROOT,
        )
        LOGGER.info(
            "duplicate live WhatsApp message skipped: message_id=%s raw_sha256=%s reason=%s raw_txt_path=%s",
            exc.message_id,
            exc.raw_sha256,
            exc.reason,
            exc.raw_txt_path,
        )
        response = {
            "ok": True,
            "ingress": INGRESS_NAME,
            "workspace_root": str(REPO_ROOT),
            "raw_written": False,
            "duplicate": True,
            "duplicate_reason": duplicate_reason,
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
        response["conversation_response"] = _generate_conversation_response(
            envelope=envelope,
            outcome={
                "status": "duplicate",
                "governance": {"status": "duplicate", "reasons": [duplicate_reason]},
                "policy_guard": {"duplicate": True, "reason": duplicate_reason},
                "branch_hint": envelope.group_name,
                "raw_message": {"text": envelope.text},
                "raw_record": {
                    "raw_txt_path": exc.raw_txt_path,
                    "raw_meta_path": exc.raw_meta_path,
                },
            },
        )
        return _with_conversation_response_output(response)
    except Exception as exc:
        LOGGER.exception(
            "whatsapp raw write failed: message_id=%s raw_sha256=%s",
            envelope.message_id,
            raw_sha256,
        )
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

    command = route_whatsapp_command(
        envelope.text,
        source_message_id=envelope.message_id,
        sender_phone=envelope.sender_phone,
        channel=envelope.channel,
        is_replay=envelope.replay.get("is_replay") is True,
    )
    if command.get("is_command") is True:
        return _command_response(
            envelope=envelope,
            raw_record=raw_record,
            raw_sha256=raw_sha256,
            message_sha256=message_sha256,
            command=command,
        )

    nl_command = route_natural_language_command(
        envelope.text,
        source_message_id=envelope.message_id,
        sender_phone=envelope.sender_phone,
        channel=envelope.channel,
        is_replay=envelope.replay.get("is_replay") is True,
        received_at=envelope.received_at,
        output_root=str(REPO_ROOT),
    )
    if isinstance(nl_command, Mapping) and nl_command.get("is_command") is True:
        return _command_response(
            envelope=envelope,
            raw_record=raw_record,
            raw_sha256=raw_sha256,
            message_sha256=message_sha256,
            command=nl_command,
        )

    intent = detect_message_intent(envelope.text)
    if intent.get("message_intent") == "operational_query":
        return _operational_query_response(
            envelope=envelope,
            raw_record=raw_record,
            raw_sha256=raw_sha256,
            message_sha256=message_sha256,
            intent=intent,
        )

    validation_result: dict[str, Any] | None = None
    if replay.get("is_replay") is not True:
        validation_result = validate_inbound_text(
            normalized_text,
            payload_kind=envelope.payload_kind,
            metadata={
                "message_id": envelope.message_id,
                "received_at": envelope.received_at,
                "human_tolerance_applied": human_tolerance.applied,
            },
        )
        _record_pre_ingestion_validation(
            envelope=envelope,
            raw_record=raw_record,
            result=validation_result,
        )
        _write_pre_ingestion_validation_to_raw_meta(
            raw_meta_path=raw_record.get("raw_meta_path"),
            validation_result=validation_result,
        )
        if validation_result.get("status") == "rejected":
            LOGGER.info(
                "pre-ingestion validation rejected message before orchestration: message_id=%s reasons=%s",
                envelope.message_id,
                ",".join(_validation_reason_codes(validation_result)) or "none",
            )
            return _validation_rejection_response(
                envelope=envelope,
                raw_record=raw_record,
                raw_sha256=raw_sha256,
                message_sha256=message_sha256,
                validation_result=validation_result,
            )

    try:
        work_item = build_work_item(
            envelope=envelope,
            raw_record=raw_record,
            raw_sha256=raw_sha256,
            message_sha256=message_sha256,
            validation_result=validation_result,
            normalized_text=normalized_text,
            human_tolerance=human_tolerance.to_payload(),
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
        LOGGER.exception(
            "whatsapp orchestrator dispatch failed: message_id=%s raw_txt_path=%s",
            envelope.message_id,
            raw_record.get("raw_txt_path"),
        )
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

    response = _success_response(
        envelope=envelope,
        raw_record=raw_record,
        raw_sha256=raw_sha256,
        message_sha256=message_sha256,
        result=result,
    )
    LOGGER.info(
        "whatsapp orchestrator dispatch completed: message_id=%s agent=%s status=%s route=%s outputs=%s",
        envelope.message_id,
        response.get("agent"),
        response.get("orchestrator_status"),
        response.get("route"),
        len(response.get("outputs", [])) if isinstance(response.get("outputs"), list) else 0,
    )
    return response


def build_work_item(
    *,
    envelope: InboundMessageEnvelope,
    raw_record: dict[str, Any],
    raw_sha256: str,
    message_sha256: str,
    validation_result: dict[str, Any] | None = None,
    normalized_text: str | None = None,
    human_tolerance: Mapping[str, Any] | None = None,
) -> WorkItem:
    """Build the existing raw-message work item with a stable ingress envelope."""

    sender = envelope.sender_name or envelope.sender_phone
    metadata: dict[str, Any] = {
        "received_at": envelope.received_at,
        "sender": sender,
    }
    if envelope.group_name:
        metadata["branch_hint"] = envelope.group_name

    has_validation = isinstance(validation_result, Mapping)
    cleaned_text = (
        validation_result.get("cleaned_text")
        if has_validation and validation_result.get("status") in {"accepted", "cleaned"}
        else normalized_text or envelope.text
    )
    ingress_envelope = {
        "signal_type": "whatsapp_ingress",
        "source_agent": SERVICE_NAME,
        "received_at": envelope.received_at,
        "payload": {
            "text": cleaned_text,
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

    work_payload: dict[str, Any] = {
        "source": envelope.source,
        "raw_message": {
            "text": envelope.text,
            "normalized_text": normalized_text or envelope.text,
        },
        "metadata": metadata,
        "replay": dict(envelope.replay),
        "raw_record": dict(raw_record),
        "ingress_envelope": ingress_envelope,
        "ingress_policy": {
            "reject_mixed_reports": False,
        },
    }
    if isinstance(human_tolerance, Mapping) and human_tolerance:
        work_payload["human_tolerance"] = dict(human_tolerance)
    if has_validation:
        work_payload["cleaned_text"] = cleaned_text
        work_payload["pre_ingestion_validation"] = dict(validation_result)

    return WorkItem(kind="raw_message", payload=work_payload)


def _write_live_raw_record(
    *,
    envelope: InboundMessageEnvelope,
    raw_sha256: str,
    message_sha256: str,
    human_tolerance: Mapping[str, Any] | None = None,
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
        raw_txt_path, raw_meta_path = _collision_safe_raw_paths(
            raw_txt_path=raw_txt_path,
            message_sha256=message_sha256,
        )
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
        LOGGER.info(
            "raw audit path collision resolved: message_id=%s raw_sha256=%s raw_txt_path=%s",
            envelope.message_id,
            raw_sha256,
            raw_txt_path,
        )

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
            human_tolerance=human_tolerance,
        ),
    )
    LOGGER.info(
        "raw audit written: message_id=%s raw_txt_path=%s raw_meta_path=%s",
        envelope.message_id,
        raw_txt_path,
        raw_meta_path,
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
    human_tolerance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the bridge audit metadata persisted before orchestration."""

    sender = envelope.sender_name or envelope.sender_phone
    payload = {
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
    if isinstance(human_tolerance, Mapping) and human_tolerance:
        payload["human_tolerance"] = dict(human_tolerance)
    return payload


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
    raw_metadata = _read_raw_metadata(raw_meta_path=_clean_text(raw_record.get("raw_meta_path")))
    policy_guard = {
        **(
            raw_metadata.get("policy_guard")
            if isinstance(raw_metadata.get("policy_guard"), Mapping)
            else {}
        ),
        **(payload.get("policy_guard") if isinstance(payload.get("policy_guard"), Mapping) else {}),
    }
    processing_status = (
        _clean_text(payload.get("processing_status"))
        or _clean_text(raw_metadata.get("processing_status"))
        or orchestrator_status
    )
    duplicate_handling = _mapping(payload.get("duplicate_handling"))
    raw_duplicate_handling = _mapping(raw_metadata.get("duplicate_handling"))
    duplicate = (
        processing_status == "duplicate"
        or policy_guard.get("duplicate") is True
        or duplicate_handling.get("duplicate") is True
        or raw_duplicate_handling.get("duplicate") is True
    )
    duplicate_reason = (
        _clean_text(duplicate_handling.get("reason"))
        or _clean_text(raw_duplicate_handling.get("reason"))
        or (_clean_text(policy_guard.get("reason")) if policy_guard.get("duplicate") is True else None)
        if duplicate
        else None
    )

    response = {
        "ok": True,
        "ingress": INGRESS_NAME,
        "workspace_root": str(REPO_ROOT),
        "raw_written": raw_record["raw_written"],
        "duplicate": duplicate,
        "duplicate_reason": duplicate_reason,
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
    response["conversation_response"] = _dispatch_policy_guard_duplicate_notice(
        envelope=envelope,
        raw_metadata=raw_metadata,
        policy_guard=policy_guard,
        processing_status=processing_status,
    ) or _generate_conversation_response(
        envelope=envelope,
        outcome=_result_with_outputs(
            result,
            raw_record=raw_record,
            raw_text=envelope.text,
            branch_hint=envelope.group_name,
        ),
    )
    return _with_conversation_response_output(response)


def _dispatch_policy_guard_duplicate_notice(
    *,
    envelope: InboundMessageEnvelope,
    raw_metadata: Mapping[str, Any],
    policy_guard: Mapping[str, Any],
    processing_status: str,
) -> dict[str, Any] | None:
    """Persist and dispatch the explicit duplicate reply for hard policy rejects."""

    if not _should_dispatch_policy_guard_duplicate_notice(
        processing_status=processing_status,
        policy_guard=policy_guard,
    ):
        return None

    response_context = {
        "source_message_id": envelope.message_id,
        "sender_phone": envelope.sender_phone,
        "response_type": "duplicate_notice",
        "governance_status": "duplicate",
        "report_type": _clean_text(policy_guard.get("report_type"))
        or _clean_text(raw_metadata.get("detected_report_type")),
        "branch": _clean_text(raw_metadata.get("branch_hint")) or envelope.group_name,
        "report_date": _clean_text(raw_metadata.get("report_date")),
        "reason": _clean_text(policy_guard.get("reason")),
        "review_queue_path": None,
        "structured_output_path": None,
        "should_reply": True,
        "is_replay": envelope.replay.get("is_replay") is True,
    }
    payload = {
        "source_message_id": envelope.message_id,
        "sender_phone": envelope.sender_phone,
        "response_type": "duplicate_notice",
        "response_text": DUPLICATE_NOTICE_RESPONSE_TEXT,
        "governance_status": "duplicate",
        "report_type": response_context["report_type"],
        "branch": response_context["branch"],
        "report_date": response_context["report_date"],
        "reason": response_context["reason"],
        "generated_at": _utc_timestamp(),
        "dispatch_status": "generated",
        "provider_message_id": None,
        "dispatch_error": None,
        "http_status": None,
        "dispatched_at": None,
    }

    try:
        artifact = _persist_duplicate_notice_artifact(payload)
        if envelope.replay.get("is_replay") is not True:
            store_sender_interaction(
                sender_phone=envelope.sender_phone,
                response_context=response_context,
                output_root=REPO_ROOT,
            )
        LOGGER.info(
            "duplicate_notice_generated: response_id=%s source_message_id=%s sender_phone=%s governance_status=duplicate",
            artifact["response_id"],
            envelope.message_id,
            envelope.sender_phone,
        )
        dispatch_payload = dict(artifact["payload"])
        dispatch_payload["response_id"] = artifact["response_id"]
        dispatch_payload["is_replay"] = envelope.replay.get("is_replay") is True
        dispatch_result = _dispatch_outbound_response(dispatch_payload)
    except Exception as exc:
        LOGGER.exception(
            "duplicate notice dispatch failed: message_id=%s sender_phone=%s error=%s",
            envelope.message_id,
            envelope.sender_phone,
            exc,
        )
        return None

    dispatch_status = _dispatch_status(dispatch_result)
    generated_at = _clean_text(artifact["payload"].get("generated_at")) or _utc_timestamp()
    record_conversation_reply_event(
        report_date=generated_at[:10],
        branch=_clean_text(response_context.get("branch")),
        response_type="duplicate_notice",
        dispatch_status=dispatch_status,
        source_message_id=envelope.message_id,
        governance_status="duplicate",
        report_type=_clean_text(response_context.get("report_type")),
        replay_suppressed=envelope.replay.get("is_replay") is True and dispatch_status == "suppressed",
        reason=_clean_text(response_context.get("reason")),
        conversation_date=_clean_text(response_context.get("report_date")),
        output_root=REPO_ROOT,
    )
    return {
        "response_id": artifact["response_id"],
        "response_type": "duplicate_notice",
        "dispatch_status": dispatch_status,
        "json_path": _clean_text(dispatch_result.get("json_path")) or artifact["json_path"],
        "text_path": _clean_text(dispatch_result.get("text_path")) or artifact["text_path"],
    }


def _should_dispatch_policy_guard_duplicate_notice(
    *,
    processing_status: str,
    policy_guard: Mapping[str, Any],
) -> bool:
    """Return whether the explicit duplicate reply must be emitted."""

    return (
        processing_status == "duplicate"
        and policy_guard.get("fallback_eligible") is True
        and policy_guard.get("hard_reject") is True
    )


def _persist_duplicate_notice_artifact(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one persisted duplicate-notice artifact without clobbering sent replies."""

    normalized_payload = dict(payload)
    response_id = _response_artifact_id(normalized_payload)
    existing_artifact = load_response_artifact(response_id, output_root=REPO_ROOT)
    if existing_artifact is None:
        return write_response_artifacts(normalized_payload, output_root=REPO_ROOT)

    existing_payload = (
        existing_artifact.get("payload")
        if isinstance(existing_artifact.get("payload"), Mapping)
        else {}
    )
    existing_dispatch_status = _clean_text(existing_payload.get("dispatch_status"))
    existing_response_text = existing_payload.get("response_text")
    if existing_dispatch_status in {"sent", "duplicate"} or existing_response_text == normalized_payload["response_text"]:
        return existing_artifact

    normalized_payload["generated_at"] = _clean_text(existing_payload.get("generated_at")) or normalized_payload["generated_at"]
    return write_response_artifacts(
        normalized_payload,
        output_root=REPO_ROOT,
        overwrite=True,
    )


def _response_artifact_id(payload: Mapping[str, Any]) -> str:
    """Return the stable response artifact id used by the response store."""

    stable_fields = {
        "source_message_id": payload.get("source_message_id"),
        "sender_phone": payload.get("sender_phone"),
        "response_type": payload.get("response_type"),
        "governance_status": payload.get("governance_status"),
        "report_type": payload.get("report_type"),
        "branch": payload.get("branch"),
        "reason": payload.get("reason"),
    }
    digest = hashlib.sha256(
        json.dumps(stable_fields, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    return digest[:24]


def _read_raw_metadata(*, raw_meta_path: str | None) -> dict[str, Any]:
    """Return the latest raw metadata persisted by the bridge or orchestrator."""

    cleaned_path = _clean_text(raw_meta_path)
    if cleaned_path is None:
        return {}
    return _read_json_file(Path(cleaned_path))


def _dispatch_status(result: Mapping[str, Any]) -> str:
    """Return one normalized dispatch status from an outbound reply result."""

    status = _clean_text(result.get("dispatch_status")) or _clean_text(result.get("status"))
    if status in {"generated", "suppressed", "sent", "failed", "skipped", "dry_run", "duplicate"}:
        return status
    return "generated"


def _with_conversation_response_output(response: dict[str, Any]) -> dict[str, Any]:
    """Attach the persisted response artifact to the outward `outputs` list."""

    outputs = response.get("outputs")
    merged_outputs = list(outputs) if isinstance(outputs, list) else []

    conversation_response = response.get("conversation_response")
    if isinstance(conversation_response, Mapping):
        response_output_path = _clean_text(conversation_response.get("json_path")) or _clean_text(
            conversation_response.get("text_path")
        )
        if response_output_path is not None and response_output_path not in merged_outputs:
            merged_outputs.append(response_output_path)

    response["outputs"] = merged_outputs
    return response


def _outputs_from_result(result: AgentResult) -> list[str]:
    """Infer structured output paths from the downstream result when available."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    duplicate_handling = payload.get("duplicate_handling")
    if isinstance(duplicate_handling, Mapping) and duplicate_handling.get("write_suppressed") is True:
        return []
    explicit_outputs = payload.get("outputs")
    if isinstance(explicit_outputs, list) and all(isinstance(path, str) for path in explicit_outputs):
        return list(explicit_outputs)

    status = payload.get("status")
    if status in {"invalid_input", "rejected", "duplicate", "conflict_blocked"}:
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


def _bridge_duplicate_reason(reason: str) -> str:
    """Return one stable archive reason for bridge-level duplicate suppression."""

    mapping = {
        "message_id": "duplicate_message_id",
        "raw_sha256_and_received_at": "duplicate_raw_sha256",
    }
    return mapping.get(reason, reason)


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


def _collision_safe_raw_paths(*, raw_txt_path: Path, message_sha256: str) -> tuple[Path, Path]:
    """Return a stable fallback raw path when the primary hash path already exists."""

    alternate_txt_path = raw_txt_path.with_name(f"{raw_txt_path.stem}__{message_sha256[:12]}{raw_txt_path.suffix}")
    return alternate_txt_path, alternate_txt_path.with_suffix(".meta.json")


def _summarize_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one small summary of the inbound Meta payload shape for logging."""

    summary = {
        "object": _clean_text(payload.get("object")) or "unknown",
        "message_count": 0,
        "status_count": 0,
        "unsupported_change_fields": [],
        "unsupported_message_types": [],
    }
    entries = payload.get("entry")
    if not isinstance(entries, list):
        return summary

    unsupported_change_fields: set[str] = set()
    unsupported_message_types: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        changes = entry.get("changes")
        if not isinstance(changes, list):
            continue
        for change in changes:
            if not isinstance(change, Mapping):
                continue
            field_name = _clean_text(change.get("field")) or "unknown"
            value = change.get("value")
            if not isinstance(value, Mapping):
                unsupported_change_fields.add(field_name)
                continue
            messages = value.get("messages")
            statuses = value.get("statuses")
            saw_supported_block = False
            if isinstance(messages, list):
                saw_supported_block = True
                for message in messages:
                    if not isinstance(message, Mapping):
                        continue
                    summary["message_count"] += 1
                    if _extract_meta_text(message) is None:
                        unsupported_message_types.add(_clean_text(message.get("type")) or "unknown")
            if isinstance(statuses, list):
                saw_supported_block = True
                summary["status_count"] += len(statuses)
            if not saw_supported_block:
                unsupported_change_fields.add(field_name)

    summary["unsupported_change_fields"] = sorted(unsupported_change_fields)
    summary["unsupported_message_types"] = sorted(unsupported_message_types)
    return summary


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


def _record_pre_ingestion_validation(
    *,
    envelope: InboundMessageEnvelope,
    raw_record: dict[str, Any],
    result: dict[str, Any],
) -> None:
    """Persist one daily pre-ingestion validation observability event."""

    report_date = _validation_report_date(envelope.received_at)
    record_pre_ingestion_validation_event(
        report_date=report_date,
        received_at=envelope.received_at,
        message_id=envelope.message_id,
        payload_kind=envelope.payload_kind,
        result=result,
        raw_txt_path=raw_record.get("raw_txt_path"),
        raw_meta_path=raw_record.get("raw_meta_path"),
    )


def _write_pre_ingestion_validation_to_raw_meta(
    *,
    raw_meta_path: str | None,
    validation_result: Mapping[str, Any],
) -> None:
    """Merge the validator outcome into the existing raw metadata file."""

    if not isinstance(raw_meta_path, str) or not raw_meta_path.strip():
        return

    path = Path(raw_meta_path)
    payload = _read_json_file(path)
    if not payload:
        return
    payload["pre_ingestion_validation"] = dict(validation_result)
    write_json_file(path, payload)


def _validation_rejection_response(
    *,
    envelope: InboundMessageEnvelope,
    raw_record: dict[str, Any],
    raw_sha256: str,
    message_sha256: str,
    validation_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the bridge response for early validator rejection."""

    response = {
        "ok": True,
        "ingress": INGRESS_NAME,
        "workspace_root": str(REPO_ROOT),
        "raw_written": raw_record["raw_written"],
        "duplicate": False,
        "replay": False,
        "orchestrator_status": "skipped",
        "validator_status": validation_result.get("status"),
        "validator_reason_summary": _validation_reason_codes(validation_result),
        "route": None,
        "agent": None,
        "message_id": envelope.message_id,
        "message_sha256": message_sha256,
        "raw_sha256": raw_sha256,
        "raw_txt_path": raw_record.get("raw_txt_path"),
        "raw_meta_path": raw_record.get("raw_meta_path"),
        "outputs": [],
    }
    response["conversation_response"] = _generate_conversation_response(
        envelope=envelope,
        outcome={
            "status": validation_result.get("status"),
            "pre_ingestion_validation": dict(validation_result),
            "classification": {"report_type": "unknown"},
            "routing": {"classification": "unknown"},
        },
    )
    return _with_conversation_response_output(response)


def _command_response(
    *,
    envelope: InboundMessageEnvelope,
    raw_record: dict[str, Any],
    raw_sha256: str,
    message_sha256: str,
    command: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the bridge response for one recognized command message."""

    response = {
        "ok": True,
        "ingress": INGRESS_NAME,
        "workspace_root": str(REPO_ROOT),
        "raw_written": raw_record["raw_written"],
        "duplicate": False,
        "replay": envelope.replay.get("is_replay") is True,
        "command": True,
        "command_name": command.get("command_name"),
        "orchestrator_status": "skipped",
        "route": None,
        "agent": "command_handler",
        "message_id": envelope.message_id,
        "message_sha256": message_sha256,
        "raw_sha256": raw_sha256,
        "raw_txt_path": raw_record.get("raw_txt_path"),
        "raw_meta_path": raw_record.get("raw_meta_path"),
        "outputs": [],
    }
    response["conversation_response"] = _generate_command_response(command=command)
    return _with_conversation_response_output(response)


def _operational_query_response(
    *,
    envelope: InboundMessageEnvelope,
    raw_record: dict[str, Any],
    raw_sha256: str,
    message_sha256: str,
    intent: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the bridge response for one deterministic operational query."""

    response = {
        "ok": True,
        "ingress": INGRESS_NAME,
        "workspace_root": str(REPO_ROOT),
        "raw_written": raw_record["raw_written"],
        "duplicate": False,
        "replay": envelope.replay.get("is_replay") is True,
        "query": True,
        "query_type": intent.get("query_type"),
        "orchestrator_status": "skipped",
        "route": "operational_query",
        "agent": "taop_feedback",
        "message_id": envelope.message_id,
        "message_sha256": message_sha256,
        "raw_sha256": raw_sha256,
        "raw_txt_path": raw_record.get("raw_txt_path"),
        "raw_meta_path": raw_record.get("raw_meta_path"),
        "outputs": [],
    }
    response_context = build_operational_query_response(intent, Path(REPO_ROOT))
    response_context.update(
        {
            "channel": envelope.channel,
            "source_message_id": envelope.message_id,
            "sender_phone": envelope.sender_phone,
            "is_replay": envelope.replay.get("is_replay") is True,
        }
    )
    response["conversation_response"] = _generate_direct_response_context(response_context)
    return _with_conversation_response_output(response)


def _generate_conversation_response(
    *,
    envelope: InboundMessageEnvelope,
    outcome: AgentResult | Mapping[str, Any],
) -> dict[str, Any] | None:
    """Generate one auditable conversation reply without blocking the main flow."""

    try:
        return generate_whatsapp_conversation_reply(
            outcome=outcome,
            source_message_id=envelope.message_id,
            sender_phone=envelope.sender_phone,
            replay=envelope.replay.get("is_replay") is True,
            source_root=REPO_ROOT,
            dispatcher=_dispatch_outbound_response,
        )
    except Exception as exc:
        LOGGER.exception(
            "conversation response generation failed: message_id=%s sender_phone=%s error=%s",
            envelope.message_id,
            envelope.sender_phone,
            exc,
        )
        return None


def _generate_command_response(
    *,
    command: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Generate one auditable command reply without touching the report pipeline."""

    try:
        if _clean_text(command.get("command_name")) in {
            "approve",
            "reject",
            "replay",
            "list_proposals",
            "show_proposal",
            "approve_proposal",
            "reject_proposal",
            "simulate_proposal",
            "apply_proposal",
        }:
            response_context = handle_supervisor_command(command, output_root=REPO_ROOT)
        elif _clean_text(command.get("command_name")) == "ceo_query":
            response_context = handle_ceo_query(command, output_root=REPO_ROOT)
        elif _clean_text(command.get("command_name")) == "cross_branch_query":
            response_context = handle_cross_branch_query(command, output_root=REPO_ROOT)
        else:
            response_context = handle_whatsapp_command(command, output_root=REPO_ROOT)
        return dispatch_whatsapp_response(
            response_context=response_context,
            source_root=REPO_ROOT,
            dispatcher=_dispatch_outbound_response,
        )
    except Exception as exc:
        LOGGER.exception(
            "command response generation failed: message_id=%s sender_phone=%s error=%s",
            command.get("source_message_id"),
            command.get("sender_phone"),
            exc,
        )
        return None


def _generate_direct_response_context(
    response_context: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Persist and dispatch one deterministic response that bypasses report routing."""

    try:
        if response_context.get("is_replay") is not True:
            store_sender_interaction(
                sender_phone=_clean_text(response_context.get("sender_phone")),
                response_context=response_context,
                output_root=REPO_ROOT,
            )
        return dispatch_whatsapp_response(
            response_context=response_context,
            source_root=REPO_ROOT,
            dispatcher=_dispatch_outbound_response,
        )
    except Exception as exc:
        LOGGER.exception(
            "direct response generation failed: message_id=%s sender_phone=%s error=%s",
            response_context.get("source_message_id"),
            response_context.get("sender_phone"),
            exc,
        )
        return None


def _dispatch_outbound_response(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Send one persisted response artifact through the outbound reply agent."""

    return dispatch_outbound_reply(
        dict(payload),
        output_root=REPO_ROOT,
    )


def _result_with_outputs(
    result: AgentResult,
    *,
    raw_record: Mapping[str, Any] | None = None,
    raw_text: str | None = None,
    branch_hint: str | None = None,
) -> AgentResult:
    """Return one result copy augmented with inferred output paths for routing."""

    payload = dict(result.payload) if isinstance(result.payload, dict) else {}
    payload["outputs"] = _outputs_from_result(result)
    if isinstance(raw_record, Mapping) and "raw_record" not in payload:
        payload["raw_record"] = dict(raw_record)
    raw_message = payload.get("raw_message")
    if isinstance(raw_message, Mapping):
        normalized_raw_message = dict(raw_message)
    else:
        normalized_raw_message = {}
    if raw_text is not None and "text" not in normalized_raw_message:
        normalized_raw_message["text"] = raw_text
    if normalized_raw_message:
        payload["raw_message"] = normalized_raw_message
    if branch_hint is not None and "branch" not in payload and "branch_hint" not in payload:
        payload["branch_hint"] = branch_hint
    return AgentResult(
        agent_name=result.agent_name,
        payload=payload,
        metadata=dict(result.metadata) if isinstance(result.metadata, dict) else {},
    )


def _validation_reason_codes(validation_result: Mapping[str, Any]) -> list[str]:
    """Return stable reason codes from one validator payload."""

    reasons = validation_result.get("reasons")
    if not isinstance(reasons, list):
        return []
    codes: list[str] = []
    for reason in reasons:
        if not isinstance(reason, Mapping):
            continue
        code = reason.get("code")
        if isinstance(code, str) and code.strip():
            codes.append(code.strip())
    return codes


def _validation_report_date(received_at: str) -> str:
    """Return the daily artifact date segment for one received timestamp."""

    try:
        parsed = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    except ValueError:
        return "unknown"
    return parsed.date().isoformat()


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


def _mapping(value: object) -> Mapping[str, Any]:
    """Return one mapping payload or an empty mapping."""

    if isinstance(value, Mapping):
        return value
    return {}


def _text_value(value: object) -> str | None:
    """Return one string value without trimming text content."""

    if not isinstance(value, str):
        return None
    return value


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
    host = os.getenv("WHATSAPP_WEBHOOK_HOST", os.getenv("WHATSAPP_BRIDGE_HOST", DEFAULT_HOST))
    port = int(os.getenv("WHATSAPP_WEBHOOK_PORT", os.getenv("WHATSAPP_BRIDGE_PORT", os.getenv("PORT", str(DEFAULT_PORT)))))
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
