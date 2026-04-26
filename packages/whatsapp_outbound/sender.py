"""Controlled outbound WhatsApp text sender."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib import error, request

from packages.common.paths import REPO_ROOT
from packages.response_store import load_response_artifact

WHATSAPP_OUTBOUND_MODE_ENV_VAR = "WHATSAPP_OUTBOUND_MODE"
WHATSAPP_GRAPH_API_VERSION_ENV_VAR = "WHATSAPP_GRAPH_API_VERSION"
WHATSAPP_PHONE_NUMBER_ID_ENV_VAR = "WHATSAPP_PHONE_NUMBER_ID"
WHATSAPP_ACCESS_TOKEN_ENV_VAR = "WHATSAPP_ACCESS_TOKEN"
WHATSAPP_OUTBOUND_ALLOWLIST_ENV_VAR = "WHATSAPP_OUTBOUND_ALLOWLIST"
DEFAULT_GRAPH_API_VERSION = "v25.0"
SEND_TIMEOUT_SECONDS = 5


def send_whatsapp_text(
    *,
    to: str,
    body: str,
    source_message_id: str,
    response_id: str,
    response_type: str,
    is_replay: bool,
) -> dict[str, Any]:
    """Send one plain-text reply through the Meta WhatsApp Cloud API."""

    recipient = _clean_text(to)
    message_body = _clean_text(body)
    cleaned_response_id = _clean_text(response_id)
    cleaned_response_type = _clean_text(response_type)

    if recipient is None or message_body is None or cleaned_response_id is None or cleaned_response_type is None:
        return {
            "dispatch_status": "failed",
            "provider_message_id": None,
            "error": "invalid_dispatch_payload",
            "http_status": None,
        }

    existing = _existing_response_artifact(
        response_id=cleaned_response_id,
        source_message_id=source_message_id,
        response_type=cleaned_response_type,
    )
    if existing is not None:
        existing_payload = existing.get("payload")
        if isinstance(existing_payload, dict):
            existing_status = _clean_text(existing_payload.get("dispatch_status"))
            if existing_status in {"sent", "duplicate"}:
                return {
                    "dispatch_status": "duplicate",
                    "provider_message_id": _clean_text(existing_payload.get("provider_message_id")),
                    "error": None,
                    "http_status": _int_or_none(existing_payload.get("http_status")),
                }

    if is_replay:
        return {
            "dispatch_status": "suppressed",
            "provider_message_id": None,
            "error": None,
            "http_status": None,
        }

    mode = _outbound_mode()
    if mode == "off":
        return {
            "dispatch_status": "suppressed",
            "provider_message_id": None,
            "error": None,
            "http_status": None,
        }
    if mode == "dry_run":
        return {
            "dispatch_status": "dry_run",
            "provider_message_id": None,
            "error": None,
            "http_status": None,
        }

    if not _recipient_allowlisted(recipient):
        return {
            "dispatch_status": "suppressed",
            "provider_message_id": None,
            "error": "recipient_not_allowlisted",
            "http_status": None,
        }

    phone_number_id = _clean_text(os.environ.get(WHATSAPP_PHONE_NUMBER_ID_ENV_VAR))
    access_token = _clean_text(os.environ.get(WHATSAPP_ACCESS_TOKEN_ENV_VAR))
    if phone_number_id is None or access_token is None:
        return {
            "dispatch_status": "failed",
            "provider_message_id": None,
            "error": "missing_live_configuration",
            "http_status": None,
        }

    api_version = _clean_text(os.environ.get(WHATSAPP_GRAPH_API_VERSION_ENV_VAR)) or DEFAULT_GRAPH_API_VERSION
    endpoint = f"https://graph.facebook.com/{api_version}/{phone_number_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message_body,
        },
    }
    http_request = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=SEND_TIMEOUT_SECONDS) as http_response:
            raw_body = http_response.read().decode("utf-8")
            parsed = json.loads(raw_body) if raw_body else {}
            provider_message_id = _provider_message_id(parsed)
            return {
                "dispatch_status": "sent",
                "provider_message_id": provider_message_id,
                "error": None,
                "http_status": int(http_response.getcode()),
            }
    except error.HTTPError as exc:
        error_body = _read_error_body(exc)
        return {
            "dispatch_status": "failed",
            "provider_message_id": None,
            "error": error_body or str(exc),
            "http_status": int(exc.code),
        }
    except (error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return {
            "dispatch_status": "failed",
            "provider_message_id": None,
            "error": str(exc),
            "http_status": None,
        }


def _outbound_mode() -> str:
    value = _clean_text(os.environ.get(WHATSAPP_OUTBOUND_MODE_ENV_VAR)) or "off"
    normalized = value.casefold()
    if normalized in {"off", "dry_run", "live"}:
        return normalized
    return "off"


def _existing_response_artifact(
    *,
    response_id: str,
    source_message_id: str,
    response_type: str,
) -> dict[str, Any] | None:
    exact = load_response_artifact(response_id, output_root=REPO_ROOT)
    if exact is not None:
        return exact

    base_dir = Path(REPO_ROOT) / "records" / "responses" / "whatsapp"
    for json_path in sorted(base_dir.glob("*/*.json")):
        text_path = json_path.with_suffix(".txt")
        if not text_path.exists():
            continue
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if _clean_text(payload.get("source_message_id")) != source_message_id:
            continue
        if _clean_text(payload.get("response_type")) != response_type:
            continue
        return {
            "response_id": json_path.stem,
            "json_path": str(json_path),
            "text_path": str(text_path),
            "payload": payload,
        }
    return None


def _recipient_allowlisted(recipient: str) -> bool:
    allowlist_value = os.environ.get(WHATSAPP_OUTBOUND_ALLOWLIST_ENV_VAR, "")
    allowed = {
        value.strip()
        for value in allowlist_value.split(",")
        if value.strip()
    }
    return recipient in allowed


def _provider_message_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        return None
    first = messages[0]
    if not isinstance(first, dict):
        return None
    return _clean_text(first.get("id"))


def _read_error_body(exc: error.HTTPError) -> str | None:
    try:
        raw = exc.read().decode("utf-8")
    except OSError:
        return None
    cleaned = raw.strip()
    return cleaned or None


def _clean_text(value: object) -> str | None:
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
