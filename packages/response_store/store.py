"""Persist deterministic WhatsApp response artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from packages.record_store.writer import ensure_directory, write_json_file, write_text_file


def write_response_artifacts(
    payload: dict[str, Any],
    *,
    output_root: str | Path | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Write one outbound WhatsApp response JSON artifact and matching text file."""

    artifact = _normalized_payload(payload)
    response_id = _response_id(artifact)
    generated_at = artifact["generated_at"]
    artifact["response_id"] = response_id

    base_dir = _responses_dir(output_root=output_root) / generated_at[:10]
    ensure_directory(base_dir)
    json_path = base_dir / f"{response_id}.json"
    text_path = base_dir / f"{response_id}.txt"
    text_content = f"{artifact['response_text']}\n"

    if json_path.exists() or text_path.exists():
        if overwrite:
            pass
        elif _artifacts_match(json_path=json_path, text_path=text_path, payload=artifact, text_content=text_content):
            return {
                "response_id": response_id,
                "json_path": str(json_path),
                "text_path": str(text_path),
                "payload": artifact,
            }
        else:
            raise FileExistsError(f"Response artifact already exists with different content: {json_path}")

    write_json_file(json_path, artifact)
    write_text_file(text_path, text_content)
    return {
        "response_id": response_id,
        "json_path": str(json_path),
        "text_path": str(text_path),
        "payload": artifact,
    }


def load_response_artifact(
    response_id: str,
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Load one persisted response artifact by deterministic response id."""

    cleaned_response_id = _required_text(response_id, field_name="response_id")
    base_dir = _responses_dir(output_root=output_root)
    for json_path in sorted(base_dir.glob(f"*/{cleaned_response_id}.json")):
        text_path = json_path.with_suffix(".txt")
        if not text_path.exists():
            continue
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        return {
            "response_id": cleaned_response_id,
            "json_path": str(json_path),
            "text_path": str(text_path),
            "payload": payload,
        }
    return None


def update_response_artifact_dispatch(
    response_id: str,
    *,
    dispatch_status: str,
    provider_message_id: str | None = None,
    dispatch_error: str | None = None,
    http_status: int | str | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Update dispatch metadata for one persisted response artifact."""

    existing = load_response_artifact(response_id, output_root=output_root)
    if existing is None:
        raise FileNotFoundError(f"Response artifact not found for update: {response_id}")

    payload = existing.get("payload")
    if not isinstance(payload, dict):
        raise ValueError(f"Response artifact payload is invalid: {response_id}")

    updated_payload = dict(payload)
    updated_payload["dispatch_status"] = _required_text(dispatch_status, field_name="dispatch_status")
    updated_payload["provider_message_id"] = _string_or_none(provider_message_id)
    updated_payload["dispatch_error"] = _string_or_none(dispatch_error)
    updated_payload["http_status"] = _int_or_none(http_status)
    return write_response_artifacts(updated_payload, output_root=output_root, overwrite=True)


def _normalized_payload(payload: dict[str, Any]) -> dict[str, Any]:
    generated_at = _required_text(payload.get("generated_at"), field_name="generated_at")
    response_text = _required_text(payload.get("response_text"), field_name="response_text")
    response_type = _required_text(payload.get("response_type"), field_name="response_type")
    dispatch_status = _required_text(payload.get("dispatch_status"), field_name="dispatch_status")

    return {
        "source_message_id": _string_or_none(payload.get("source_message_id")),
        "sender_phone": _string_or_none(payload.get("sender_phone")),
        "response_type": response_type,
        "governance_status": _string_or_none(payload.get("governance_status")),
        "report_type": _string_or_none(payload.get("report_type")),
        "branch": _string_or_none(payload.get("branch")),
        "report_date": _string_or_none(payload.get("report_date")),
        "reason": _string_or_none(payload.get("reason")),
        "generated_at": generated_at,
        "response_text": response_text,
        "dispatch_status": dispatch_status,
        "provider_message_id": _string_or_none(payload.get("provider_message_id")),
        "dispatch_error": _string_or_none(payload.get("dispatch_error") or payload.get("error")),
        "http_status": _int_or_none(payload.get("http_status")),
        "feedback": _feedback_or_none(payload.get("feedback")),
    }


def _response_id(payload: dict[str, Any]) -> str:
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


def _responses_dir(*, output_root: str | Path | None) -> Path:
    root = Path(output_root) if output_root is not None else Path.cwd()
    return root / "records" / "responses" / "whatsapp"


def _artifacts_match(
    *,
    json_path: Path,
    text_path: Path,
    payload: dict[str, Any],
    text_content: str,
) -> bool:
    try:
        existing_json = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    try:
        existing_text = text_path.read_text(encoding="utf-8")
    except OSError:
        return False
    return existing_json == payload and existing_text == text_content


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _required_text(value: object, *, field_name: str) -> str:
    cleaned = _string_or_none(value)
    if cleaned is None:
        raise ValueError(f"Response artifact field `{field_name}` must be a non-empty string.")
    return cleaned


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


def _feedback_or_none(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    try:
        normalized = json.loads(json.dumps(value, ensure_ascii=True))
    except (TypeError, ValueError):
        return None
    return normalized if isinstance(normalized, dict) else None
