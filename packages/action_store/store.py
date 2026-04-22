"""Write deterministic autonomous action artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from collections.abc import Mapping

from packages.common.whatsapp_action_formatter import format_whatsapp_action_preview
from packages.record_store.paths import get_action_path, get_action_preview_path
from packages.record_store.writer import write_json_file, write_text_file


def write_action_record(
    action: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, str]:
    """Write one action JSON artifact and matching WhatsApp preview."""

    payload = dict(action)
    report_date = _required_text(payload.get("report_date"), field_name="report_date")
    branch = _required_text(payload.get("branch"), field_name="branch")
    action_type = _required_text(payload.get("action_type") or payload.get("rule_code"), field_name="action_type")
    action_id = _required_text(payload.get("action_id"), field_name="action_id")
    dedupe_key = _required_text(payload.get("dedupe_key"), field_name="dedupe_key")

    existing_path = _existing_action_path(
        report_date=report_date,
        branch=branch,
        action_type=action_type,
        dedupe_key=dedupe_key,
        output_root=output_root,
    )
    action_path = existing_path or get_action_path(
        report_date,
        branch,
        action_type,
        action_id,
        output_root=output_root,
    )
    preview_path = get_action_preview_path(
        report_date,
        branch,
        action_type,
        action_path.stem,
        output_root=output_root,
    )

    persisted_payload = dict(payload)
    persisted_payload["action_type"] = action_type
    persisted_payload["delivery_status"] = "pending_manual_dispatch"
    persisted_payload["source_paths"] = _string_list(persisted_payload.get("source_paths"))
    evidence = persisted_payload.get("evidence")
    persisted_payload["evidence"] = dict(evidence) if isinstance(evidence, Mapping) else {}

    write_json_file(action_path, persisted_payload)
    write_text_file(preview_path, f"{format_whatsapp_action_preview(persisted_payload)}\n")
    return {
        "status": "updated" if existing_path is not None else "created",
        "action_path": str(action_path),
        "preview_path": str(preview_path),
    }


def _existing_action_path(
    *,
    report_date: str,
    branch: str,
    action_type: str,
    dedupe_key: str,
    output_root: str | Path | None,
) -> Path | None:
    root = get_action_path(report_date, branch, action_type, "placeholder", output_root=output_root).parent
    if not root.exists():
        return None
    for candidate in sorted(root.glob("*.json")):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and payload.get("dedupe_key") == dedupe_key:
            return candidate
    return None


def _required_text(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"action field `{field_name}` must be a non-empty string")


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]
