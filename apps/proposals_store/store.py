"""Deterministic storage helpers for optimization proposals."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any

import packages.record_store.paths as record_paths
from packages.proposal_store import write_proposal_record
from packages.record_store.writer import write_json_file


def write_optimization_proposal(
    payload: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> str:
    """Persist one optimization proposal under `records/proposals/`."""

    proposal = _normalized_payload(payload)
    if output_root is None:
        return write_proposal_record(
            generated_date=proposal["generated_date"],
            report_type=proposal["report_type"],
            proposal_type=proposal["proposal_type"],
            proposal_key=proposal["proposal_id"],
            payload=proposal,
        )

    proposal_path = _proposal_root(output_root) / proposal["generated_date"] / proposal["report_type"] / proposal["proposal_type"]
    target = proposal_path / f"{proposal['proposal_id']}.json"
    write_json_file(target, proposal)
    return str(target)


def load_proposal(
    proposal_id: str,
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any] | None:
    """Load one proposal payload by proposal id."""

    normalized_id = _required_text(proposal_id, field_name="proposal_id")
    for path in sorted(_proposal_root(output_root).rglob(f"{normalized_id}.json")):
        payload = _read_json(path)
        if payload is None:
            continue
        payload["proposal_path"] = str(path)
        return payload
    return None


def list_proposals(
    *,
    output_root: str | Path | None = None,
    branches: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Return visible proposals filtered by branch when provided."""

    allowed_branches = {branch for branch in branches or [] if isinstance(branch, str) and branch.strip()}
    items: list[dict[str, Any]] = []
    for path in sorted(_proposal_root(output_root).rglob("*.json")):
        payload = _read_json(path)
        if payload is None:
            continue
        branch = _text_or_none(payload.get("branch"))
        if allowed_branches and branch not in allowed_branches:
            continue
        payload["proposal_path"] = str(path)
        items.append(payload)
    items.sort(
        key=lambda item: (
            _text_or_none(item.get("generated_date")) or "",
            _text_or_none(item.get("branch")) or "",
            _text_or_none(item.get("proposal_id")) or "",
        ),
        reverse=True,
    )
    return items


def update_proposal(
    proposal_id: str,
    updates: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Update one persisted proposal in place and return the new payload."""

    existing = load_proposal(proposal_id, output_root=output_root)
    if existing is None:
        raise FileNotFoundError(f"proposal not found: {proposal_id}")
    proposal_path = Path(_required_text(existing.get("proposal_path"), field_name="proposal_path"))
    payload = deepcopy(existing)
    payload.pop("proposal_path", None)
    for key, value in updates.items():
        payload[key] = deepcopy(value)
    write_json_file(proposal_path, _normalized_payload(payload))
    updated = _read_json(proposal_path)
    if updated is None:
        raise ValueError(f"failed to reload proposal after update: {proposal_id}")
    updated["proposal_path"] = str(proposal_path)
    return updated


def _proposal_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.PROPOSALS_DIR
    return Path(output_root) / "records" / "proposals"


def _normalized_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("proposal payload must be a mapping")
    normalized = deepcopy(dict(payload))
    normalized["proposal_id"] = _required_text(normalized.get("proposal_id"), field_name="proposal_id")
    normalized["generated_date"] = _required_text(normalized.get("generated_date"), field_name="generated_date")
    normalized["proposal_type"] = _required_text(normalized.get("proposal_type"), field_name="proposal_type")
    normalized["report_type"] = _required_text(normalized.get("report_type"), field_name="report_type")
    normalized["branch"] = _required_text(normalized.get("branch"), field_name="branch")
    normalized["status"] = _required_text(normalized.get("status"), field_name="status")
    normalized["approval_status"] = _required_text(normalized.get("approval_status"), field_name="approval_status")
    normalized["apply_status"] = _required_text(normalized.get("apply_status"), field_name="apply_status")
    normalized["summary"] = _required_text(normalized.get("summary"), field_name="summary")
    proposed_action = normalized.get("proposed_action")
    if not isinstance(proposed_action, Mapping):
        raise ValueError("proposal field `proposed_action` must be a mapping")
    normalized["proposed_action"] = deepcopy(dict(proposed_action))
    source_paths = normalized.get("source_paths")
    normalized["source_paths"] = [item for item in source_paths if isinstance(item, str) and item.strip()] if isinstance(source_paths, list) else []
    evidence = normalized.get("evidence")
    normalized["evidence"] = deepcopy(dict(evidence)) if isinstance(evidence, Mapping) else {}
    return normalized


def _read_json(path: Path) -> dict[str, Any] | None:
    import json

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, dict) else None


def _required_text(value: object, *, field_name: str) -> str:
    text = _text_or_none(value)
    if text is None:
        raise ValueError(f"proposal field `{field_name}` must be a non-empty string")
    return text


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
