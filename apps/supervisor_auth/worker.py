"""Supervisor authorization helpers for branch-scoped WhatsApp actions."""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from packages.branch_registry import canonical_branch_slug_or_none
from packages.common.paths import REPO_ROOT

_CONFIG_PATH = REPO_ROOT / "config" / "supervisors.json"


def load_supervisors() -> list[dict[str, Any]]:
    """Return the configured supervisor roster."""

    return list(_load_supervisors_cached())


def lookup_supervisor(*, sender_phone: str | None) -> dict[str, Any] | None:
    """Return one supervisor record by phone or `None` when missing."""

    normalized_phone = _text_or_none(sender_phone)
    if normalized_phone is None:
        return None
    for supervisor in _load_supervisors_cached():
        if supervisor["sender_phone"] == normalized_phone:
            return supervisor
    return None


def authorize_supervisor(
    *,
    sender_phone: str | None,
    branch: str | None,
) -> dict[str, Any]:
    """Return one deterministic authorization decision."""

    normalized_phone = _text_or_none(sender_phone)
    normalized_branch = _branch_or_none(branch)
    if normalized_phone is None or normalized_branch is None:
        return {
            "authorized": False,
            "reason": "missing_supervisor_identity",
            "supervisor": None,
            "branch": normalized_branch,
        }

    for supervisor in _load_supervisors_cached():
        if supervisor["sender_phone"] != normalized_phone:
            continue
        if normalized_branch not in supervisor["branches"]:
            return {
                "authorized": False,
                "reason": "branch_not_allowed",
                "supervisor": supervisor,
                "branch": normalized_branch,
            }
        return {
            "authorized": True,
            "reason": "authorized",
            "supervisor": supervisor,
            "branch": normalized_branch,
        }

    return {
        "authorized": False,
        "reason": "supervisor_not_found",
        "supervisor": None,
        "branch": normalized_branch,
    }


@lru_cache(maxsize=1)
def _load_supervisors_cached() -> tuple[dict[str, Any], ...]:
    try:
        payload = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ()
    supervisors = payload.get("supervisors")
    if not isinstance(supervisors, list):
        return ()

    normalized: list[dict[str, Any]] = []
    for item in supervisors:
        if not isinstance(item, dict):
            continue
        phone = _text_or_none(item.get("sender_phone"))
        if phone is None:
            continue
        branches_value = item.get("branches")
        if not isinstance(branches_value, list):
            continue
        branches = sorted(
            {
                branch
                for branch in (_branch_or_none(value) for value in branches_value)
                if branch is not None
            }
        )
        if not branches:
            continue
        normalized.append(
            {
                "name": _text_or_none(item.get("name")),
                "sender_phone": phone,
                "branches": branches,
            }
        )
    return tuple(normalized)


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
