"""Safe file writers for record storage."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .paths import (
    get_raw_path,
    get_rejected_path,
    get_structured_path,
    get_structured_path_for_root,
)


def ensure_directory(path: Path) -> Path:
    """Create a directory path if it does not already exist."""

    path.mkdir(parents=True, exist_ok=True)
    return path


def _atomic_write_text(path: Path, content: str) -> Path:
    ensure_directory(path.parent)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    os.replace(temporary_path, path)
    return path


def write_json_file(path: Path, payload: dict[str, Any]) -> Path:
    """Write JSON content deterministically and atomically."""

    content = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)
    return _atomic_write_text(path, f"{content}\n")


def write_json_sidecar(path: Path, suffix: str, payload: dict[str, Any]) -> Path:
    """Write one JSON sidecar file beside a canonical primary path."""

    sidecar_path = path.with_suffix(suffix)
    return write_json_file(sidecar_path, payload)


def write_text_file(path: Path, content: str) -> Path:
    """Write plain text content atomically."""

    return _atomic_write_text(path, content)


def write_structured(
    signal_type: str,
    branch: str,
    date: str,
    payload: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    *,
    root: str | Path | None = None,
    colony_root: str | Path | None = None,
) -> Path:
    """Write one trusted structured JSON record and refresh downstream automation."""

    explicit_root = Path(root) if root is not None else None
    structured_path = (
        get_structured_path(signal_type, branch, date)
        if explicit_root is None
        else get_structured_path_for_root(
            explicit_root / "records" / "structured",
            signal_type=signal_type,
            branch=branch,
            date=date,
        )
    )
    source_root = explicit_root if explicit_root is not None else structured_path.parents[4]
    persisted_payload = dict(payload)
    persisted_payload["status"] = _normalized_direct_write_status(persisted_payload)
    persisted_payload["export_allowed"] = persisted_payload["status"] in {"accepted", "accepted_with_warning"}
    written_path = write_json_file(structured_path, persisted_payload)
    if isinstance(metadata, dict) and metadata:
        write_json_sidecar(written_path, ".validation.json", metadata)

    from packages.data_governance.layer import GovernanceDecision, write_governance_sidecar

    governance = GovernanceDecision(
        status=persisted_payload["status"],
        export_allowed=persisted_payload["export_allowed"],
        report_family=signal_type,
        signal_type=signal_type,
        branch=branch,
        report_date=date,
        message_id=None,
        raw_sha256=None,
        normalized_scope=f"{signal_type}:{branch}:{date}",
        semantic_sha256=_direct_write_semantic_sha(signal_type=signal_type, branch=branch, date=date, payload=persisted_payload),
        reasons=[],
        warnings=[
            str(warning.get("code"))
            for warning in persisted_payload.get("warnings", [])
            if isinstance(warning, dict) and isinstance(warning.get("code"), str)
        ],
        source_status=str(persisted_payload["status"]),
    )
    persisted_payload["governance"] = governance.to_payload()
    written_path = write_json_file(written_path, persisted_payload)
    write_governance_sidecar(written_path, governance)

    from .automation import log_post_write_failure, run_post_write_automation

    try:
        run_post_write_automation(
            signal_type,
            branch,
            date,
            source_root=source_root,
            colony_root=colony_root,
        )
    except Exception as error:
        log_post_write_failure(
            signal_type=signal_type,
            branch=branch,
            report_date=date,
            structured_path=written_path,
            error=error,
        )
    return written_path


def write_governed_structured(
    signal_type: str,
    branch: str,
    date: str,
    payload: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    *,
    root: str | Path | None = None,
    colony_root: str | Path | None = None,
):
    """Write one structured record after applying the governance layer."""

    explicit_root = Path(root) if root is not None else None
    structured_path = (
        get_structured_path(signal_type, branch, date)
        if explicit_root is None
        else get_structured_path_for_root(
            explicit_root / "records" / "structured",
            signal_type=signal_type,
            branch=branch,
            date=date,
        )
    )
    source_root = explicit_root if explicit_root is not None else structured_path.parents[4]
    governance_metadata = dict(metadata) if isinstance(metadata, dict) else {}

    from packages.data_governance import govern_record
    from packages.data_governance.layer import GovernedWriteResult, write_governance_sidecar

    decision = govern_record(
        signal_type=signal_type,
        branch=branch,
        report_date=date,
        payload=payload,
        metadata=governance_metadata,
        structured_path=structured_path,
        source_root=source_root,
    )

    persisted = decision.status not in {"rejected", "duplicate", "conflict_blocked"}
    written_path = structured_path
    if persisted:
        persisted_payload = dict(payload)
        persisted_payload["status"] = decision.status
        persisted_payload["export_allowed"] = decision.export_allowed
        persisted_payload["governance"] = decision.to_payload()
        written_path = write_json_file(structured_path, persisted_payload)
        if governance_metadata:
            write_json_sidecar(written_path, ".validation.json", governance_metadata)
        write_governance_sidecar(written_path, decision)

    from .automation import log_post_write_failure, run_post_write_automation

    if persisted:
        try:
            run_post_write_automation(
                signal_type,
                branch,
                date,
                source_root=source_root,
                colony_root=colony_root,
            )
        except Exception as error:
            log_post_write_failure(
                signal_type=signal_type,
                branch=branch,
                report_date=date,
                structured_path=written_path,
                error=error,
            )
    return GovernedWriteResult(path=written_path, persisted=persisted, governance=decision)


def write_raw(report_type: str, filename: str, text: str) -> Path:
    """Write one raw WhatsApp report under its canonical base directory."""

    return write_text_file(get_raw_path(report_type) / filename, text)


def write_rejected(report_type: str, filename: str, text: str) -> Path:
    """Write one rejected report under its canonical base directory."""

    return write_text_file(get_rejected_path(report_type) / filename, text)


def _normalized_direct_write_status(payload: dict[str, Any]) -> str:
    """Normalize direct-write payload statuses onto the final status contract."""

    status = payload.get("status")
    if status == "ready":
        return "accepted"
    if isinstance(status, str) and status in {"accepted", "accepted_with_warning", "needs_review", "rejected"}:
        return status
    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        return "accepted_with_warning"
    return "accepted"


def _direct_write_semantic_sha(*, signal_type: str, branch: str, date: str, payload: dict[str, Any]) -> str:
    """Return a stable semantic hash for direct overwrite writes."""

    serialized = json.dumps(
        {
            "signal_type": signal_type,
            "branch": branch,
            "report_date": date,
            "payload": payload,
        },
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    import hashlib

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
