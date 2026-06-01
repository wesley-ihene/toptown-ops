"""Safe file writers for record storage."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any

from packages.branch_registry import canonical_branch_slug_or_none
from packages.common.paths import REPO_ROOT

from .duplicate_archive import archive_duplicate_record
from .paths import (
    get_raw_path,
    get_rejected_path,
    get_structured_path,
    get_structured_path_for_root,
)

LOGGER = logging.getLogger(__name__)


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

    canonical_branch = _require_canonical_branch_for_write(branch)
    explicit_root = Path(root) if root is not None else None
    structured_path = (
        get_structured_path(signal_type, canonical_branch, date)
        if explicit_root is None
        else get_structured_path_for_root(
            explicit_root / "records" / "structured",
            signal_type=signal_type,
            branch=canonical_branch,
            date=date,
        )
    )
    source_root = explicit_root if explicit_root is not None else structured_path.parents[4]
    persisted_payload = _normalized_persisted_payload(payload, canonical_branch=canonical_branch)
    persisted_payload["status"] = _normalized_direct_write_status(persisted_payload)
    if signal_type == "supervisor_control":
        persisted_payload["status"] = "accepted"
    persisted_payload["export_allowed"] = persisted_payload["status"] in {"accepted", "accepted_with_warning"}
    written_path = write_json_file(structured_path, persisted_payload)
    if isinstance(metadata, dict) and metadata:
        write_json_sidecar(written_path, ".validation.json", metadata)

    from packages.data_governance.layer import GovernanceDecision, write_governance_sidecar

    governance = GovernanceDecision(
        status=persisted_payload["status"],
        export_allowed=persisted_payload["export_allowed"],
        report_family="intelligence" if signal_type == "supervisor_control" else signal_type,
        signal_type=signal_type,
        branch=canonical_branch,
        report_date=date,
        message_id=None,
        raw_sha256=None,
        normalized_scope=f"{signal_type}:{canonical_branch}:{date}",
        semantic_sha256=_direct_write_semantic_sha(
            signal_type=signal_type,
            branch=canonical_branch,
            date=date,
            payload=persisted_payload,
        ),
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
    if signal_type == "supervisor_control" and governance.status == "accepted":
        _log_intelligence_event(
            "intelligence_report_accepted",
            branch=canonical_branch,
            report_date=date,
            path=str(written_path),
            report_family=governance.report_family,
        )

    from .automation import log_post_write_failure, run_post_write_automation

    try:
        run_post_write_automation(
            signal_type,
            canonical_branch,
            date,
            source_root=source_root,
            colony_root=colony_root,
        )
    except Exception as error:
        log_post_write_failure(
            signal_type=signal_type,
            branch=canonical_branch,
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
    governance_metadata = metadata if isinstance(metadata, dict) else dict(metadata) if isinstance(metadata, Mapping) else {}
    canonical_branch = canonical_branch_slug_or_none(branch)
    if canonical_branch is None:
        _annotate_unknown_branch_metadata(governance_metadata, raw_branch=branch)
        return _unknown_branch_write_result(
            signal_type=signal_type,
            branch=branch,
            date=date,
            payload=payload,
            root=explicit_root,
        )

    structured_path = (
        get_structured_path(signal_type, canonical_branch, date)
        if explicit_root is None
        else get_structured_path_for_root(
            explicit_root / "records" / "structured",
            signal_type=signal_type,
            branch=canonical_branch,
            date=date,
        )
    )
    source_root = explicit_root if explicit_root is not None else structured_path.parents[4]

    from packages.data_governance import govern_record
    from packages.data_governance.layer import GovernedWriteResult, write_governance_sidecar

    decision = govern_record(
        signal_type=signal_type,
        branch=canonical_branch,
        report_date=date,
        payload=_normalized_persisted_payload(payload, canonical_branch=canonical_branch),
        metadata=governance_metadata,
        structured_path=structured_path,
        source_root=source_root,
    )

    governance_context = governance_metadata.get("governance_context") if isinstance(governance_metadata.get("governance_context"), Mapping) else {}
    replacement_requested = _replacement_supersede_requested(
        signal_type=signal_type,
        governance_context=governance_context,
        decision=decision,
    )
    persisted = decision.status not in {"rejected", "duplicate", "conflict_blocked"}
    if replacement_requested and decision.status not in {"accepted", "accepted_with_warning"}:
        persisted = False
    written_path = structured_path
    if persisted:
        persisted_payload = _normalized_persisted_payload(payload, canonical_branch=canonical_branch)
        replacement_metadata = None
        if replacement_requested:
            replacement_metadata = _archive_superseded_record(
                existing_record_path=Path(str(decision.duplicate_of)),
                active_record_path=structured_path,
                signal_type=signal_type,
                branch=canonical_branch,
                report_date=date,
                source_root=source_root,
                governance_context=governance_context,
            )
            persisted_payload.update(replacement_metadata)
        persisted_payload["status"] = decision.status
        persisted_payload["export_allowed"] = decision.export_allowed
        persisted_payload["governance"] = decision.to_payload()
        written_path = write_json_file(structured_path, persisted_payload)
        metadata_to_write = dict(governance_metadata) if governance_metadata else {}
        if replacement_metadata is not None:
            metadata_to_write["replacement"] = dict(replacement_metadata)
        if metadata_to_write:
            write_json_sidecar(written_path, ".validation.json", metadata_to_write)
        write_governance_sidecar(written_path, decision)
        if signal_type == "supervisor_control" and decision.status == "accepted":
            _log_intelligence_event(
                "intelligence_report_accepted",
                branch=canonical_branch,
                report_date=date,
                path=str(written_path),
                report_family=decision.report_family,
            )
    elif decision.status == "duplicate":
        archive_duplicate_record(
            source_message_id=_string_or_none(governance_context.get("message_id")),
            sender_phone=_string_or_none(governance_context.get("sender_phone")),
            branch=canonical_branch,
            report_type=_string_or_none(governance_context.get("classified_report_type")) or decision.report_family,
            report_date=date,
            raw_txt_path=_string_or_none(governance_context.get("raw_txt_path")),
            raw_meta_path=_string_or_none(governance_context.get("raw_meta_path")),
            duplicate_reason=decision.reasons[0] if decision.reasons else "duplicate",
            duplicate_basis=_duplicate_archive_basis(decision=decision, governance_context=governance_context),
            original_or_duplicate_of=decision.duplicate_of,
            output_root=source_root,
        )

    from .automation import log_post_write_failure, run_post_write_automation

    if persisted:
        try:
            run_post_write_automation(
                signal_type,
                canonical_branch,
                date,
                source_root=source_root,
                colony_root=colony_root,
            )
        except Exception as error:
            log_post_write_failure(
                signal_type=signal_type,
                branch=canonical_branch,
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


def _normalized_persisted_payload(payload: Mapping[str, Any], *, canonical_branch: str) -> dict[str, Any]:
    """Return one persisted payload with canonical branch fields only."""

    persisted_payload = dict(payload)
    raw_branch = _string_or_none(persisted_payload.get("branch"))
    if raw_branch is not None and raw_branch != canonical_branch:
        provenance = persisted_payload.get("provenance")
        if isinstance(provenance, Mapping):
            updated_provenance = dict(provenance)
            if _string_or_none(updated_provenance.get("raw_branch")) is None and _string_or_none(updated_provenance.get("branch_text")) is None:
                updated_provenance["raw_branch"] = raw_branch
            persisted_payload["provenance"] = updated_provenance

    if "branch" in persisted_payload or raw_branch is not None:
        persisted_payload["branch"] = canonical_branch
    if isinstance(persisted_payload.get("branch_slug"), str):
        persisted_payload["branch_slug"] = canonical_branch
    return persisted_payload


def _require_canonical_branch_for_write(branch: str) -> str:
    """Return one canonical branch slug or raise for invalid direct writes."""

    canonical_branch = canonical_branch_slug_or_none(branch)
    if canonical_branch is None:
        raise ValueError(f"unknown_branch_slug: {branch!r}")
    return canonical_branch


def _unknown_branch_write_result(
    *,
    signal_type: str,
    branch: str,
    date: str,
    payload: Mapping[str, Any],
    root: Path | None,
):
    """Return one non-persisted governance result for an invalid branch slug."""

    from packages.data_governance.layer import GovernanceDecision, GovernedWriteResult

    source_root = root if root is not None else REPO_ROOT
    if signal_type == "supervisor_control":
        preview_path = source_root / "records" / "intelligence" / signal_type / date / "unknown.json"
    else:
        preview_path = source_root / "records" / "structured" / signal_type / "unknown" / f"{date}.json"
    decision = GovernanceDecision(
        status="needs_review",
        export_allowed=False,
        report_family=_report_family_for_signal_type(signal_type),
        signal_type=signal_type,
        branch=_string_or_none(branch),
        report_date=_string_or_none(date),
        message_id=None,
        raw_sha256=None,
        normalized_scope=None,
        semantic_sha256=None,
        reasons=["unknown_branch_slug"],
        warnings=_warning_codes(payload),
        source_status=_string_or_none(payload.get("status")),
    )
    return GovernedWriteResult(path=preview_path, persisted=False, governance=decision)


def _report_family_for_signal_type(signal_type: str) -> str:
    """Return the governance report-family label for one signal type."""

    return {
        "sales_income": "sales",
        "pricing_stock_release": "bale_summary",
        "hr_attendance": "staff_attendance",
        "hr_performance": "staff_performance",
        "supervisor_control": "intelligence",
    }.get(signal_type, signal_type)


def _annotate_unknown_branch_metadata(metadata: dict[str, Any], *, raw_branch: str) -> None:
    """Annotate validation metadata with one explicit unknown-branch rejection."""

    validation = metadata.get("validation")
    validation_payload = dict(validation) if isinstance(validation, Mapping) else {}
    reason_code = "unknown_branch_slug"
    reason_detail = "Branch slug is not configured and cannot be persisted safely."

    reason_codes = [
        code
        for code in validation_payload.get("reason_codes", [])
        if isinstance(code, str) and code.strip()
    ]
    if reason_code not in reason_codes:
        reason_codes.append(reason_code)

    rejections = [
        dict(entry)
        for entry in validation_payload.get("rejections", [])
        if isinstance(entry, Mapping)
    ]
    if not any(entry.get("reason_code") == reason_code for entry in rejections):
        rejections.append(
            {
                "reason_code": reason_code,
                "reason_detail": reason_detail,
                "code": reason_code,
                "message": reason_detail,
                "field": "branch",
                "raw_branch": raw_branch,
            }
        )

    details = dict(validation_payload.get("details")) if isinstance(validation_payload.get("details"), Mapping) else {}
    details["final_status"] = "needs_review"
    details["validation_error_code"] = reason_code
    details["validation_error_message"] = reason_detail
    details["raw_branch"] = raw_branch

    validation_payload["status"] = "rejected"
    validation_payload["accepted"] = False
    validation_payload["reason_codes"] = reason_codes
    validation_payload["rejections"] = rejections
    validation_payload["details"] = details
    metadata["validation"] = validation_payload


def _warning_codes(payload: Mapping[str, Any]) -> list[str]:
    """Return warning codes carried by one candidate payload."""

    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        return []
    codes: list[str] = []
    for warning in warnings:
        if isinstance(warning, Mapping):
            code = _string_or_none(warning.get("code"))
            if code is not None:
                codes.append(code)
    return codes


def _replacement_supersede_requested(
    *,
    signal_type: str,
    governance_context: Mapping[str, Any],
    decision: Any,
) -> bool:
    duplicate_of = getattr(decision, "duplicate_of", None)
    return (
        signal_type == "pricing_stock_release"
        and governance_context.get("correction_intent_detected") is True
        and governance_context.get("allow_same_scope_supersede") is True
        and isinstance(duplicate_of, str)
        and duplicate_of.strip() != ""
    )


def _archive_superseded_record(
    *,
    existing_record_path: Path,
    active_record_path: Path,
    signal_type: str,
    branch: str,
    report_date: str,
    source_root: Path,
    governance_context: Mapping[str, Any],
) -> dict[str, Any]:
    corrected_at = _utc_now_iso()
    replacement_reason = _string_or_none(governance_context.get("replacement_reason")) or "correction_replacement_report"
    replacement_source_message_id = _string_or_none(governance_context.get("message_id"))
    audit_dir = source_root / "records" / "audit" / "superseded_records" / signal_type / branch / report_date
    audit_path = audit_dir / f"{_timestamp_segment(corrected_at)}__superseded.json"

    archived_payload = _read_json_file(existing_record_path)
    archived_payload.update(
        {
            "superseded": True,
            "replacement_reason": replacement_reason,
            "superseded_at": corrected_at,
            "corrected_at": corrected_at,
            "replacement_source_message_id": replacement_source_message_id,
            "superseded_by_record_path": str(active_record_path),
        }
    )
    write_json_file(audit_path, archived_payload)
    _archive_sidecar_copy(
        source_path=existing_record_path.with_suffix(".validation.json"),
        target_path=audit_path.with_suffix(".validation.json"),
        extra_fields={
            "replacement_reason": replacement_reason,
            "superseded_at": corrected_at,
            "corrected_at": corrected_at,
            "replacement_source_message_id": replacement_source_message_id,
            "superseded_by_record_path": str(active_record_path),
        },
    )
    _archive_sidecar_copy(
        source_path=existing_record_path.with_suffix(".governance.json"),
        target_path=audit_path.with_suffix(".governance.json"),
        extra_fields={
            "replacement_reason": replacement_reason,
            "superseded_at": corrected_at,
            "corrected_at": corrected_at,
            "replacement_source_message_id": replacement_source_message_id,
            "superseded_by_record_path": str(active_record_path),
        },
    )
    return {
        "replacement_reason": replacement_reason,
        "supersedes_record_path": str(audit_path),
        "superseded_at": corrected_at,
        "corrected_at": corrected_at,
        "replacement_source_message_id": replacement_source_message_id,
    }


def _archive_sidecar_copy(
    *,
    source_path: Path,
    target_path: Path,
    extra_fields: Mapping[str, Any],
) -> None:
    if not source_path.exists():
        return
    payload = _read_json_file(source_path)
    payload.update(extra_fields)
    write_json_file(target_path, payload)


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _timestamp_segment(timestamp: str) -> str:
    return (
        timestamp.replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
        .replace(".", "")
    )


def _duplicate_archive_basis(*, decision, governance_context: Mapping[str, Any]) -> str:
    """Return one stable duplicate basis for disposal archive records."""

    reason = decision.reasons[0] if decision.reasons else "duplicate"
    if reason == "duplicate_message_id" and decision.message_id:
        return f"message_id:{decision.message_id}"
    if reason == "duplicate_raw_sha256" and decision.raw_sha256:
        return f"raw_sha256:{decision.raw_sha256}"
    if reason == "duplicate_semantic" and decision.semantic_sha256:
        return f"semantic_sha256:{decision.semantic_sha256}"
    candidate = _string_or_none(governance_context.get("duplicate_basis"))
    if candidate is not None:
        return candidate
    if decision.duplicate_of:
        return f"reference:{decision.duplicate_of}"
    return reason


def _string_or_none(value: object) -> str | None:
    """Return one stripped string or ``None``."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _log_intelligence_event(event: str, **fields: Any) -> None:
    """Emit one compact intelligence write log event."""

    LOGGER.info(json.dumps({"event": event, **fields}, sort_keys=True, ensure_ascii=True))
