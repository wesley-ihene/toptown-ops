"""Final governance layer for TopTown structured records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Literal

import packages.record_store.paths as record_paths
from packages.branch_registry import is_canonical_branch_slug

GovernedStatus = Literal[
    "accepted",
    "accepted_with_warning",
    "needs_review",
    "rejected",
    "duplicate",
    "conflict_blocked",
]

GovernanceReason = Literal[
    "invalid_pricing_card_format",
    "correction_request_requires_full_report",
    "duplicate_message_id",
    "duplicate_raw_sha256",
    "duplicate_semantic",
    "conflicting_record_same_scope",
    "unknown_report_type",
    "unknown_branch_slug",
    "ambiguous_branch",
    "ambiguous_report_date",
    "insufficient_structure",
    "parser_failure",
]

GOVERNANCE_SIDECAR_SUFFIX = ".governance.json"
EXPORTABLE_FINAL_STATUSES: frozenset[str] = frozenset({"accepted", "accepted_with_warning"})
INTELLIGENCE_REPORT_FAMILY = "intelligence"

_SIGNAL_TYPE_TO_REPORT_FAMILY = {
    "sales_income": "sales",
    "pricing_stock_release": "bale_summary",
    "hr_attendance": "staff_attendance",
    "hr_performance": "staff_performance",
    "supervisor_control": "supervisor_control",
}
_IGNORED_SEMANTIC_FIELDS = {
    "confidence",
    "export_allowed",
    "governance",
    "reconciliation",
    "review_policy",
    "source_agent",
    "status",
    "warnings",
}


@dataclass(slots=True, frozen=True)
class GovernanceDecision:
    """Stable governance output for one candidate structured record."""

    status: GovernedStatus
    export_allowed: bool
    report_family: str
    signal_type: str
    branch: str | None
    report_date: str | None
    message_id: str | None
    raw_sha256: str | None
    normalized_scope: str | None
    semantic_sha256: str | None
    reasons: list[GovernanceReason] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    source_status: str | None = None
    duplicate_of: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe governance payload."""

        return {
            "version": "v1",
            "status": self.status,
            "export_allowed": self.export_allowed,
            "report_family": self.report_family,
            "signal_type": self.signal_type,
            "branch": self.branch,
            "report_date": self.report_date,
            "message_id": self.message_id,
            "raw_sha256": self.raw_sha256,
            "normalized_scope": self.normalized_scope,
            "semantic_sha256": self.semantic_sha256,
            "reasons": list(self.reasons),
            "warnings": list(self.warnings),
            "source_status": self.source_status,
            "duplicate_of": self.duplicate_of,
        }


@dataclass(slots=True, frozen=True)
class GovernedWriteResult:
    """Result returned after governance-aware structured persistence."""

    path: Path
    persisted: bool
    governance: GovernanceDecision


def build_governance_context(work_item_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Extract stable governance inputs from a routed work item payload."""

    ingress = work_item_payload.get("ingress_envelope")
    raw_record = work_item_payload.get("raw_record")
    raw_message = work_item_payload.get("raw_message")
    classification = work_item_payload.get("classification")
    human_tolerance = work_item_payload.get("human_tolerance")

    ingress_payload = ingress.get("payload") if isinstance(ingress, Mapping) else {}
    raw_record_payload = raw_record if isinstance(raw_record, Mapping) else {}
    raw_message_payload = raw_message if isinstance(raw_message, Mapping) else {}
    classification_payload = classification if isinstance(classification, Mapping) else {}
    human_tolerance_payload = human_tolerance if isinstance(human_tolerance, Mapping) else {}

    return {
        "message_id": _string_or_none(ingress_payload.get("message_id")),
        "sender_phone": _string_or_none(ingress_payload.get("sender_phone")),
        "raw_sha256": _string_or_none(raw_record_payload.get("raw_sha256"))
        or _string_or_none(ingress_payload.get("raw_sha256")),
        "raw_txt_path": _string_or_none(raw_record_payload.get("raw_txt_path"))
        or _string_or_none(ingress_payload.get("raw_txt_path")),
        "raw_meta_path": _string_or_none(raw_record_payload.get("raw_meta_path")),
        "raw_text": _string_or_none(raw_message_payload.get("text")),
        "classified_report_family": _string_or_none(classification_payload.get("report_family")),
        "classified_report_type": _string_or_none(classification_payload.get("report_type")),
        "human_tolerance": dict(human_tolerance_payload) if human_tolerance_payload else None,
    }


def govern_record(
    *,
    signal_type: str,
    branch: str | None,
    report_date: str | None,
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any] | None,
    structured_path: Path,
    source_root: Path,
) -> GovernanceDecision:
    """Return the final governance decision for one candidate structured record."""

    governance_context = _mapping((metadata or {}).get("governance_context"))
    validation = _mapping((metadata or {}).get("validation"))
    acceptance = _mapping((metadata or {}).get("acceptance"))
    source_status = _string_or_none(payload.get("status"))
    message_id = _string_or_none(governance_context.get("message_id"))
    raw_sha256 = _string_or_none(governance_context.get("raw_sha256"))
    raw_meta_path = _string_or_none(governance_context.get("raw_meta_path"))
    classified_report_family = _string_or_none(governance_context.get("classified_report_family"))
    classified_report_type = _string_or_none(governance_context.get("classified_report_type"))
    intelligence_signal = _is_intelligence_signal(
        signal_type=signal_type,
        classified_report_type=classified_report_type,
        classified_report_family=classified_report_family,
    )

    report_family = _report_family_for_signal_type(signal_type, governance_context)
    normalized_scope = _scope(report_family=report_family, branch=branch, report_date=report_date)
    semantic_sha256 = _semantic_sha256(
        report_family=report_family,
        branch=branch,
        report_date=report_date,
        payload=payload,
    )

    duplicate_reason, duplicate_of = _find_exact_duplicate(
        records_root=source_root / "records",
        signal_type=signal_type,
        report_family=report_family,
        message_id=message_id,
        raw_sha256=raw_sha256,
        exclude_raw_meta_path=raw_meta_path,
        exclude_structured_path=structured_path,
    )
    if duplicate_reason is not None:
        return GovernanceDecision(
            status="duplicate",
            export_allowed=False,
            report_family=report_family,
            signal_type=signal_type,
            branch=branch,
            report_date=report_date,
            message_id=message_id,
            raw_sha256=raw_sha256,
            normalized_scope=normalized_scope,
            semantic_sha256=semantic_sha256,
            reasons=[duplicate_reason],
            warnings=_warning_codes(payload),
            source_status=source_status,
            duplicate_of=duplicate_of,
        )

    rejection_reasons: list[GovernanceReason] = []
    if not intelligence_signal:
        rejection_reasons = _taxonomy_reasons(
            signal_type=signal_type,
            report_family=report_family,
            branch=branch,
            report_date=report_date,
            source_status=source_status,
            payload=payload,
            validation=validation,
            acceptance=acceptance,
            governance_context=governance_context,
        )
    if rejection_reasons:
        return GovernanceDecision(
            status="rejected",
            export_allowed=False,
            report_family=report_family,
            signal_type=signal_type,
            branch=branch,
            report_date=report_date,
            message_id=message_id,
            raw_sha256=raw_sha256,
            normalized_scope=normalized_scope,
            semantic_sha256=semantic_sha256,
            reasons=rejection_reasons,
            warnings=_warning_codes(payload),
            source_status=source_status,
        )

    existing_record_path = structured_path
    legacy_record_path = _legacy_record_path_if_present(
        signal_type=signal_type,
        branch=branch,
        report_date=report_date,
        structured_path=structured_path,
        source_root=source_root,
    )
    if not existing_record_path.exists() and legacy_record_path is not None:
        existing_record_path = legacy_record_path

    if existing_record_path.exists():
        existing_governance = read_governance_sidecar(existing_record_path)
        existing_semantic_sha = None if intelligence_signal else _string_or_none(existing_governance.get("semantic_sha256"))
        existing_semantic_sha = existing_semantic_sha or _semantic_sha256(
            report_family=report_family,
            branch=branch,
            report_date=report_date,
            payload=_read_json_file(existing_record_path),
        )
        if existing_semantic_sha == semantic_sha256:
            return GovernanceDecision(
                status="duplicate",
                export_allowed=False,
                report_family=report_family,
                signal_type=signal_type,
                branch=branch,
                report_date=report_date,
                message_id=message_id,
                raw_sha256=raw_sha256,
                normalized_scope=normalized_scope,
                semantic_sha256=semantic_sha256,
                reasons=["duplicate_semantic"],
                warnings=_warning_codes(payload),
                source_status=source_status,
                duplicate_of=str(existing_record_path),
            )
        if _replacement_write_requested(
            signal_type=signal_type,
            report_family=report_family,
            governance_context=governance_context,
        ):
            replacement_status = (
                _intelligence_governed_status()
                if intelligence_signal
                else _governed_status(
                    source_status=source_status,
                    warnings=_warning_codes(payload),
                    acceptance=acceptance,
                )
            )
            return GovernanceDecision(
                status=replacement_status,
                export_allowed=True if intelligence_signal else replacement_status in EXPORTABLE_FINAL_STATUSES,
                report_family=report_family,
                signal_type=signal_type,
                branch=branch,
                report_date=report_date,
                message_id=message_id,
                raw_sha256=raw_sha256,
                normalized_scope=normalized_scope,
                semantic_sha256=semantic_sha256,
                reasons=[],
                warnings=_warning_codes(payload),
                source_status=source_status,
                duplicate_of=str(existing_record_path),
            )
        return GovernanceDecision(
            status="conflict_blocked",
            export_allowed=False,
            report_family=report_family,
            signal_type=signal_type,
            branch=branch,
            report_date=report_date,
            message_id=message_id,
            raw_sha256=raw_sha256,
            normalized_scope=normalized_scope,
            semantic_sha256=semantic_sha256,
            reasons=["conflicting_record_same_scope"],
            warnings=_warning_codes(payload),
            source_status=source_status,
            duplicate_of=str(existing_record_path),
        )

    status = (
        _intelligence_governed_status()
        if intelligence_signal
        else _governed_status(
            source_status=source_status,
            warnings=_warning_codes(payload),
            acceptance=acceptance,
        )
    )
    return GovernanceDecision(
        status=status,
        export_allowed=True if intelligence_signal else status in EXPORTABLE_FINAL_STATUSES,
        report_family=report_family,
        signal_type=signal_type,
        branch=branch,
        report_date=report_date,
        message_id=message_id,
        raw_sha256=raw_sha256,
        normalized_scope=normalized_scope,
        semantic_sha256=semantic_sha256,
        reasons=[],
        warnings=_warning_codes(payload),
        source_status=source_status,
    )


def read_governance_sidecar(structured_path: Path) -> dict[str, Any]:
    """Return one structured-record governance sidecar payload."""

    return _read_json_file(structured_path.with_suffix(GOVERNANCE_SIDECAR_SUFFIX))


def write_governance_sidecar(structured_path: Path, decision: GovernanceDecision) -> Path:
    """Persist the governance sidecar beside a structured payload."""

    sidecar_path = structured_path.with_suffix(GOVERNANCE_SIDECAR_SUFFIX)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = sidecar_path.with_suffix(f"{sidecar_path.suffix}.tmp")
    content = json.dumps(decision.to_payload(), indent=2, sort_keys=True, ensure_ascii=True)
    temporary_path.write_text(f"{content}\n", encoding="utf-8")
    os.replace(temporary_path, sidecar_path)
    return sidecar_path


def _taxonomy_reasons(
    *,
    signal_type: str,
    report_family: str,
    branch: str | None,
    report_date: str | None,
    source_status: str | None,
    payload: Mapping[str, Any],
    validation: Mapping[str, Any],
    acceptance: Mapping[str, Any],
    governance_context: Mapping[str, Any],
) -> list[GovernanceReason]:
    reasons: list[GovernanceReason] = []
    classified_report_type = _string_or_none(governance_context.get("classified_report_type"))
    if classified_report_type == "invalid_pricing_card_format":
        reasons.append("invalid_pricing_card_format")
    if report_family == "unknown" or classified_report_type == "unknown":
        reasons.append("unknown_report_type")
    if branch is None or not branch.strip():
        reasons.append("ambiguous_branch")
    elif not is_canonical_branch_slug(branch):
        reasons.append("unknown_branch_slug")
    if _is_missing_scope_value(report_date):
        reasons.append("ambiguous_report_date")
    validation_details = _mapping(validation.get("details"))
    if (
        validation_details.get("parser_failure") is True
        or "parser_failure" in _warning_codes(payload)
        or "parser_failure" in _reason_codes(validation)
    ):
        reasons.append("parser_failure")
    if _has_reason_code(validation, "correction_request_requires_full_report") or "correction_request_requires_full_report" in _warning_codes(payload):
        reasons.append("correction_request_requires_full_report")
    if source_status in {"invalid_input", "rejected"}:
        reasons.append("insufficient_structure")
    if acceptance.get("decision") == "reject":
        reasons.append("insufficient_structure")
    if validation.get("accepted") is False:
        reasons.append("insufficient_structure")
    if not _has_meaningful_structure(signal_type=signal_type, payload=payload):
        reasons.append("insufficient_structure")
    return _dedupe_text_items(reasons)


def _governed_status(
    *,
    source_status: str | None,
    warnings: Sequence[str],
    acceptance: Mapping[str, Any],
) -> GovernedStatus:
    decision = _string_or_none(acceptance.get("decision"))
    if decision == "reject":
        return "rejected"
    if decision == "review":
        return "needs_review"
    if source_status in {"invalid_input", "rejected"}:
        return "rejected"
    if source_status == "needs_review":
        return "needs_review"
    if source_status == "accepted_with_warning":
        return "accepted_with_warning"
    if source_status in {"accepted", "ready"}:
        return "accepted_with_warning" if warnings else "accepted"
    return "needs_review"


def _intelligence_governed_status() -> GovernedStatus:
    """Return the stable final status for accepted intelligence reports."""

    return "accepted"


def _find_exact_duplicate(
    *,
    records_root: Path,
    signal_type: str,
    report_family: str,
    message_id: str | None,
    raw_sha256: str | None,
    exclude_raw_meta_path: str | None,
    exclude_structured_path: Path,
) -> tuple[GovernanceReason | None, str | None]:
    excluded_raw_meta = Path(exclude_raw_meta_path) if exclude_raw_meta_path else None

    for meta_path in record_paths.RAW_WHATSAPP_DIR.glob("**/*.meta.json"):
        if excluded_raw_meta is not None and meta_path == excluded_raw_meta:
            continue
        payload = _read_json_file(meta_path)
        if not payload:
            continue
        processing_status = _string_or_none(payload.get("processing_status"))
        if processing_status in {None, "received"}:
            continue
        if message_id and _string_or_none(payload.get("message_id")) == message_id:
            return "duplicate_message_id", str(meta_path)
        if raw_sha256 and _string_or_none(payload.get("raw_sha256")) == raw_sha256:
            return "duplicate_raw_sha256", str(meta_path)

    allowed_families = _matching_report_families(report_family=report_family, signal_type=signal_type)
    for storage_root in _governance_storage_roots(records_root):
        for governance_path in storage_root.glob("**/*" + GOVERNANCE_SIDECAR_SUFFIX):
            if governance_path == exclude_structured_path.with_suffix(GOVERNANCE_SIDECAR_SUFFIX):
                continue
            payload = _read_json_file(governance_path)
            if not payload:
                continue
            existing_report_family = _string_or_none(payload.get("report_family"))
            if existing_report_family is not None and existing_report_family not in allowed_families:
                continue
            if message_id and _string_or_none(payload.get("message_id")) == message_id:
                return "duplicate_message_id", str(governance_path)
            if raw_sha256 and _string_or_none(payload.get("raw_sha256")) == raw_sha256:
                return "duplicate_raw_sha256", str(governance_path)
    return None, None


def _semantic_sha256(
    *,
    report_family: str,
    branch: str | None,
    report_date: str | None,
    payload: Mapping[str, Any],
) -> str:
    normalized = {
        "report_family": report_family,
        "branch": branch,
        "report_date": report_date,
        "payload": _semantic_value(payload),
    }
    serialized = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _semantic_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for key in sorted(value):
            if key in _IGNORED_SEMANTIC_FIELDS:
                continue
            output[str(key)] = _semantic_value(value[key])
        return output
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_semantic_value(item) for item in value]
    if isinstance(value, str):
        return " ".join(value.strip().casefold().split())
    return value


def _has_meaningful_structure(*, signal_type: str, payload: Mapping[str, Any]) -> bool:
    items = payload.get("items")
    metrics = payload.get("metrics")
    if signal_type == "sales_income":
        return isinstance(metrics, Mapping) and any(_is_nonzero(metrics.get(field)) for field in ("gross_sales", "cash_sales", "eftpos_sales"))
    if signal_type == "pricing_stock_release":
        return isinstance(items, Sequence) and any(isinstance(item, Mapping) for item in items)
    if signal_type in {"hr_attendance", "hr_performance"}:
        return isinstance(items, Sequence) and len([item for item in items if isinstance(item, Mapping)]) > 0
    if signal_type == "supervisor_control":
        return bool(_sequence_size(items)) or bool(_mapping(payload.get("metrics")))
    return bool(_mapping(metrics)) or bool(_sequence_size(items))


def _scope(*, report_family: str, branch: str | None, report_date: str | None) -> str | None:
    if _is_missing_scope_value(branch) or _is_missing_scope_value(report_date) or not is_canonical_branch_slug(branch):
        return None
    return f"{report_family}:{branch}:{report_date}"


def _report_family_for_signal_type(signal_type: str, governance_context: Mapping[str, Any]) -> str:
    classified_report_family = _string_or_none(governance_context.get("classified_report_family"))
    classified_report_type = _string_or_none(governance_context.get("classified_report_type"))
    if _is_intelligence_signal(
        signal_type=signal_type,
        classified_report_type=classified_report_type,
        classified_report_family=classified_report_family,
    ):
        return INTELLIGENCE_REPORT_FAMILY
    if classified_report_family is not None:
        return classified_report_family
    if classified_report_type is not None:
        return classified_report_type
    return _SIGNAL_TYPE_TO_REPORT_FAMILY.get(signal_type, "unknown")


def _is_intelligence_signal(
    *,
    signal_type: str,
    classified_report_type: str | None,
    classified_report_family: str | None,
) -> bool:
    """Return whether one governed record is intelligence-only."""

    return (
        signal_type == "supervisor_control"
        or classified_report_type == "supervisor_control"
        or classified_report_family == INTELLIGENCE_REPORT_FAMILY
    )


def _matching_report_families(*, report_family: str, signal_type: str) -> set[str]:
    """Return the report-family labels that should be treated as equivalent."""

    families = {report_family}
    if signal_type == "supervisor_control" or report_family == INTELLIGENCE_REPORT_FAMILY:
        families.update({INTELLIGENCE_REPORT_FAMILY, "supervisor_control"})
    return families


def _governance_storage_roots(records_root: Path) -> list[Path]:
    """Return the record roots that may contain governance sidecars."""

    roots = [records_root / "structured"]
    intelligence_root = records_root / "intelligence"
    if intelligence_root not in roots:
        roots.append(intelligence_root)
    return roots


def _legacy_record_path_if_present(
    *,
    signal_type: str,
    branch: str | None,
    report_date: str | None,
    structured_path: Path,
    source_root: Path,
) -> Path | None:
    """Return the legacy structured path for migrated intelligence records when present."""

    if not record_paths.is_intelligence_signal_type(signal_type):
        return None
    if _is_missing_scope_value(branch) or _is_missing_scope_value(report_date) or not is_canonical_branch_slug(branch):
        return None
    legacy_path = record_paths.get_legacy_structured_path_for_root(
        source_root / "records" / "structured",
        signal_type=signal_type,
        branch=branch,
        date=report_date,
    )
    if legacy_path == structured_path or not legacy_path.exists():
        return None
    return legacy_path


def _warning_codes(payload: Mapping[str, Any]) -> list[str]:
    warnings = payload.get("warnings")
    if not isinstance(warnings, Sequence) or isinstance(warnings, (str, bytes, bytearray)):
        return []
    codes: list[str] = []
    for entry in warnings:
        if isinstance(entry, Mapping):
            code = _string_or_none(entry.get("code"))
            if code is not None:
                codes.append(code)
    return _dedupe_text_items(codes)


def _has_reason_code(validation: Mapping[str, Any], reason_code: str) -> bool:
    return reason_code in _reason_codes(validation)


def _replacement_write_requested(
    *,
    signal_type: str,
    report_family: str,
    governance_context: Mapping[str, Any],
) -> bool:
    return (
        signal_type == "pricing_stock_release"
        and report_family == "bale_summary"
        and governance_context.get("correction_intent_detected") is True
        and governance_context.get("allow_same_scope_supersede") is True
    )


def _reason_codes(validation: Mapping[str, Any]) -> list[str]:
    reason_codes = validation.get("reason_codes")
    if not isinstance(reason_codes, Sequence) or isinstance(reason_codes, (str, bytes, bytearray)):
        return []
    output: list[str] = []
    for entry in reason_codes:
        if isinstance(entry, str) and entry.strip():
            output.append(entry.strip())
    return _dedupe_text_items(output)


def _is_missing_scope_value(value: str | None) -> bool:
    return value is None or value.strip() in {"", "unknown"}


def _dedupe_text_items(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _sequence_size(value: Any) -> int:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return 0
    return len(value)


def _is_nonzero(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value != 0
