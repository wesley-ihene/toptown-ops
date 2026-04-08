"""Replay archived raw or rejected records without creating duplicate raw audits."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any
from collections.abc import Mapping

import apps.hr_agent.record_store as hr_record_store
import apps.hr_agent.worker as hr_worker
import apps.orchestrator_agent.worker as orchestrator_worker
import apps.pricing_stock_release_agent.record_store as pricing_record_store
import apps.pricing_stock_release_agent.worker as pricing_worker
import apps.sales_income_agent.record_store as sales_record_store
import apps.sales_income_agent.worker as sales_worker
import apps.staff_performance_agent.worker as staff_performance_worker
import apps.supervisor_control_agent.worker as supervisor_control_worker
from packages.common.paths import REPO_ROOT
from packages.record_store.naming import build_rejected_filename, safe_segment
from packages.record_store.paths import get_rejected_path, get_structured_path
from packages.record_store.writer import ensure_directory, write_json_file, write_structured, write_text_file
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

SUPPORTED_SOURCES = ("raw", "rejected")
SUPPORTED_MODES = ("orchestrator", "specialist")
SUPPORTED_REPORT_TYPES = ("sales", "bale_release", "hr_attendance", "hr_performance", "supervisor_control", "unknown")
MANIFEST_STATUS_VALUES = ("structured_written", "rejected", "skipped", "failed")
LOGS_REPLAY_DIR = REPO_ROOT / "logs" / "replay"
PROGRESS_EVERY = 10

REPORT_TYPE_TO_CLASSIFICATION = {
    "sales": "sales",
    "bale_release": "bale_summary",
    "hr_attendance": "staff_attendance",
    "hr_performance": "staff_performance",
    "supervisor_control": "supervisor_control",
    "unknown": "unknown",
}

CLASSIFICATION_TO_REPORT_TYPE = {
    "sales": "sales",
    "bale_summary": "bale_release",
    "staff_attendance": "hr_attendance",
    "staff_performance": "hr_performance",
    "supervisor_control": "supervisor_control",
    "unknown": "unknown",
}

REPORT_TYPE_TO_AGENT = {
    "sales": "sales_income_agent",
    "bale_release": "pricing_stock_release_agent",
    "hr_attendance": "hr_agent",
    "hr_performance": "staff_performance_agent",
    "supervisor_control": "supervisor_control_agent",
}


@dataclass(slots=True)
class ArchivedRecord:
    """One archived text file plus its companion metadata."""

    text_path: Path
    metadata_path: Path | None
    source: str
    source_bucket: str
    text: str
    metadata: dict[str, Any]


@dataclass(slots=True)
class ReplayMetadata:
    """Deterministic replay metadata resolved by explicit precedence."""

    source: str
    sender: str | None
    branch_hint: str | None
    received_at: str
    report_type: str | None
    replay: dict[str, Any]


@dataclass(slots=True)
class StructuredArtifact:
    """Structured payload and canonical target path for a replay result."""

    path: Path
    payload: dict[str, Any]


@dataclass(slots=True)
class RejectedCapture:
    """Deferred rejected write captured during replay execution."""

    rejection_reason: str
    attempted_report_type: str
    attempted_agent: str | None
    exception_message: str | None


def main(argv: list[str] | None = None) -> int:
    """Replay archived records through orchestrator or direct specialist processing."""

    args = _parse_args(argv)
    print(f"[REPLAY MODE] {args.mode}")
    started_at = _utc_timestamp()
    run_id = _run_id()
    candidates, scanned_count = _select_records(args)
    if args.limit is not None:
        candidates = candidates[: args.limit]

    manifest_source = _manifest_source(candidates)
    results: list[dict[str, Any]] = []
    summary = {
        "scanned": scanned_count,
        "replayed": 0,
        "written": 0,
        "rejected": 0,
        "skipped": 0,
        "failed": 0,
    }

    for index, record in enumerate(candidates, start=1):
        if args.batch_size and (index - 1) % args.batch_size == 0:
            batch_end = min(index + args.batch_size - 1, len(candidates))
            print(f"batch {((index - 1) // args.batch_size) + 1}: files {index}-{batch_end}")
        elif index == 1 or index % PROGRESS_EVERY == 0:
            print(f"progress: {index}/{len(candidates)}")

        result = _replay_record(record=record, args=args)
        results.append(result)
        summary["replayed"] += 1
        if result["status"] == "structured_written":
            summary["written"] += _result_written_count(result)
        elif result["status"] == "rejected":
            summary["rejected"] += 1
        elif result["status"] == "skipped":
            summary["skipped"] += 1
        else:
            summary["failed"] += 1

        print(_format_console_result(index=index, total=len(candidates), result=result))

    finished_at = _utc_timestamp()
    manifest_path = _write_manifest(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        source=manifest_source,
        args=args,
        results=results,
        summary=summary,
    )
    print(f"manifest: {_display_path(manifest_path)}")
    print(
        "summary:"
        f" scanned={summary['scanned']}"
        f" replayed={summary['replayed']}"
        f" written={summary['written']}"
        f" rejected={summary['rejected']}"
        f" skipped={summary['skipped']}"
        f" failed={summary['failed']}"
    )
    return 1 if summary["failed"] else 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse replay CLI options."""

    parser = argparse.ArgumentParser(
        description=(
            "Replay archived raw or rejected records through orchestrator or direct "
            "specialist flows. Replay is read-only at the raw archive layer."
        )
    )
    parser.add_argument("--source", choices=SUPPORTED_SOURCES, required=True, help="Archive source to scan.")
    parser.add_argument(
        "--mode",
        choices=SUPPORTED_MODES,
        default="orchestrator",
        help="Replay mode (default: orchestrator)",
    )
    parser.add_argument("--path", action="append", default=[], help="Replay a specific archived .txt file.")
    parser.add_argument("--report-type", choices=SUPPORTED_REPORT_TYPES, help="Known report type override for replay selection or specialist mode.")
    parser.add_argument("--date", help="Filter by YYYY-MM-DD.")
    parser.add_argument("--branch", help="Override branch hint and filter by branch slug or hint.")
    parser.add_argument("--all", action="store_true", help="Replay all matching files from the selected source.")
    parser.add_argument("--limit", type=int, help="Maximum files to replay after filtering.")
    parser.add_argument("--batch-size", type=int, help="Print simple batch progress for larger runs.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve routing and outputs without writing structured or rejected files.")
    parser.add_argument("--compare-only", action="store_true", help="Compare replay output against existing structured files without writing them.")
    parser.add_argument("--overwrite", action="store_true", help="Explicitly allow structured overwrite when a canonical file already exists.")
    args = parser.parse_args(argv)

    if not any((args.path, args.report_type, args.date, args.branch, args.all)):
        parser.error("provide at least one selector: --path, --report-type, --date, --branch, or --all")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be greater than zero")
    if args.batch_size is not None and args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero")
    if args.compare_only and args.overwrite:
        parser.error("--compare-only and --overwrite cannot be combined")
    if args.mode == "specialist" and args.report_type == "unknown":
        parser.error("--mode specialist cannot use --report-type unknown")
    return args


def _select_records(args: argparse.Namespace) -> tuple[list[ArchivedRecord], int]:
    """Select replay records from explicit paths and filtered archive scans."""

    selected: dict[Path, ArchivedRecord] = {}
    scanned_count = 0

    for raw_path in args.path:
        text_path = Path(raw_path).resolve()
        if not text_path.exists():
            raise SystemExit(f"missing path: {raw_path}")
        if text_path.suffix != ".txt":
            raise SystemExit(f"expected a .txt replay path: {raw_path}")
        record = _load_archived_record(text_path)
        selected[text_path] = record
        scanned_count += 1

    if args.all or args.report_type or args.date or args.branch:
        root = _source_root(args.source)
        for text_path in sorted(root.rglob("*.txt")):
            if text_path.name == ".gitkeep":
                continue
            scanned_count += 1
            record = _load_archived_record(text_path)
            if _matches_filters(record=record, args=args):
                selected[text_path.resolve()] = record

    return sorted(selected.values(), key=lambda record: str(record.text_path)), scanned_count


def _load_archived_record(text_path: Path) -> ArchivedRecord:
    """Load archived text plus companion metadata."""

    metadata_path = text_path.with_suffix(".meta.json")
    metadata: dict[str, Any] = {}
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    source = "raw" if "/records/raw/" in str(text_path) else "rejected"
    return ArchivedRecord(
        text_path=text_path.resolve(),
        metadata_path=metadata_path.resolve() if metadata_path.exists() else None,
        source=source,
        source_bucket=text_path.parent.name,
        text=text_path.read_text(encoding="utf-8"),
        metadata=metadata,
    )


def _matches_filters(*, record: ArchivedRecord, args: argparse.Namespace) -> bool:
    """Return whether a record matches selection filters."""

    resolved = _resolve_replay_metadata(record=record, args=args)
    if args.report_type and resolved.report_type != args.report_type:
        return False
    if args.date and _record_date(record, resolved) != args.date:
        return False
    if args.branch and safe_segment(resolved.branch_hint or "") != safe_segment(args.branch):
        return False
    return True


def _resolve_replay_metadata(*, record: ArchivedRecord, args: argparse.Namespace) -> ReplayMetadata:
    """Resolve replay metadata with deterministic precedence.

    Precedence:
    1. CLI overrides
    2. Companion metadata file
    3. Inferred/default values
    """

    inferred_branch = _infer_branch_hint(record)
    inferred_received_at = _infer_received_at(record)
    inferred_report_type = _infer_report_type(record)

    metadata_sender = _read_optional_string(record.metadata, "sender")
    metadata_source = _read_optional_string(record.metadata, "source")
    metadata_branch = _read_optional_string(record.metadata, "branch_hint")
    metadata_received_at = _read_optional_string(record.metadata, "received_at")
    metadata_report_type = _metadata_report_type(record.metadata)

    branch_hint = _cli_or_metadata_or_default(
        cli_value=args.branch,
        metadata_value=metadata_branch,
        default_value=inferred_branch,
    )
    received_at = metadata_received_at or inferred_received_at
    report_type = _normalize_report_type(
        _cli_or_metadata_or_default(
            cli_value=args.report_type,
            metadata_value=metadata_report_type,
            default_value=inferred_report_type,
        )
    )

    return ReplayMetadata(
        source=metadata_source or "whatsapp",
        sender=metadata_sender,
        branch_hint=branch_hint,
        received_at=received_at,
        report_type=report_type,
        replay={
            "is_replay": True,
            "source": record.source,
            "original_path": _display_path(record.text_path),
            "replayed_at": _utc_timestamp(),
        },
    )


def _cli_or_metadata_or_default(
    *,
    cli_value: str | None,
    metadata_value: str | None,
    default_value: str | None,
) -> str | None:
    """Return one value using explicit replay precedence."""

    if cli_value is not None and cli_value.strip():
        return cli_value.strip()
    if metadata_value is not None and metadata_value.strip():
        return metadata_value.strip()
    if default_value is not None and default_value.strip():
        return default_value.strip()
    return None


def _replay_record(*, record: ArchivedRecord, args: argparse.Namespace) -> dict[str, Any]:
    """Replay one record and return one manifest result entry."""

    started = datetime.now(timezone.utc)
    resolved = _resolve_replay_metadata(record=record, args=args)

    try:
        work_item = _build_work_item(record=record, resolved=resolved)
        capture = RejectedCapture("", "", None, None)
        with _suppress_replay_side_effects(capture):
            if args.mode == "orchestrator":
                result = orchestrator_worker.process_work_item(work_item)
            else:
                result = _run_specialist_mode(work_item=work_item, resolved=resolved)

        structured_artifacts = _structured_artifacts_from_result(result)
        entry = _finalize_replay_result(
            record=record,
            resolved=resolved,
            result=result,
            structured_artifacts=structured_artifacts,
            rejected_capture=capture if capture.rejection_reason else None,
            args=args,
            duration_ms=_duration_ms(started),
        )
        return entry
    except Exception as exc:
        return {
            "file": _display_path(record.text_path),
            "status": "failed",
            "agent": None,
            "output_path": None,
            "reason": str(exc),
            "duration_ms": _duration_ms(started),
        }


def _build_work_item(*, record: ArchivedRecord, resolved: ReplayMetadata) -> WorkItem:
    """Build the replay work item with an explicit replay marker."""

    payload: dict[str, Any] = {
        "source": resolved.source,
        "raw_message": {"text": record.text},
        "metadata": {
            "received_at": resolved.received_at,
        },
        "replay": resolved.replay,
    }
    if resolved.sender:
        payload["metadata"]["sender"] = resolved.sender
    if resolved.branch_hint:
        payload["metadata"]["branch_hint"] = resolved.branch_hint
    return WorkItem(kind="raw_message", payload=payload)


def _run_specialist_mode(*, work_item: WorkItem, resolved: ReplayMetadata) -> AgentResult:
    """Replay directly to one specialist with an explicit report type mapping."""

    report_type = resolved.report_type
    if report_type is None or report_type not in REPORT_TYPE_TO_AGENT:
        raise ValueError("specialist replay requires a valid mapped report type")

    classification = REPORT_TYPE_TO_CLASSIFICATION[report_type]
    payload = dict(work_item.payload)
    payload["classification"] = {"report_type": classification}
    specialist_item = WorkItem(kind=work_item.kind, payload=payload)

    target_agent = REPORT_TYPE_TO_AGENT[report_type]
    if target_agent == "sales_income_agent":
        return sales_worker.process_work_item(specialist_item)
    if target_agent == "pricing_stock_release_agent":
        return pricing_worker.process_work_item(specialist_item)
    if target_agent == "hr_agent":
        return hr_worker.process_work_item(specialist_item)
    if target_agent == "staff_performance_agent":
        return staff_performance_worker.process_work_item(specialist_item)
    if target_agent == "supervisor_control_agent":
        return supervisor_control_worker.process_work_item(specialist_item)
    raise ValueError(f"specialist replay has no valid agent mapping for report type: {report_type}")


def _structured_artifacts_from_result(result: AgentResult) -> list[StructuredArtifact]:
    """Return canonical structured artifacts for a replay result."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    fanout = payload.get("fanout")
    if result.agent_name == "orchestrator_agent" and isinstance(fanout, dict):
        artifacts: list[StructuredArtifact] = []
        children = fanout.get("children")
        if isinstance(children, list):
            for child in children:
                if not isinstance(child, Mapping):
                    continue
                agent_name = child.get("agent_name")
                child_payload = child.get("payload")
                if not isinstance(agent_name, str) or not isinstance(child_payload, dict):
                    continue
                artifacts.extend(
                    _structured_artifacts_from_result(
                        AgentResult(agent_name=agent_name, payload=child_payload)
                    )
                )
        return artifacts

    artifact = _structured_artifact_from_single_result(result)
    if artifact is None:
        return []
    return [artifact]


def _structured_artifact_from_single_result(result: AgentResult) -> StructuredArtifact | None:
    """Return the canonical structured artifact for a single specialist result."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    if payload.get("status") == "invalid_input":
        return None

    if result.agent_name == "sales_income_agent":
        branch = payload.get("branch")
        report_date = payload.get("report_date")
        if not isinstance(branch, str) or not isinstance(report_date, str):
            return None
        canonical_branch = sales_record_store._canonical_branch_or_none(branch)
        normalized_date = sales_record_store._iso_date_or_none(report_date)
        if canonical_branch is None or normalized_date is None:
            return None
        payload_copy = dict(payload)
        payload_copy["branch"] = canonical_branch
        payload_copy["report_date"] = normalized_date
        return StructuredArtifact(
            path=get_structured_path("sales_income", canonical_branch, normalized_date),
            payload=payload_copy,
        )

    if result.agent_name == "pricing_stock_release_agent":
        branch = payload.get("branch")
        report_date = payload.get("report_date")
        if not isinstance(branch, str) or not isinstance(report_date, str):
            return None
        branch_slug = pricing_record_store._canonical_branch_slug(branch)
        normalized_date = pricing_record_store._normalize_report_date(report_date)
        persisted_payload = dict(payload)
        persisted_payload["branch_slug"] = branch_slug
        persisted_payload["report_date"] = normalized_date
        return StructuredArtifact(
            path=get_structured_path("pricing_stock_release", branch_slug, normalized_date),
            payload=persisted_payload,
        )

    if result.agent_name == "hr_agent":
        signal_subtype = payload.get("signal_subtype")
        branch = payload.get("branch")
        report_date = payload.get("report_date")
        if not isinstance(signal_subtype, str) or not isinstance(branch, str) or not isinstance(report_date, str):
            return None
        canonical_branch = hr_record_store._canonical_branch_or_none(branch)
        normalized_date = hr_record_store._iso_date_or_none(report_date)
        signal_type = {
            "staff_attendance": "hr_attendance",
            "staff_performance": "hr_performance",
        }.get(signal_subtype)
        if signal_type is None or canonical_branch is None or normalized_date is None:
            return None
        payload_copy = dict(payload)
        payload_copy["report_date"] = normalized_date
        return StructuredArtifact(
            path=get_structured_path(signal_type, canonical_branch, normalized_date),
            payload=payload_copy,
        )

    if result.agent_name == "staff_performance_agent":
        branch = payload.get("branch")
        report_date = payload.get("report_date")
        if not isinstance(branch, str) or not isinstance(report_date, str):
            return None
        canonical_branch = hr_record_store._canonical_branch_or_none(branch)
        normalized_date = hr_record_store._iso_date_or_none(report_date)
        if canonical_branch is None or normalized_date is None:
            return None
        payload_copy = dict(payload)
        payload_copy["report_date"] = normalized_date
        return StructuredArtifact(
            path=get_structured_path("hr_performance", canonical_branch, normalized_date),
            payload=payload_copy,
        )

    if result.agent_name == "supervisor_control_agent":
        branch = payload.get("branch")
        report_date = payload.get("report_date")
        if not isinstance(branch, str) or not isinstance(report_date, str):
            return None
        return StructuredArtifact(
            path=get_structured_path("supervisor_control", branch, report_date),
            payload=dict(payload),
        )

    return None


def _finalize_replay_result(
    *,
    record: ArchivedRecord,
    resolved: ReplayMetadata,
    result: AgentResult,
    structured_artifacts: list[StructuredArtifact],
    rejected_capture: RejectedCapture | None,
    args: argparse.Namespace,
    duration_ms: int,
) -> dict[str, Any]:
    """Write or skip deferred outputs and return one manifest entry."""

    structured_outcome = _handle_structured_artifacts(structured_artifacts=structured_artifacts, args=args)
    mixed_details = _mixed_result_details(result=result, structured_artifacts=structured_artifacts)
    if structured_outcome["status"] == "structured_written":
        entry = {
            "file": _display_path(record.text_path),
            "status": "structured_written",
            "agent": result.agent_name,
            "output_path": _display_path(structured_outcome["path"]) if structured_outcome["path"] else None,
            "written_count": structured_outcome["written_count"],
            "reason": structured_outcome["reason"],
            "duration_ms": duration_ms,
        }
        if structured_outcome["paths"]:
            entry["output_paths"] = [_display_path(path) for path in structured_outcome["paths"]]
        entry.update(mixed_details)
        return entry

    if rejected_capture is not None:
        rejected_outcome = _handle_rejected_copy(
            record=record,
            resolved=resolved,
            rejected_capture=rejected_capture,
            args=args,
        )
        if rejected_outcome["status"] == "rejected":
            return {
                "file": _display_path(record.text_path),
                "status": "rejected",
                "agent": result.agent_name,
                "output_path": _display_path(rejected_outcome["path"]),
                "written_count": 0,
                "reason": rejected_outcome["reason"],
                "duration_ms": duration_ms,
                **mixed_details,
            }
        entry = {
            "file": _display_path(record.text_path),
            "status": "skipped",
            "agent": result.agent_name,
            "output_path": _display_path(rejected_outcome["path"]) if rejected_outcome["path"] else None,
            "written_count": 0,
            "reason": rejected_outcome["reason"],
            "duration_ms": duration_ms,
        }
        entry.update(mixed_details)
        return entry

    entry = {
        "file": _display_path(record.text_path),
        "status": "skipped",
        "agent": result.agent_name,
        "output_path": _display_path(structured_outcome["path"]) if structured_outcome["path"] else None,
        "written_count": structured_outcome["written_count"],
        "reason": structured_outcome["reason"],
        "duration_ms": duration_ms,
    }
    if structured_outcome["paths"]:
        entry["output_paths"] = [_display_path(path) for path in structured_outcome["paths"]]
    entry.update(mixed_details)
    return entry


def _handle_structured_artifacts(
    *,
    structured_artifacts: list[StructuredArtifact],
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Apply overwrite, compare-only, and dry-run rules to one or more artifacts."""

    if not structured_artifacts:
        return {"status": "skipped", "path": None, "paths": [], "reason": "no_structured_output", "written_count": 0}

    outcomes = [_handle_single_structured_artifact(structured=artifact, args=args) for artifact in structured_artifacts]
    first_path = outcomes[0]["path"]
    first_reason = outcomes[0]["reason"]
    written_count = sum(1 for outcome in outcomes if outcome["status"] == "structured_written")

    if any(outcome["status"] == "structured_written" for outcome in outcomes):
        return {
            "status": "structured_written",
            "path": first_path,
            "paths": [outcome["path"] for outcome in outcomes if outcome["path"] is not None],
            "written_count": written_count,
            "reason": "; ".join(outcome["reason"] for outcome in outcomes),
        }
    return {
        "status": "skipped",
        "path": first_path,
        "paths": [outcome["path"] for outcome in outcomes if outcome["path"] is not None],
        "written_count": 0,
        "reason": "; ".join(outcome["reason"] for outcome in outcomes),
    }


def _handle_single_structured_artifact(
    *,
    structured: StructuredArtifact,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Apply overwrite and dry-run rules to one structured artifact."""

    existing_hash = _file_sha256(structured.path) if structured.path.exists() else None
    new_hash = _payload_sha256(structured.payload)
    if args.compare_only:
        compare_reason = "compare_only_missing"
        if existing_hash is not None:
            compare_reason = "compare_only_same" if _load_json(structured.path) == structured.payload else "compare_only_different"
        return {"status": "skipped", "path": structured.path, "reason": compare_reason}
    if structured.path.exists() and not args.overwrite:
        return {"status": "skipped", "path": structured.path, "reason": "structured_exists_use_overwrite"}
    if args.dry_run:
        if structured.path.exists():
            return {
                "status": "skipped",
                "path": structured.path,
                "reason": f"dry_run_would_overwrite previous_sha256={existing_hash} new_sha256={new_hash}",
            }
        return {"status": "skipped", "path": structured.path, "reason": f"dry_run_would_write new_sha256={new_hash}"}

    reason = f"written new_sha256={new_hash}"
    if structured.path.exists():
        reason = f"overwritten previous_sha256={existing_hash} new_sha256={new_hash}"
    write_structured(
        structured.path.parent.parent.name,
        structured.path.parent.name,
        structured.path.stem,
        structured.payload,
        root=structured.path.parents[4],
    )
    return {"status": "structured_written", "path": structured.path, "reason": reason}


def _handle_rejected_copy(
    *,
    record: ArchivedRecord,
    resolved: ReplayMetadata,
    rejected_capture: RejectedCapture,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Write or report a new rejected quarantine copy for replay failures."""

    report_type = _normalize_report_type(rejected_capture.attempted_report_type) or "unknown"
    rejected_path = get_rejected_path(report_type) / build_rejected_filename(
        report_type,
        rejected_capture.rejection_reason,
    )
    if args.dry_run or args.compare_only:
        return {
            "status": "skipped",
            "path": rejected_path,
            "reason": "dry_run_would_write_rejected" if args.dry_run else "compare_only_would_write_rejected",
        }

    write_text_file(rejected_path, record.text)
    metadata_payload: dict[str, Any] = {
        "replay": True,
        "replay_source": resolved.replay["source"],
        "replay_original_path": resolved.replay["original_path"],
        "replayed_at": resolved.replay["replayed_at"],
        "rejection_reason": rejected_capture.rejection_reason,
        "source": resolved.source,
        "received_at": resolved.received_at,
        "sender": resolved.sender,
        "branch_hint": resolved.branch_hint,
        "attempted_report_type": REPORT_TYPE_TO_CLASSIFICATION.get(report_type, report_type),
        "attempted_agent": rejected_capture.attempted_agent,
        "raw_sha256": hashlib.sha256(record.text.encode("utf-8")).hexdigest(),
        "exception_message": rejected_capture.exception_message,
    }
    if record.source == "rejected":
        metadata_payload["original_rejected_path"] = _display_path(record.text_path)
    write_json_file(rejected_path.with_suffix(".meta.json"), metadata_payload)
    return {"status": "rejected", "path": rejected_path, "reason": "rejected_copy_written"}


@contextmanager
def _suppress_replay_side_effects(rejected_capture: RejectedCapture):
    """Suppress live writes during replay while capturing rejected outcomes."""

    original_sales_write = sales_worker.write_structured_record
    original_hr_write = hr_worker.write_structured_record
    original_staff_performance_write = staff_performance_worker.write_structured_record
    original_supervisor_control_write = supervisor_control_worker.write_structured_record
    original_pricing_write = pricing_worker.write_structured_record
    original_pricing_outbox = pricing_worker._write_result_to_outbox
    original_rejected_write = orchestrator_worker._write_rejected_record

    sales_worker.write_structured_record = lambda payload: None
    hr_worker.write_structured_record = lambda payload: None
    staff_performance_worker.write_structured_record = lambda payload: None
    supervisor_control_worker.write_structured_record = lambda payload: None
    pricing_worker.write_structured_record = lambda payload: None
    pricing_worker._write_result_to_outbox = lambda result: REPO_ROOT / "data" / "outbox" / "replay_suppressed.json"

    def capture_rejected(
        audit,
        *,
        rejection_reason,
        attempted_report_type,
        attempted_agent,
        attempted_branch_hint,
        exception_message,
    ):
        rejected_capture.rejection_reason = rejection_reason
        rejected_capture.attempted_report_type = attempted_report_type
        rejected_capture.attempted_agent = attempted_agent
        rejected_capture.exception_message = exception_message
        report_type = _normalize_report_type(attempted_report_type) or "unknown"
        return get_rejected_path(report_type) / build_rejected_filename(report_type, rejection_reason)

    orchestrator_worker._write_rejected_record = capture_rejected
    try:
        yield
    finally:
        sales_worker.write_structured_record = original_sales_write
        hr_worker.write_structured_record = original_hr_write
        staff_performance_worker.write_structured_record = original_staff_performance_write
        supervisor_control_worker.write_structured_record = original_supervisor_control_write
        pricing_worker.write_structured_record = original_pricing_write
        pricing_worker._write_result_to_outbox = original_pricing_outbox
        orchestrator_worker._write_rejected_record = original_rejected_write


def _write_manifest(
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    source: str,
    args: argparse.Namespace,
    results: list[dict[str, Any]],
    summary: dict[str, int],
) -> Path:
    """Write one replay manifest for the run."""

    ensure_directory(LOGS_REPLAY_DIR)
    manifest_path = LOGS_REPLAY_DIR / f"{run_id}__{args.mode}.json"
    write_json_file(
        manifest_path,
        {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "mode": args.mode,
            "source": source,
            "filters": {
                "branch": args.branch,
                "report_type": args.report_type,
                "date": args.date,
                "limit": args.limit or 0,
                "batch_size": args.batch_size or 0,
                "overwrite": args.overwrite,
                "dry_run": args.dry_run,
            },
            "results": results,
            "summary": summary,
        },
    )
    return manifest_path


def _infer_branch_hint(record: ArchivedRecord) -> str | None:
    """Infer a branch hint from the archived filename when available."""

    parts = record.text_path.name.split("__")
    if len(parts) >= 3 and parts[1]:
        return parts[1]
    return None


def _infer_received_at(record: ArchivedRecord) -> str:
    """Infer a received timestamp from archived metadata or filename prefix."""

    prefix = record.text_path.name.split("__", 1)[0]
    if len(prefix) == 10 and prefix[4] == "-" and prefix[7] == "-":
        return f"{prefix}T00:00:00Z"
    return _utc_timestamp()


def _infer_report_type(record: ArchivedRecord) -> str | None:
    """Infer a report type from archive bucket names when possible."""

    if record.source_bucket in SUPPORTED_REPORT_TYPES:
        return record.source_bucket
    return None


def _metadata_report_type(metadata: dict[str, Any]) -> str | None:
    """Resolve one report type from companion metadata fields."""

    for field_name in ("attempted_report_type", "detected_report_type"):
        value = metadata.get(field_name)
        if isinstance(value, str):
            normalized = _normalize_report_type(value)
            if normalized is not None:
                return normalized
    return None


def _normalize_report_type(value: str | None) -> str | None:
    """Normalize report type aliases into replay CLI report types."""

    if value is None:
        return None
    normalized = safe_segment(value)
    aliases = {
        "sales": "sales",
        "sales_income": "sales",
        "bale_release": "bale_release",
        "bale_summary": "bale_release",
        "pricing_stock_release": "bale_release",
        "hr_attendance": "hr_attendance",
        "staff_attendance": "hr_attendance",
        "attendance": "hr_attendance",
        "hr_performance": "hr_performance",
        "staff_performance": "hr_performance",
        "supervisor_control": "supervisor_control",
        "staff_sales": "unknown",
        "unknown": "unknown",
    }
    return aliases.get(normalized)


def _record_date(record: ArchivedRecord, resolved: ReplayMetadata) -> str | None:
    """Return the date used for replay filtering."""

    if len(resolved.received_at) >= 10:
        return resolved.received_at[:10]
    prefix = record.text_path.name.split("__", 1)[0]
    if len(prefix) == 10 and prefix[4] == "-" and prefix[7] == "-":
        return prefix
    return None


def _file_sha256(path: Path) -> str:
    """Return SHA-256 of one file."""

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _payload_sha256(payload: dict[str, Any]) -> str:
    """Return SHA-256 of one JSON payload."""

    encoded = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    """Read one JSON file."""

    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_string(metadata: dict[str, Any], key: str) -> str | None:
    """Return one optional stripped string value."""

    value = metadata.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _source_root(source: str) -> Path:
    """Return one replay archive root."""

    if source == "raw":
        return REPO_ROOT / "records" / "raw" / "whatsapp"
    return REPO_ROOT / "records" / "rejected" / "whatsapp"


def _manifest_source(records: list[ArchivedRecord]) -> str:
    """Return raw, rejected, or mixed for the manifest."""

    if not records:
        return "raw"
    sources = {record.source for record in records}
    if len(sources) == 1:
        return next(iter(sources))
    return "mixed"


def _format_console_result(*, index: int, total: int, result: dict[str, Any]) -> str:
    """Format one console progress line."""

    detail_parts = []
    written_count = result.get("written_count")
    if isinstance(written_count, int):
        detail_parts.append(f"written={written_count}")
    segment_count = result.get("segment_count")
    if isinstance(segment_count, int):
        detail_parts.append(f"segments={segment_count}")
    detail_suffix = f" | {' | '.join(detail_parts)}" if detail_parts else ""

    return (
        f"[{index}/{total}] {result['file']} | status={result['status']} | "
        f"agent={result['agent'] or 'none'} | output={result['output_path'] or 'none'}{detail_suffix} | "
        f"reason={result['reason']}"
    )


def _result_written_count(result: dict[str, Any]) -> int:
    """Return the structured artifact count represented by one manifest result."""

    written_count = result.get("written_count")
    if isinstance(written_count, int) and written_count >= 0:
        return written_count
    output_paths = result.get("output_paths")
    if result.get("status") == "structured_written" and isinstance(output_paths, list):
        return len(output_paths)
    return 1 if result.get("status") == "structured_written" else 0


def _mixed_result_details(
    *,
    result: AgentResult,
    structured_artifacts: list[StructuredArtifact],
) -> dict[str, Any]:
    """Return extra mixed-report debug metadata for one replay manifest result."""

    payload = result.payload if isinstance(result.payload, dict) else {}
    fanout = payload.get("fanout")
    if result.agent_name != "orchestrator_agent" or not isinstance(fanout, dict):
        return {}

    children = fanout.get("children")
    if not isinstance(children, list):
        return {}

    return {
        "segment_count": len(children),
        "derived_output_paths": [_display_path(artifact.path) for artifact in structured_artifacts],
        "segments": [
            {
                "segment_id": child.get("segment_id"),
                "report_family": child.get("report_family"),
                "branch": _segment_branch(child),
                "report_date": _segment_report_date(child),
                "status": child.get("status"),
                "output_paths": child.get("output_paths") if isinstance(child.get("output_paths"), list) else [],
            }
            for child in children
            if isinstance(child, Mapping)
        ],
    }


def _segment_branch(child: Mapping[str, Any]) -> str | None:
    """Return one mixed child branch from summary or payload."""

    branch = child.get("branch")
    if isinstance(branch, str) and branch.strip():
        return branch.strip()
    payload = child.get("payload")
    if isinstance(payload, Mapping):
        payload_branch = payload.get("branch")
        if isinstance(payload_branch, str) and payload_branch.strip():
            return payload_branch.strip()
    return None


def _segment_report_date(child: Mapping[str, Any]) -> str | None:
    """Return one mixed child report date from summary or payload."""

    report_date = child.get("report_date")
    if isinstance(report_date, str) and report_date.strip():
        return report_date.strip()
    payload = child.get("payload")
    if isinstance(payload, Mapping):
        payload_report_date = payload.get("report_date")
        if isinstance(payload_report_date, str) and payload_report_date.strip():
            return payload_report_date.strip()
    return None


def _run_id() -> str:
    """Return one stable replay run id."""

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_timestamp() -> str:
    """Return a stable UTC ISO timestamp."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _duration_ms(started: datetime) -> int:
    """Return elapsed milliseconds from a UTC datetime."""

    return int((datetime.now(timezone.utc) - started).total_seconds() * 1000)


def _display_path(path: Path) -> str:
    """Return a repo-relative path when possible."""

    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
