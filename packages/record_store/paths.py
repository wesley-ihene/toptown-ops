"""Canonical repository paths for stored operational records."""

from __future__ import annotations

from pathlib import Path

from packages.common.paths import REPO_ROOT

from .naming import build_structured_filename, safe_segment

RECORDS_DIR = REPO_ROOT / "records"
RAW_WHATSAPP_DIR = RECORDS_DIR / "raw" / "whatsapp"
STRUCTURED_DIR = RECORDS_DIR / "structured"
INTELLIGENCE_DIR = RECORDS_DIR / "intelligence"
REJECTED_DIR = RECORDS_DIR / "rejected" / "whatsapp"
DUPLICATES_DIR = RECORDS_DIR / "duplicates" / "whatsapp"
REVIEW_DIR = RECORDS_DIR / "review"
FEEDBACK_DIR = RECORDS_DIR / "feedback"
PROVENANCE_DIR = RECORDS_DIR / "provenance"
PROPOSALS_DIR = RECORDS_DIR / "proposals"
OBSERVABILITY_DIR = RECORDS_DIR / "observability"
ACTIONS_DIR = RECORDS_DIR / "actions"
_INTELLIGENCE_SIGNAL_TYPES = frozenset({"supervisor_control"})


def get_raw_path(report_type: str) -> Path:
    """Return the base path for raw WhatsApp reports of one type."""

    return RAW_WHATSAPP_DIR / safe_segment(report_type)


def get_structured_path(signal_type: str, branch: str, date: str) -> Path:
    """Return the canonical JSON path for one persisted record."""

    return get_structured_path_for_root(
        STRUCTURED_DIR,
        signal_type=signal_type,
        branch=branch,
        date=date,
    )


def get_structured_path_for_root(root: Path, signal_type: str, branch: str, date: str) -> Path:
    """Return the canonical JSON path for one persisted record under a specific root."""

    storage_root = _storage_root_for_signal_type(root, signal_type)
    if is_intelligence_signal_type(signal_type):
        return get_intelligence_path_for_root(
            storage_root,
            signal_type=signal_type,
            branch=branch,
            date=date,
        )

    return (
        storage_root
        / safe_segment(signal_type)
        / safe_segment(branch)
        / build_structured_filename(date)
    )


def get_intelligence_path(signal_type: str, branch: str, date: str) -> Path:
    """Return the canonical JSON path for one intelligence record."""

    return get_intelligence_path_for_root(
        INTELLIGENCE_DIR,
        signal_type=signal_type,
        branch=branch,
        date=date,
    )


def get_intelligence_path_for_root(root: Path, signal_type: str, branch: str, date: str) -> Path:
    """Return the canonical JSON path for one intelligence record under a specific root."""

    return (
        root
        / safe_segment(signal_type)
        / build_structured_filename(date).removesuffix(".json")
        / f"{safe_segment(branch)}.json"
    )


def get_legacy_structured_path(signal_type: str, branch: str, date: str) -> Path:
    """Return the legacy structured-store path for one record."""

    return get_legacy_structured_path_for_root(
        STRUCTURED_DIR,
        signal_type=signal_type,
        branch=branch,
        date=date,
    )


def get_legacy_structured_path_for_root(root: Path, signal_type: str, branch: str, date: str) -> Path:
    """Return the legacy structured-store path for one record under a specific root."""

    return (
        root
        / safe_segment(signal_type)
        / safe_segment(branch)
        / build_structured_filename(date)
    )


def get_rejected_path(report_type: str) -> Path:
    """Return the base path for rejected reports of one type."""

    return REJECTED_DIR / safe_segment(report_type)


def get_duplicate_archive_dir(
    archive_date: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    """Return the duplicate-archive directory for one UTC archive date."""

    duplicates_dir = DUPLICATES_DIR if output_root is None else Path(output_root) / "records" / "duplicates" / "whatsapp"
    return duplicates_dir / safe_segment(archive_date)


def get_review_path(
    date: str,
    branch: str,
    report_type: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    """Return the base path for review items under date/branch/report_type."""

    review_dir = REVIEW_DIR if output_root is None else Path(output_root) / "records" / "review"
    return (
        review_dir
        / safe_segment(date)
        / safe_segment(branch)
        / safe_segment(report_type)
    )


def get_feedback_path(
    report_date: str,
    branch: str,
    action_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    """Return the canonical JSON path for one operator feedback artifact."""

    feedback_dir = FEEDBACK_DIR if output_root is None else Path(output_root) / "records" / "feedback"
    return (
        feedback_dir
        / report_date
        / safe_segment(branch)
        / f"{safe_segment(action_id)}.json"
    )


def get_provenance_path(outcome: str, date: str, branch: str, report_type: str) -> Path:
    """Return the base path for provenance records by outcome/date/branch/report type."""

    return (
        PROVENANCE_DIR
        / safe_segment(outcome)
        / safe_segment(date)
        / safe_segment(branch)
        / safe_segment(report_type)
    )


def get_proposal_path(generated_date: str, report_type: str, proposal_type: str) -> Path:
    """Return the base path for generated learning proposals."""

    return (
        PROPOSALS_DIR
        / safe_segment(generated_date)
        / safe_segment(report_type)
        / safe_segment(proposal_type)
    )


def get_observability_summary_path(report_date: str) -> Path:
    """Return the daily observability summary artifact path."""

    return OBSERVABILITY_DIR / "daily" / safe_segment(report_date) / "summary.json"


def get_action_path(
    report_date: str,
    branch: str,
    action_type: str,
    action_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    """Return the canonical JSON path for one autonomous action artifact."""

    actions_dir = ACTIONS_DIR if output_root is None else Path(output_root) / "records" / "actions"
    return (
        actions_dir
        / report_date
        / safe_segment(branch)
        / safe_segment(action_type)
        / f"{safe_segment(action_id)}.json"
    )


def get_action_preview_path(
    report_date: str,
    branch: str,
    action_type: str,
    action_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    """Return the canonical WhatsApp preview path for one autonomous action artifact."""

    return get_action_path(
        report_date,
        branch,
        action_type,
        action_id,
        output_root=output_root,
    ).with_suffix(".whatsapp.txt")


def raw_whatsapp_records_dir(record_type: str | None = None) -> Path:
    """Backward-compatible raw directory helper."""

    if record_type is None:
        return RAW_WHATSAPP_DIR
    return get_raw_path(record_type)


def structured_records_dir(
    record_type: str | None = None,
    branch: str | None = None,
) -> Path:
    """Backward-compatible structured directory helper."""

    path = INTELLIGENCE_DIR if record_type is not None and is_intelligence_signal_type(record_type) else STRUCTURED_DIR
    if record_type is not None:
        path /= safe_segment(record_type)
    if branch is not None:
        path /= safe_segment(branch)
    return path


def structured_record_path(record_type: str, branch: str, record_date: str) -> Path:
    """Backward-compatible structured path helper."""

    return get_structured_path(record_type, branch, record_date)


def rejected_records_dir(record_type: str | None = None) -> Path:
    """Backward-compatible rejected directory helper."""

    if record_type is None:
        return REJECTED_DIR
    return get_rejected_path(record_type)


def is_intelligence_signal_type(signal_type: str) -> bool:
    """Return whether one signal type is stored under the intelligence tree."""

    return safe_segment(signal_type) in _INTELLIGENCE_SIGNAL_TYPES


def _storage_root_for_signal_type(root: Path, signal_type: str) -> Path:
    """Return the storage root that should hold one signal type for a caller root."""

    if not is_intelligence_signal_type(signal_type):
        return root
    if root == STRUCTURED_DIR:
        return INTELLIGENCE_DIR
    if root.name == STRUCTURED_DIR.name:
        return root.parent / INTELLIGENCE_DIR.name
    return root
