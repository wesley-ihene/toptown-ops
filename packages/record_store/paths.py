"""Canonical repository paths for stored operational records."""

from __future__ import annotations

from pathlib import Path

from packages.common.paths import REPO_ROOT

from .naming import build_structured_filename, safe_segment

RECORDS_DIR = REPO_ROOT / "records"
RAW_WHATSAPP_DIR = RECORDS_DIR / "raw" / "whatsapp"
STRUCTURED_DIR = RECORDS_DIR / "structured"
REJECTED_DIR = RECORDS_DIR / "rejected" / "whatsapp"
REVIEW_DIR = RECORDS_DIR / "review"
PROVENANCE_DIR = RECORDS_DIR / "provenance"
PROPOSALS_DIR = RECORDS_DIR / "proposals"
OBSERVABILITY_DIR = RECORDS_DIR / "observability"


def get_raw_path(report_type: str) -> Path:
    """Return the base path for raw WhatsApp reports of one type."""

    return RAW_WHATSAPP_DIR / safe_segment(report_type)


def get_structured_path(signal_type: str, branch: str, date: str) -> Path:
    """Return the canonical JSON path for one structured record."""

    return get_structured_path_for_root(
        STRUCTURED_DIR,
        signal_type=signal_type,
        branch=branch,
        date=date,
    )


def get_structured_path_for_root(root: Path, signal_type: str, branch: str, date: str) -> Path:
    """Return the canonical JSON path for one structured record under a specific root."""

    return (
        root
        / safe_segment(signal_type)
        / safe_segment(branch)
        / build_structured_filename(date)
    )


def get_rejected_path(report_type: str) -> Path:
    """Return the base path for rejected reports of one type."""

    return REJECTED_DIR / safe_segment(report_type)


def get_review_path(date: str, branch: str, report_type: str) -> Path:
    """Return the base path for review items under date/branch/report_type."""

    return (
        REVIEW_DIR
        / safe_segment(date)
        / safe_segment(branch)
        / safe_segment(report_type)
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

    path = STRUCTURED_DIR
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
