"""Deterministic TAOP operational-query responses backed by stored records."""

from __future__ import annotations

from collections.abc import Mapping
import json
import logging
from pathlib import Path
from typing import Any

from packages.normalization.branches import normalize_branch
from packages.normalization.dates import normalize_report_date
from packages.record_store.paths import get_structured_path_for_root

_BRANCH_ORDER = ("waigani", "lae_5th_street", "lae_malaita", "bena_road")
_BRANCH_DISPLAY = {
    "waigani": "Waigani",
    "lae_5th_street": "LAE 5th Street",
    "lae_malaita": "LAE Malaita",
    "bena_road": "Bena Road",
}
_QUERY_CONFIG = {
    "attendance_receipt_status": {
        "header": "📋 TAOP ATTENDANCE RECEIPT STATUS",
        "report_type": "attendance_status",
        "signal_type": "hr_attendance",
        "label": "Attendance",
    },
    "bale_summary_receipt_status": {
        "header": "📋 TAOP BALE SUMMARY RECEIPT STATUS",
        "report_type": "bale_summary_status",
        "signal_type": "pricing_stock_release",
        "label": "Daily Bale Summary",
    },
}
_DUPLICATE_TYPE_ALIASES = {
    "hr_attendance": {"attendance", "staff_attendance", "hr_attendance"},
    "pricing_stock_release": {"bale_summary", "pricing_stock_release", "bale_release"},
    "sales_income": {"sales", "sales_income", "day_end_sales"},
}
_DUPLICATE_REASON_CODES = {
    "duplicate_message",
    "duplicate_message_id",
    "duplicate_raw_sha256",
    "duplicate_semantic",
    "conflicting_record_same_scope",
}
LOGGER = logging.getLogger(__name__)


def build_operational_query_response(
    intent: Mapping[str, Any],
    workspace_root: Path,
) -> dict[str, Any]:
    """Build a deterministic response context for one supported operational query."""

    query_type = _text(intent.get("query_type"))
    report_date = _text(intent.get("date"))
    branch = _text(intent.get("branch"))
    if query_type is None:
        raise ValueError("Operational query intent is missing `query_type`.")
    if report_date is None:
        raise ValueError("Operational query intent is missing `date`.")

    normalized_branch = _normalized_branch(branch) or branch
    normalized_report_date = _normalized_date(report_date) or report_date

    if query_type in _QUERY_CONFIG:
        return _single_report_status_response(
            query_type=query_type,
            branch=normalized_branch,
            report_date=normalized_report_date,
            workspace_root=workspace_root,
        )
    if query_type == "report_receipt_status":
        return _combined_report_status_response(
            branch=normalized_branch,
            report_date=normalized_report_date,
            workspace_root=workspace_root,
        )

    return {
        "response_type": "operational_query_status",
        "response_text": "\n".join(
            [
                "⚠️ TAOP STATUS QUERY NEEDS REPORT TYPE",
                f"Date: {_display_date(normalized_report_date)}",
                "",
                "SUPPORTED STATUS QUERIES",
                "1. ATTENDANCE REPORT STATUS",
                "2. DAILY BALE SUMMARY STATUS",
                "",
                "ACTION",
                "Ask, for example: confirm received attendance for today.",
            ]
        ),
        "report_type": "report_status",
        "branch": normalized_branch,
        "report_date": normalized_report_date,
        "governance_status": "fallback",
        "reason": "unsupported_query_type",
        "should_reply": True,
        "dispatch_allowed": True,
        "observability_outcome": "fallback",
        "feedback": {
            "message_intent": "operational_query",
            "query_type": query_type,
            "status": "fallback",
            "date": normalized_report_date,
            "branch": normalized_branch,
            "report_checks": [],
        },
    }


def _single_report_status_response(
    *,
    query_type: str,
    branch: str | None,
    report_date: str,
    workspace_root: Path,
) -> dict[str, Any]:
    config = _QUERY_CONFIG[query_type]
    branches = _target_branches(branch)
    status = _status_for_signal(
        signal_type=str(config["signal_type"]),
        report_date=report_date,
        branches=branches,
        workspace_root=workspace_root,
    )
    if query_type == "attendance_receipt_status":
        _log_attendance_query(
            report_date=report_date,
            found_branches=status["received_branches"],
            missing_branches=status["missing_branches"],
        )
    response_text = _render_single_status(
        header=str(config["header"]),
        report_date=report_date,
        status=status,
    )
    return {
        "response_type": "operational_query_status",
        "response_text": response_text,
        "report_type": str(config["report_type"]),
        "branch": branch,
        "report_date": report_date,
        "governance_status": status["status_code"],
        "reason": None,
        "should_reply": True,
        "dispatch_allowed": True,
        "observability_outcome": "success",
        "feedback": {
            "message_intent": "operational_query",
            "query_type": query_type,
            "status": status["status_code"],
            "date": report_date,
            "branch": branch,
            "report_checks": [
                {
                    "report_type": str(config["report_type"]),
                    "signal_type": str(config["signal_type"]),
                    "status": status["status_code"],
                    "received_branches": status["received_branches"],
                    "missing_branches": status["missing_branches"],
                }
            ],
        },
    }


def _combined_report_status_response(
    *,
    branch: str | None,
    report_date: str,
    workspace_root: Path,
) -> dict[str, Any]:
    attendance = _status_for_signal(
        signal_type="hr_attendance",
        report_date=report_date,
        branches=_target_branches(branch),
        workspace_root=workspace_root,
    )
    bale = _status_for_signal(
        signal_type="pricing_stock_release",
        report_date=report_date,
        branches=_target_branches(branch),
        workspace_root=workspace_root,
    )
    sections = [
        "📋 TAOP REPORT RECEIPT STATUS",
        f"Date: {_display_date(report_date)}",
        "",
        "ATTENDANCE",
        *_render_status_section(attendance),
        "",
        "DAILY BALE SUMMARY",
        *_render_status_section(bale),
    ]
    if attendance["missing_branches"] or bale["missing_branches"]:
        sections.extend(
            [
                "",
                "ACTION",
                "Please resend any missing reports if they were already submitted.",
            ]
        )
        status_code = "partial"
    else:
        sections.extend(["", "No further action required."])
        status_code = "all_received"
    return {
        "response_type": "operational_query_status",
        "response_text": "\n".join(sections),
        "report_type": "report_status",
        "branch": branch,
        "report_date": report_date,
        "governance_status": status_code,
        "reason": None,
        "should_reply": True,
        "dispatch_allowed": True,
        "observability_outcome": "success",
        "feedback": {
            "message_intent": "operational_query",
            "query_type": "report_receipt_status",
            "status": status_code,
            "date": report_date,
            "branch": branch,
            "report_checks": [
                {
                    "report_type": "attendance_status",
                    "signal_type": "hr_attendance",
                    "status": attendance["status_code"],
                    "received_branches": attendance["received_branches"],
                    "missing_branches": attendance["missing_branches"],
                },
                {
                    "report_type": "bale_summary_status",
                    "signal_type": "pricing_stock_release",
                    "status": bale["status_code"],
                    "received_branches": bale["received_branches"],
                    "missing_branches": bale["missing_branches"],
                },
            ],
        },
    }


def _status_for_signal(
    *,
    signal_type: str,
    report_date: str,
    branches: tuple[str, ...],
    workspace_root: Path,
) -> dict[str, Any]:
    structured_root = workspace_root / "records" / "structured"
    duplicate_branches = _duplicate_visibility_branches(
        signal_type=signal_type,
        report_date=report_date,
        branches=branches,
        workspace_root=workspace_root,
    )
    received: list[str] = []
    missing: list[str] = []
    for branch in branches:
        path = get_structured_path_for_root(structured_root, signal_type=signal_type, branch=branch, date=report_date)
        if path.exists() or branch in duplicate_branches:
            received.append(branch)
        else:
            missing.append(branch)

    if missing and received:
        status_line = "⚠️ PARTIAL / NEEDS CHECK"
        status_code = "partial"
    elif missing and not received:
        status_line = "❌ NOT FOUND"
        status_code = "not_found"
    elif len(branches) == 1:
        status_line = "✅ RECEIVED"
        status_code = "received"
    else:
        status_line = "✅ ALL RECEIVED"
        status_code = "all_received"

    return {
        "status_line": status_line,
        "status_code": status_code,
        "received_branches": received,
        "missing_branches": missing,
    }


def _duplicate_visibility_branches(
    *,
    signal_type: str,
    report_date: str,
    branches: tuple[str, ...],
    workspace_root: Path,
) -> set[str]:
    aliases = _DUPLICATE_TYPE_ALIASES.get(signal_type)
    if not aliases:
        return set()

    target_date = _normalized_date(report_date)
    if target_date is None:
        return set()

    candidate_branches = set(branches)
    found: set[str] = set()
    raw_root = workspace_root / "records" / "raw" / "whatsapp"
    if not raw_root.exists():
        return found

    for meta_path in raw_root.rglob("*.meta.json"):
        payload = _read_json(meta_path)
        if not _is_duplicate_payload(payload):
            continue
        branch = _normalized_branch(payload.get("branch_hint"))
        if branch is None or branch not in candidate_branches:
            continue
        metadata_date = _normalized_date(
            payload.get("normalized_report_date")
            or payload.get("resolved_report_date")
            or payload.get("report_date")
            or payload.get("raw_report_date")
        )
        if metadata_date != target_date:
            continue
        if not _matches_duplicate_signal(payload, aliases):
            continue
        found.add(branch)
    return found


def _is_duplicate_payload(payload: Mapping[str, Any]) -> bool:
    processing_status = _text(payload.get("processing_status"))
    if processing_status == "duplicate":
        return True
    governance_status = _text(payload.get("governance_status"))
    if governance_status == "duplicate":
        return True
    policy_guard = payload.get("policy_guard")
    if isinstance(policy_guard, Mapping):
        if policy_guard.get("duplicate") is True:
            return True
        reason = _text(policy_guard.get("reason"))
        if reason in _DUPLICATE_REASON_CODES:
            return True
    return False


def _matches_duplicate_signal(payload: Mapping[str, Any], aliases: set[str]) -> bool:
    for field_name in ("specialist_report_type", "detected_report_type", "attempted_report_type"):
        value = _text(payload.get(field_name))
        if value is None:
            continue
        normalized = value.strip().casefold().replace("-", "_").replace(" ", "_")
        if normalized in aliases:
            return True
    return False


def _log_attendance_query(
    *,
    report_date: str,
    found_branches: list[str],
    missing_branches: list[str],
) -> None:
    LOGGER.info(
        json.dumps(
            {
                "query_type": "attendance_status",
                "date": report_date,
                "found_branches": found_branches,
                "missing_branches": missing_branches,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _normalized_branch(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    normalized = normalize_branch(text).normalized_value
    return normalized if isinstance(normalized, str) and normalized.strip() else None


def _normalized_date(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    normalized = normalize_report_date(text).normalized_value
    return normalized if isinstance(normalized, str) and normalized.strip() else None


def _render_single_status(
    *,
    header: str,
    report_date: str,
    status: Mapping[str, Any],
) -> str:
    lines = [
        header,
        f"Date: {_display_date(report_date)}",
        "",
        f"STATUS: {status['status_line']}",
        "",
        *_render_status_section(status),
    ]
    if status["missing_branches"]:
        lines.extend(
            [
                "",
                "ACTION",
                "Please resend the missing report if it was already submitted.",
            ]
        )
    else:
        lines.extend(["", "No further action required."])
    return "\n".join(lines)


def _render_status_section(status: Mapping[str, Any]) -> list[str]:
    lines = ["RECEIVED"]
    received = status.get("received_branches")
    if isinstance(received, list) and received:
        for branch in received:
            lines.append(f"✔ {_display_branch(branch)}")
    else:
        lines.append("- None")

    missing = status.get("missing_branches")
    if isinstance(missing, list) and missing:
        lines.extend(["", "MISSING / NOT FOUND"])
        for branch in missing:
            lines.append(f"❌ {_display_branch(branch)}")
    return lines


def _target_branches(branch: str | None) -> tuple[str, ...]:
    if branch is not None:
        normalized = _normalized_branch(branch)
        if normalized is not None:
            return (normalized,)
        return (branch,)
    return _BRANCH_ORDER


def _display_branch(branch: str) -> str:
    return _BRANCH_DISPLAY.get(branch, branch.replace("_", " ").title())


def _display_date(value: str) -> str:
    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return f"{value[8:10]}/{value[5:7]}/{value[2:4]}"
    return value


def _text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
