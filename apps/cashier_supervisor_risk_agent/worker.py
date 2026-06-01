"""Worker for cashier and supervisor retail-adjustment risk intelligence."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from apps.cashier_supervisor_risk_agent.rules import extract_record_signals
from apps.cashier_supervisor_risk_agent.scoring import build_risk_entries, build_summary
from apps.cashier_supervisor_risk_agent.storage import (
    iso_timestamp,
    load_sales_record_payloads,
    write_branch_audit,
    write_daily_audit,
    write_dashboard_summary,
    write_risk_payload,
)
from packages.common.branch import canonical_branch_slug_or_none
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "cashier_supervisor_risk_agent"
SIGNAL_TYPE = "cashier_supervisor_risk"
RUNTIME_STATUS = "MANUAL_ONLY"
RUNTIME_OWNER = "scripts/build_cashier_supervisor_risk.py"
RUNTIME_NOTE = "Post-processing risk intelligence only; does not alter sales acceptance."


@dataclass(slots=True)
class CashierSupervisorRiskAgentWorker:
    """Build risk intelligence from structured sales records."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Build cashier and supervisor risk outputs for the requested filter."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    try:
        request = _validated_request(payload)
    except ValueError as error:
        return AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "source_agent": AGENT_NAME,
                "status": "invalid_input",
                "warnings": [
                    {
                        "code": "missing_fields",
                        "severity": "error",
                        "message": str(error),
                    }
                ],
            },
        )

    result = build_risk_outputs(
        start_date=request["start_date"],
        end_date=request["end_date"],
        branch=request["branch"],
        output_root=request["output_root"],
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "status": "written",
            **result,
        },
    )


def build_risk_outputs(
    *,
    start_date: str,
    end_date: str,
    branch: str | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build and persist all cashier/supervisor risk intelligence artifacts."""

    observations = []
    participations = []
    for source_path, payload in load_sales_record_payloads(
        start_date=start_date,
        end_date=end_date,
        branch=branch,
        output_root=output_root,
    ):
        extraction = extract_record_signals(payload, source_path)
        observations.extend(extraction.observations)
        participations.extend(extraction.participations)

    entries = build_risk_entries(observations, participations)
    generated_at = iso_timestamp()
    risk_payload = {
        "generated_at": generated_at,
        "entries": entries,
    }
    aggregate_path = write_risk_payload(risk_payload, output_root=output_root)

    daily_paths = _write_daily_audits(
        entries,
        generated_at=generated_at,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    branch_paths = _write_branch_audits(
        entries,
        generated_at=generated_at,
        branch_filter=branch,
        output_root=output_root,
    )
    dashboard_payload = {
        "generated_at": generated_at,
        "date_range": {
            "start": start_date,
            "end": end_date,
        },
        "branch_filter": branch,
        "summary": build_summary(entries),
        "entries": entries,
    }
    summary_path = write_dashboard_summary(dashboard_payload, output_root=output_root)

    return {
        "generated_at": generated_at,
        "date_range": {"start": start_date, "end": end_date},
        "branch_filter": branch,
        "entry_count": len(entries),
        "output_paths": {
            "aggregate": str(aggregate_path),
            "daily": daily_paths,
            "branch": branch_paths,
            "summary": str(summary_path),
        },
        "entries": entries,
        "summary": dashboard_payload["summary"],
    }


def _write_daily_audits(
    entries: list[dict[str, Any]],
    *,
    generated_at: str,
    start_date: str,
    end_date: str,
    output_root: str | Path | None = None,
) -> list[str]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        for report_date in entry.get("distinct_dates", []):
            grouped[str(report_date)].append(entry)
    if not grouped:
        for report_date in _date_range(start_date, end_date):
            grouped[report_date] = []

    written: list[str] = []
    for report_date, date_entries in sorted(grouped.items()):
        payload = {
            "generated_at": generated_at,
            "report_date": report_date,
            "summary": build_summary(date_entries),
            "entries": date_entries,
        }
        written.append(str(write_daily_audit(report_date, payload, output_root=output_root)))
    return written


def _write_branch_audits(
    entries: list[dict[str, Any]],
    *,
    generated_at: str,
    branch_filter: str | None,
    output_root: str | Path | None = None,
) -> list[str]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[str(entry.get("branch") or "unknown")].append(entry)
    if not grouped:
        grouped[branch_filter or "all"] = []

    written: list[str] = []
    for branch, branch_entries in sorted(grouped.items()):
        payload = {
            "generated_at": generated_at,
            "branch": branch,
            "summary": build_summary(branch_entries),
            "entries": branch_entries,
        }
        written.append(str(write_branch_audit(branch, payload, output_root=output_root)))
    return written


def _validated_request(payload: dict[str, Any]) -> dict[str, Any]:
    branch = payload.get("branch")
    root = payload.get("root") or payload.get("output_root")
    date = payload.get("date")
    start_date = payload.get("start_date") or payload.get("start")
    end_date = payload.get("end_date") or payload.get("end")

    if date is not None:
        start_date = end_date = date
    if not isinstance(start_date, str) or not isinstance(end_date, str):
        raise ValueError("Provide either date or both start_date and end_date.")

    start_date = _validate_iso_date(start_date)
    end_date = _validate_iso_date(end_date)
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date.")

    canonical_branch = None
    if branch is not None:
        if not isinstance(branch, str):
            raise ValueError("branch must be a string when provided.")
        canonical_branch = canonical_branch_slug_or_none(branch)
        if canonical_branch is None:
            raise ValueError("branch must resolve to a canonical branch slug.")

    return {
        "start_date": start_date,
        "end_date": end_date,
        "branch": canonical_branch,
        "output_root": Path(root) if isinstance(root, (str, Path)) else None,
    }


def _validate_iso_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    current: date = start
    dates: list[str] = []
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates
