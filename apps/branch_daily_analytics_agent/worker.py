"""Worker for branch daily analytics output generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from analytics.phase3 import build_branch_daily_analytics, write_branch_daily_analytics_json
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "branch_daily_analytics_agent"
SIGNAL_TYPE = "branch_daily_analytics"


@dataclass(slots=True)
class BranchDailyAnalyticsAgentWorker:
    """Generate one branch daily analytics artifact."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem, overwrite: bool = False) -> AgentResult:
    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    branch = payload.get("branch")
    report_date = payload.get("report_date") or payload.get("date")
    root = payload.get("root")
    overwrite = overwrite or payload.get("overwrite") is True

    if not isinstance(branch, str) or not isinstance(report_date, str):
        return _failure_result("branch and report_date are required for branch daily analytics.")

    analytics_payload = build_branch_daily_analytics(branch, report_date, root=root)
    output_path = write_branch_daily_analytics_json(
        analytics_payload,
        output_root=_root_path_or_none(root),
        overwrite=overwrite,
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "branch": analytics_payload["branch"],
            "report_date": analytics_payload["report_date"],
            "output_path": str(output_path),
            "warnings": analytics_payload["warnings"],
            "status": "written",
            "analytics_payload": analytics_payload,
        },
    )


def _failure_result(message: str) -> AgentResult:
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "branch": None,
            "report_date": None,
            "output_path": None,
            "warnings": [
                {
                    "code": "missing_fields",
                    "severity": "error",
                    "message": message,
                }
            ],
            "status": "invalid_input",
        },
    )


def _root_path_or_none(value: Any) -> Path | None:
    return Path(value) if isinstance(value, (str, Path)) else None
