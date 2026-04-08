"""Worker for branch comparison analytics output generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from analytics.phase3 import build_branch_comparison, write_branch_comparison_json
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "branch_comparison_agent"
SIGNAL_TYPE = "branch_comparison_analytics"


@dataclass(slots=True)
class BranchComparisonAgentWorker:
    """Generate one branch comparison analytics artifact."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem, overwrite: bool = False) -> AgentResult:
    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    report_date = payload.get("report_date") or payload.get("date")
    root = payload.get("root")
    overwrite = overwrite or payload.get("overwrite") is True
    branches = payload.get("branches")

    if not isinstance(report_date, str):
        return _failure_result("report_date is required for branch comparison analytics.")

    if not isinstance(branches, list):
        branches = None

    analytics_payload = build_branch_comparison(report_date, root=root, branches=branches)
    output_path = write_branch_comparison_json(
        analytics_payload,
        output_root=_root_path_or_none(root),
        overwrite=overwrite,
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
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
