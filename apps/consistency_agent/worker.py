"""Rules-based consistency agent for additive control checks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from apps.consistency_agent.rules import run_rules
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem
from packages.validation import ValidationMetadata

AGENT_NAME = "consistency_agent"


@dataclass(slots=True)
class ConsistencyAgentWorker:
    """Worker for deterministic structured-record consistency checks."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Process one branch/day consistency check work item."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    branch = _string(payload.get("branch")) or "unknown"
    report_date = _string(payload.get("report_date")) or "unknown"
    records = payload.get("records")
    if not isinstance(records, Mapping):
        metadata = ValidationMetadata(
            stage="consistency_agent",
            status="invalid_input",
            accepted=False,
            rejections=[
                {
                    "reason_code": "missing_records",
                    "reason_detail": "Consistency checks require a records mapping.",
                    "code": "missing_records",
                    "message": "Consistency checks require a records mapping.",
                }
            ],
        )
        return AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "branch": branch,
                "report_date": report_date,
                "status": "invalid_input",
                "issue_count": 0,
                "issues": [],
            },
            metadata={"validation": metadata.to_payload()},
        )

    issues = run_rules(branch=branch, report_date=report_date, records=records)
    metadata = ValidationMetadata(
        stage="consistency_agent",
        status="passed" if not issues else "issues_found",
        accepted=not issues,
        rejections=list(issues),
        details={"record_types": sorted(str(key) for key in records.keys())},
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "branch": branch,
            "report_date": report_date,
            "status": "accepted",
            "issue_count": len(issues),
            "issues": issues,
        },
        metadata={"validation": metadata.to_payload()},
    )


def _string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None
