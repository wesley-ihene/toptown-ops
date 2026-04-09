"""Shared writer for downstream normalized signal handoff files."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path

from packages.branch_registry import canonical_branch_slug
from packages.common.paths import REPO_ROOT
from packages.signal_contracts.agent_result import AgentResult

LOGGER = logging.getLogger(__name__)
IOI_COLONY_ROOT = REPO_ROOT.parent / "ioi-colony"
SIGNALS_ROOT = IOI_COLONY_ROOT / "SIGNALS" / "normalized"


def write_signal(result: AgentResult) -> str:
    """Write one specialist result payload into the canonical Colony signal inbox."""

    try:
        payload = result.payload if isinstance(result.payload, dict) else {}
        signal_type = _required_text(payload.get("signal_type"), field_name="signal_type")
        branch = canonical_branch_slug(_required_text(payload.get("branch"), field_name="branch"))
        report_date = _normalize_report_date(_required_text(payload.get("report_date"), field_name="report_date"))

        output_dir = SIGNALS_ROOT / branch / report_date
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = output_dir / f"{timestamp}__{branch}__{signal_type}.json"
        if output_path.exists():
            raise FileExistsError(f"{output_path} already exists.")

        serialized_payload = json.dumps(payload, indent=2, sort_keys=True)
        output_path.write_text(serialized_payload + "\n", encoding="utf-8")
        return str(output_path)
    except Exception:
        LOGGER.exception(
            "Failed to write normalized signal for agent `%s`.",
            getattr(result, "agent_name", "unknown_agent"),
        )
        return ""


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required.")
    return value.strip()


def _normalize_report_date(report_date: str) -> str:
    cleaned = report_date.strip()
    for pattern in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y", "%d-%m-%y", "%d-%m-%Y"):
        try:
            return datetime.strptime(cleaned, pattern).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Invalid report_date {report_date!r}; expected one supported date format.")
