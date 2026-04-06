"""Worker placeholders for the HR specialist agent."""

from dataclasses import dataclass


@dataclass(slots=True)
class HrAgentWorker:
    """Minimal importable HR agent placeholder."""

    agent_name: str = "hr_agent"
