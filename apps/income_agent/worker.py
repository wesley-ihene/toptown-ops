"""Worker placeholders for the income specialist agent."""

from dataclasses import dataclass


@dataclass(slots=True)
class IncomeAgentWorker:
    """Minimal importable income agent placeholder."""

    agent_name: str = "income_agent"
