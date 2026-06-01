"""Legacy placeholder for the pre-TAOP income specialist agent.

Runtime ownership note:
- Live sales-income processing is owned by ``apps.sales_income_agent.worker``.
- This placeholder remains importable for legacy compatibility only.
"""

from dataclasses import dataclass

RUNTIME_STATUS = "LEGACY_COMPAT"
RUNTIME_OWNER = "sales_income_agent"
RUNTIME_NOTE = (
    "Legacy placeholder only; live sales-income processing is owned by "
    "apps.sales_income_agent.worker."
)


@dataclass(slots=True)
class IncomeAgentWorker:
    """Minimal importable income agent placeholder."""

    agent_name: str = "income_agent"
