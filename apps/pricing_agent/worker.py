"""Legacy placeholder for the pre-TAOP pricing specialist agent.

Runtime ownership note:
- Live pricing/stock-release processing is owned by
  ``apps.pricing_stock_release_agent.worker``.
- This placeholder remains importable for legacy compatibility only.
"""

from dataclasses import dataclass

RUNTIME_STATUS = "LEGACY_COMPAT"
RUNTIME_OWNER = "pricing_stock_release_agent"
RUNTIME_NOTE = (
    "Legacy placeholder only; live pricing and stock-release processing is "
    "owned by apps.pricing_stock_release_agent.worker."
)


@dataclass(slots=True)
class PricingAgentWorker:
    """Minimal importable pricing agent placeholder."""

    agent_name: str = "pricing_agent"
