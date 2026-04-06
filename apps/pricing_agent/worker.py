"""Worker placeholders for the pricing specialist agent."""

from dataclasses import dataclass


@dataclass(slots=True)
class PricingAgentWorker:
    """Minimal importable pricing agent placeholder."""

    agent_name: str = "pricing_agent"
