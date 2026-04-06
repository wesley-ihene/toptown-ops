"""State placeholders for Orchestra coordination state."""

from dataclasses import dataclass


@dataclass(slots=True)
class OrchestraState:
    """Minimal importable placeholder for orchestra state."""

    status: str = "pending"
