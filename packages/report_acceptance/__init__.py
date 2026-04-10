"""Acceptance decisions layered on top of report validation."""

from .layer import AcceptanceResult, decide_acceptance

__all__ = ["AcceptanceResult", "decide_acceptance"]
