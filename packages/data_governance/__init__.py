"""Deterministic governance decisions for structured upstream records."""

from .layer import (
    GOVERNANCE_SIDECAR_SUFFIX,
    EXPORTABLE_FINAL_STATUSES,
    GovernanceDecision,
    GovernedWriteResult,
    build_governance_context,
    govern_record,
    read_governance_sidecar,
)

__all__ = [
    "EXPORTABLE_FINAL_STATUSES",
    "GOVERNANCE_SIDECAR_SUFFIX",
    "GovernanceDecision",
    "GovernedWriteResult",
    "build_governance_context",
    "govern_record",
    "read_governance_sidecar",
]
