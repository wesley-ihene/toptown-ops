"""Orchestra application package.

Runtime ownership note:
- Live runtime orchestration is owned by ``apps.orchestrator_agent.worker``.
- This package is retained as legacy compatibility and test scaffolding only.
"""

RUNTIME_STATUS = "LEGACY_COMPAT"
RUNTIME_OWNER = "orchestrator_agent"
RUNTIME_NOTE = (
    "Legacy compatibility and test scaffolding only; live orchestration is "
    "owned by apps.orchestrator_agent.worker."
)
