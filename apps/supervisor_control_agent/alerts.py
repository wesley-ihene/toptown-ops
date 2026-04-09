"""Warning generation helpers for supervisor control reports."""

from __future__ import annotations

from apps.supervisor_control_agent.controls import ControlsSummary
from apps.supervisor_control_agent.escalation import EscalationSummary
from apps.supervisor_control_agent.exceptions import ExceptionSummary
from apps.supervisor_control_agent.parser import ParsedSupervisorControlReport
from packages.common.warnings import WarningEntry, make_warning


def generate_alerts(
    *,
    parsed: ParsedSupervisorControlReport,
    exceptions: ExceptionSummary,
    controls: ControlsSummary,
    escalation: EscalationSummary,
) -> list[WarningEntry]:
    """Return warnings from parsed and derived supervisor-control state."""

    warnings: list[WarningEntry] = []

    if parsed.branch is None or parsed.report_date is None or not parsed.exception_entries:
        warnings.append(
            make_warning(
                code="missing_fields",
                severity="error",
                message="Branch, report date, and at least one exception entry are required for supervisor control signaling.",
            )
        )

    if exceptions.unknown_exception_type_count > 0:
        warnings.append(
            make_warning(
                code="unknown_exception_type",
                severity="warning",
                message="One or more exception entries used an unknown exception type.",
            )
        )

    if controls.control_gap_count > 0:
        warnings.append(
            make_warning(
                code="control_gap_present",
                severity="warning",
                message="One or more exception entries are missing supervisor confirmation.",
            )
        )

    if any(item.supervisor_confirmed == "UNKNOWN" for item in exceptions.items):
        warnings.append(
            make_warning(
                code="missing_confirmation",
                severity="warning",
                message="One or more exception entries have unknown supervisor confirmation status.",
            )
        )

    if escalation.escalation_required:
        warnings.append(
            make_warning(
                code="escalation_required",
                severity="warning",
                message="One or more supervisor control exceptions remain open or escalated.",
            )
        )

    return warnings
