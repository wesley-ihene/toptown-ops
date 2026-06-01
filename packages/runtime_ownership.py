"""Authoritative registry for current TAOP runtime ownership.

This module is documentation-first. It records which module currently owns each
live runtime capability, which dashboard/API product surfaces are official, and
which parallel modules are intentionally retained as manual-only,
legacy-compatibility, or disabled surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

LIVE_RUNTIME: Final[str] = "LIVE_RUNTIME"
LEGACY_COMPAT: Final[str] = "LEGACY_COMPAT"
MANUAL_ONLY: Final[str] = "MANUAL_ONLY"
DISABLED: Final[str] = "DISABLED"
EXTERNAL_STRATEGIC_SURFACE: Final[str] = "EXTERNAL_STRATEGIC_SURFACE"


@dataclass(frozen=True, slots=True)
class RuntimeCapabilityOwner:
    """One documented live runtime owner for a capability."""

    capability: str
    owner_module: str
    owner_surface: tuple[str, ...]
    runtime_status: str = LIVE_RUNTIME
    notes: str = ""


@dataclass(frozen=True, slots=True)
class RuntimeModuleStatus:
    """One documented status marker for a module or package."""

    module: str
    runtime_status: str
    runtime_owner: str
    notes: str = ""


@dataclass(frozen=True, slots=True)
class DashboardSurface:
    """Document one official dashboard or API surface.

    This registry is metadata-only. It does not alter route handling, bind-port
    behavior, response schemas, or rendering behavior.
    """

    name: str
    service: str
    owner: tuple[str, ...]
    audience: tuple[str, ...]
    status: str
    purpose: str
    route: str | None = None
    route_prefix: str | None = None
    observed_port: int | None = None
    intelligence_source: tuple[str, ...] = ()
    owned_by_taop: bool = True
    notes: str = ""

    @property
    def routing_key(self) -> str:
        """Return the route handle used for uniqueness checks."""

        return self.route if self.route is not None else (self.route_prefix or "")


DASHBOARD_SURFACES: Final[tuple[DashboardSurface, ...]] = (
    DashboardSurface(
        name="TopTown Operational Dashboard",
        service="taop_operational_dashboard",
        route="/dashboard",
        observed_port=8082,
        owner=("analytics/phase4_portal.py",),
        audience=("operations",),
        status=LIVE_RUNTIME,
        purpose=(
            "branch/day operational analytics, sales, staffing, pending actions, "
            "operational KPIs"
        ),
        notes=(
            "Observed port is deployment metadata only. Runtime binding remains "
            "configurable inside analytics.phase4_portal."
        ),
    ),
    DashboardSurface(
        name="TopTown Operational API",
        service="taop_operational_api",
        route_prefix="/api/",
        observed_port=8082,
        owner=(
            "analytics/phase4_portal.py",
            "apps/dashboard_api/routes.py",
            "apps/taop_ops_api/routes.py",
        ),
        audience=("operations", "system"),
        status=LIVE_RUNTIME,
        purpose="JSON analytics and operational action data for dashboards/tools",
        notes=(
            "Observed port is deployment metadata only. Official ownership covers "
            "operational JSON surfaces; deprecated CEO compatibility routes remain "
            "separate behavior under the same HTTP prefix."
        ),
    ),
    DashboardSurface(
        name="CEO Briefing Dashboard",
        service="ceo_briefing_dashboard",
        route="/ceo_briefing",
        owner=("analytics/phase4_portal.py",),
        intelligence_source=(
            "apps/ceo_router/worker.py",
            "apps/executive_engine/worker.py",
        ),
        audience=("executive",),
        status=LIVE_RUNTIME,
        purpose=(
            "executive prioritization, branch ranking, intervention "
            "recommendations"
        ),
        notes=(
            "Ownership metadata only. Current TAOP compatibility routing still "
            "uses /ceo and /ceo/dashboard; registering this surface does not "
            "change those routes."
        ),
    ),
    DashboardSurface(
        name="IOI Colony Dashboard",
        service="ioi_colony_dashboard",
        route="/",
        owner=("external repo: ~/.openclaw/workspace/ioi-colony/scripts/colony_web.py",),
        audience=("strategic intelligence",),
        status=EXTERNAL_STRATEGIC_SURFACE,
        purpose=(
            "opportunity intelligence, signal fusion, reinforcement/decay analysis"
        ),
        owned_by_taop=False,
        notes=(
            "Strategic dashboard lives in the external IOI Colony repo and is "
            "recorded here for boundary clarity only."
        ),
    ),
)

DASHBOARD_SURFACE_REGISTRY: Final[dict[str, DashboardSurface]] = {
    surface.service: surface for surface in DASHBOARD_SURFACES
}


RUNTIME_CAPABILITIES: Final[tuple[RuntimeCapabilityOwner, ...]] = (
    RuntimeCapabilityOwner(
        capability="report_ingestion",
        owner_module="scripts.whatsapp_webhook_bridge",
        owner_surface=("dispatch_http_request", "build_work_item"),
        notes=(
            "Live WhatsApp ingress owner. Raw writes, pre-ingestion validation, "
            "command/query bypasses, and orchestrator handoff all begin here."
        ),
    ),
    RuntimeCapabilityOwner(
        capability="runtime_orchestration",
        owner_module="apps.orchestrator_agent.worker",
        owner_surface=("process_work_item",),
        notes=(
            "Central runtime orchestrator. Owns live specialist dispatch, "
            "fallback, validation, acceptance, provenance, and governed writes."
        ),
    ),
    RuntimeCapabilityOwner(
        capability="mixed_report_splitting",
        owner_module="apps.report_splitter_agent.worker",
        owner_surface=("split_report",),
        notes="Live mixed-report splitter used by orchestrator_agent.",
    ),
    RuntimeCapabilityOwner(
        capability="sales_income_processing",
        owner_module="apps.sales_income_agent.worker",
        owner_surface=("process_work_item",),
        notes="Live specialist owner for sales-income reports.",
    ),
    RuntimeCapabilityOwner(
        capability="hr_attendance_processing",
        owner_module="apps.hr_agent.worker",
        owner_surface=("process_work_item",),
        notes="Live specialist owner for attendance routing.",
    ),
    RuntimeCapabilityOwner(
        capability="staff_performance_processing",
        owner_module="apps.hr_agent.worker",
        owner_surface=("process_work_item", "_process_staff_performance_work_item"),
        notes=(
            "Live staff-performance routing points to hr_agent. The standalone "
            "staff_performance_agent is retained for replay/tests and manual runs."
        ),
    ),
    RuntimeCapabilityOwner(
        capability="pricing_stock_release_processing",
        owner_module="apps.pricing_stock_release_agent.worker",
        owner_surface=("process_work_item",),
        notes="Live specialist owner for pricing and stock release reports.",
    ),
    RuntimeCapabilityOwner(
        capability="supervisor_control_processing",
        owner_module="apps.supervisor_control_agent.worker",
        owner_surface=("process_work_item",),
        notes="Live specialist owner for supervisor control reports.",
    ),
    RuntimeCapabilityOwner(
        capability="adaptive_sop_application",
        owner_module="apps.adaptive_sop_engine.worker",
        owner_surface=("apply_adaptive_sop",),
        notes=(
            "Live adaptive SOP application runs inline from active specialists. "
            "It is a live helper, not a separately dispatched runtime owner."
        ),
    ),
    RuntimeCapabilityOwner(
        capability="analytics_rebuild",
        owner_module="packages.record_store.automation",
        owner_surface=("run_post_write_automation",),
        notes=(
            "Live automated rebuild path. It calls analytics.phase3 directly; "
            "analytics wrapper agents remain manual/operator tools."
        ),
    ),
    RuntimeCapabilityOwner(
        capability="outbound_whatsapp_response",
        owner_module="packages.record_store.automation",
        owner_surface=("generate_whatsapp_conversation_reply", "dispatch_whatsapp_response"),
        notes=(
            "Live response orchestration owner. Rendering and persistence happen "
            "here before transport is delegated to outbound_reply_agent."
        ),
    ),
    RuntimeCapabilityOwner(
        capability="ceo_query_routing",
        owner_module="apps.ceo_router.worker",
        owner_surface=("parse_ceo_query", "handle_ceo_query"),
        notes=(
            "Live CEO query owner. command_router delegates CEO-specific parsing "
            "and reply-context generation here."
        ),
    ),
    RuntimeCapabilityOwner(
        capability="dashboard_api_routing",
        owner_module="analytics.phase4_portal",
        owner_surface=("dispatch_http_request", "serve"),
        notes=(
            "Live read-only dashboard/API router. Operator APIs stay active here "
            "while deprecated CEO surfaces remain compatibility-only."
        ),
    ),
)

CAPABILITY_OWNERS: Final[dict[str, RuntimeCapabilityOwner]] = {
    owner.capability: owner for owner in RUNTIME_CAPABILITIES
}

RUNTIME_MODULES: Final[tuple[RuntimeModuleStatus, ...]] = (
    RuntimeModuleStatus(
        module="apps.orchestrator_agent.worker",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="orchestrator_agent",
        notes="Live runtime orchestrator for intake routing and specialist dispatch.",
    ),
    RuntimeModuleStatus(
        module="scripts.whatsapp_webhook_bridge",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="whatsapp_webhook_bridge",
        notes="Live report-ingestion owner.",
    ),
    RuntimeModuleStatus(
        module="apps.orchestra",
        runtime_status=LEGACY_COMPAT,
        runtime_owner="orchestrator_agent",
        notes="Legacy compatibility/test scaffolding only; not the live orchestrator.",
    ),
    RuntimeModuleStatus(
        module="apps.report_splitter_agent.worker",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="report_splitter_agent",
        notes="Live mixed-report splitter.",
    ),
    RuntimeModuleStatus(
        module="apps.mixed_report_splitter_agent.worker",
        runtime_status=DISABLED,
        runtime_owner="report_splitter_agent",
        notes="Alternate splitter retained on disk but not wired into current runtime.",
    ),
    RuntimeModuleStatus(
        module="apps.adaptive_sop_engine.worker",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="adaptive_sop_engine",
        notes="Live adaptive SOP helper used inline by active specialists.",
    ),
    RuntimeModuleStatus(
        module="apps.hr_agent.worker",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="hr_agent",
        notes="Live owner for attendance and routed staff-performance processing.",
    ),
    RuntimeModuleStatus(
        module="apps.staff_performance_agent.worker",
        runtime_status=MANUAL_ONLY,
        runtime_owner="hr_agent",
        notes="Standalone worker retained for replay/tests and manual specialist runs.",
    ),
    RuntimeModuleStatus(
        module="apps.income_agent.worker",
        runtime_status=LEGACY_COMPAT,
        runtime_owner="sales_income_agent",
        notes="Legacy placeholder only; live sales-income processing uses sales_income_agent.",
    ),
    RuntimeModuleStatus(
        module="apps.pricing_agent.worker",
        runtime_status=LEGACY_COMPAT,
        runtime_owner="pricing_stock_release_agent",
        notes=(
            "Legacy placeholder only; live pricing and stock-release processing uses "
            "pricing_stock_release_agent."
        ),
    ),
    RuntimeModuleStatus(
        module="apps.branch_daily_analytics_agent.worker",
        runtime_status=MANUAL_ONLY,
        runtime_owner="packages.record_store.automation",
        notes="Script/operator wrapper only; live rebuilds use record_store automation.",
    ),
    RuntimeModuleStatus(
        module="apps.staff_leaderboard_agent.worker",
        runtime_status=MANUAL_ONLY,
        runtime_owner="packages.record_store.automation",
        notes="Script/operator wrapper only; live rebuilds use record_store automation.",
    ),
    RuntimeModuleStatus(
        module="apps.section_productivity_agent.worker",
        runtime_status=MANUAL_ONLY,
        runtime_owner="packages.record_store.automation",
        notes="Script/operator wrapper only; live rebuilds use record_store automation.",
    ),
    RuntimeModuleStatus(
        module="apps.branch_comparison_agent.worker",
        runtime_status=MANUAL_ONLY,
        runtime_owner="packages.record_store.automation",
        notes="Script/operator wrapper only; live rebuilds use record_store automation.",
    ),
    RuntimeModuleStatus(
        module="packages.record_store.automation",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="packages.record_store.automation",
        notes="Live owner of automated analytics rebuilds and response orchestration.",
    ),
    RuntimeModuleStatus(
        module="apps.ceo_router.worker",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="ceo_router",
        notes="Live owner of CEO query parsing and reply-context generation.",
    ),
    RuntimeModuleStatus(
        module="analytics.phase4_portal",
        runtime_status=LIVE_RUNTIME,
        runtime_owner="phase4_portal",
        notes="Live read-only portal router for dashboard and API traffic.",
    ),
)

MODULE_RUNTIME_STATUS: Final[dict[str, RuntimeModuleStatus]] = {
    module_status.module: module_status for module_status in RUNTIME_MODULES
}


def capability_owner(capability: str) -> RuntimeCapabilityOwner:
    """Return the documented live owner for one capability."""

    return CAPABILITY_OWNERS[capability]


def module_runtime_status(module: str) -> RuntimeModuleStatus | None:
    """Return the documented runtime status for one module, if recorded."""

    return MODULE_RUNTIME_STATUS.get(module)


def dashboard_surface(service: str) -> DashboardSurface:
    """Return the documented dashboard/API surface for one service."""

    return DASHBOARD_SURFACE_REGISTRY[service]


__all__ = [
    "CAPABILITY_OWNERS",
    "DASHBOARD_SURFACES",
    "DASHBOARD_SURFACE_REGISTRY",
    "DISABLED",
    "EXTERNAL_STRATEGIC_SURFACE",
    "LEGACY_COMPAT",
    "LIVE_RUNTIME",
    "MANUAL_ONLY",
    "MODULE_RUNTIME_STATUS",
    "RUNTIME_CAPABILITIES",
    "RUNTIME_MODULES",
    "DashboardSurface",
    "RuntimeCapabilityOwner",
    "RuntimeModuleStatus",
    "capability_owner",
    "dashboard_surface",
    "module_runtime_status",
]
