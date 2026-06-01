"""Tests for the non-behavioral runtime ownership registry."""

from __future__ import annotations

from importlib import import_module

from packages.runtime_ownership import (
    CAPABILITY_OWNERS,
    DISABLED,
    LEGACY_COMPAT,
    LIVE_RUNTIME,
    MANUAL_ONLY,
    MODULE_RUNTIME_STATUS,
)


def test_runtime_capability_registry_matches_current_live_owners() -> None:
    expected = {
        "report_ingestion": (
            "scripts.whatsapp_webhook_bridge",
            ("dispatch_http_request", "build_work_item"),
            LIVE_RUNTIME,
        ),
        "runtime_orchestration": (
            "apps.orchestrator_agent.worker",
            ("process_work_item",),
            LIVE_RUNTIME,
        ),
        "mixed_report_splitting": (
            "apps.report_splitter_agent.worker",
            ("split_report",),
            LIVE_RUNTIME,
        ),
        "sales_income_processing": (
            "apps.sales_income_agent.worker",
            ("process_work_item",),
            LIVE_RUNTIME,
        ),
        "hr_attendance_processing": (
            "apps.hr_agent.worker",
            ("process_work_item",),
            LIVE_RUNTIME,
        ),
        "staff_performance_processing": (
            "apps.hr_agent.worker",
            ("process_work_item", "_process_staff_performance_work_item"),
            LIVE_RUNTIME,
        ),
        "pricing_stock_release_processing": (
            "apps.pricing_stock_release_agent.worker",
            ("process_work_item",),
            LIVE_RUNTIME,
        ),
        "supervisor_control_processing": (
            "apps.supervisor_control_agent.worker",
            ("process_work_item",),
            LIVE_RUNTIME,
        ),
        "adaptive_sop_application": (
            "apps.adaptive_sop_engine.worker",
            ("apply_adaptive_sop",),
            LIVE_RUNTIME,
        ),
        "analytics_rebuild": (
            "packages.record_store.automation",
            ("run_post_write_automation",),
            LIVE_RUNTIME,
        ),
        "outbound_whatsapp_response": (
            "packages.record_store.automation",
            ("generate_whatsapp_conversation_reply", "dispatch_whatsapp_response"),
            LIVE_RUNTIME,
        ),
        "ceo_query_routing": (
            "apps.ceo_router.worker",
            ("parse_ceo_query", "handle_ceo_query"),
            LIVE_RUNTIME,
        ),
        "dashboard_api_routing": (
            "analytics.phase4_portal",
            ("dispatch_http_request", "serve"),
            LIVE_RUNTIME,
        ),
    }

    actual = {
        capability: (owner.owner_module, owner.owner_surface, owner.runtime_status)
        for capability, owner in CAPABILITY_OWNERS.items()
    }

    assert actual == expected


def test_shadow_and_legacy_modules_are_registered_with_expected_statuses() -> None:
    expected = {
        "apps.orchestrator_agent.worker": (LIVE_RUNTIME, "orchestrator_agent"),
        "scripts.whatsapp_webhook_bridge": (LIVE_RUNTIME, "whatsapp_webhook_bridge"),
        "apps.orchestra": (LEGACY_COMPAT, "orchestrator_agent"),
        "apps.report_splitter_agent.worker": (LIVE_RUNTIME, "report_splitter_agent"),
        "apps.mixed_report_splitter_agent.worker": (DISABLED, "report_splitter_agent"),
        "apps.adaptive_sop_engine.worker": (LIVE_RUNTIME, "adaptive_sop_engine"),
        "apps.hr_agent.worker": (LIVE_RUNTIME, "hr_agent"),
        "apps.staff_performance_agent.worker": (MANUAL_ONLY, "hr_agent"),
        "apps.income_agent.worker": (LEGACY_COMPAT, "sales_income_agent"),
        "apps.pricing_agent.worker": (LEGACY_COMPAT, "pricing_stock_release_agent"),
        "apps.branch_daily_analytics_agent.worker": (MANUAL_ONLY, "packages.record_store.automation"),
        "apps.staff_leaderboard_agent.worker": (MANUAL_ONLY, "packages.record_store.automation"),
        "apps.section_productivity_agent.worker": (MANUAL_ONLY, "packages.record_store.automation"),
        "apps.branch_comparison_agent.worker": (MANUAL_ONLY, "packages.record_store.automation"),
        "packages.record_store.automation": (LIVE_RUNTIME, "packages.record_store.automation"),
        "apps.ceo_router.worker": (LIVE_RUNTIME, "ceo_router"),
        "analytics.phase4_portal": (LIVE_RUNTIME, "phase4_portal"),
    }

    actual = {
        module: (status.runtime_status, status.runtime_owner)
        for module, status in MODULE_RUNTIME_STATUS.items()
    }

    assert actual == expected


def test_runtime_status_constants_match_registry() -> None:
    for module_name, registered in MODULE_RUNTIME_STATUS.items():
        module = import_module(module_name)
        assert getattr(module, "RUNTIME_STATUS") == registered.runtime_status
        assert getattr(module, "RUNTIME_OWNER") == registered.runtime_owner
        assert isinstance(getattr(module, "RUNTIME_NOTE"), str)
        assert getattr(module, "RUNTIME_NOTE").strip()
