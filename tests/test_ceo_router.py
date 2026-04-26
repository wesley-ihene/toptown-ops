"""Tests for deterministic CEO assistant routing."""

from __future__ import annotations

from apps.ceo_router import worker
from apps.command_router.worker import route_whatsapp_command


def test_parse_ceo_query_detects_overview() -> None:
    routed = worker.parse_ceo_query("ceo overview on 2026-04-23", sender_phone="67570000000")

    assert routed is not None
    assert routed["is_command"] is True
    assert routed["command_name"] == "ceo_query"
    assert routed["query_type"] == "executive_overview"
    assert routed["report_date"] == "2026-04-23"
    assert routed["requires_supervisor_auth"] is True


def test_route_whatsapp_command_detects_executive_alias() -> None:
    routed = route_whatsapp_command("executive alerts", sender_phone="67570000000")

    assert routed["is_command"] is True
    assert routed["command_name"] == "ceo_query"
    assert routed["query_type"] == "executive_alerts"


def test_parse_ceo_query_rejects_unknown_query() -> None:
    assert worker.parse_ceo_query("ceo actions") is None


def test_parse_ceo_query_ignores_replay() -> None:
    assert worker.parse_ceo_query("ceo overview", is_replay=True) is None
