"""Tests for deterministic cross-branch query routing."""

from __future__ import annotations

from apps.command_router.worker import route_whatsapp_command
from apps.cross_branch_router import worker


def test_parse_cross_branch_query_detects_sales_rank() -> None:
    routed = worker.parse_cross_branch_query("rank sales for waigani on 2026-04-23", sender_phone="67570000000")

    assert routed is not None
    assert routed["is_command"] is True
    assert routed["command_name"] == "cross_branch_query"
    assert routed["query_type"] == "branch_sales_rank"
    assert routed["branch"] == "waigani"
    assert routed["report_date"] == "2026-04-23"
    assert routed["requires_supervisor_auth"] is True


def test_route_whatsapp_command_detects_cross_branch_query() -> None:
    routed = route_whatsapp_command("rank ops for lae malaita", sender_phone="67570000000")

    assert routed["is_command"] is True
    assert routed["command_name"] == "cross_branch_query"
    assert routed["query_type"] == "branch_operational_rank"
    assert routed["branch"] == "lae_malaita"


def test_parse_cross_branch_query_rejects_unknown_metric() -> None:
    assert worker.parse_cross_branch_query("rank margin for waigani") is None


def test_parse_cross_branch_query_ignores_replay() -> None:
    assert worker.parse_cross_branch_query("rank sales for waigani", is_replay=True) is None
