"""Tests for pipeline latency artifact generation."""

from __future__ import annotations

from packages.observability import load_daily_artifact, record_export_event, record_processing_event


def test_processing_and_export_events_generate_latency_summary(tmp_path) -> None:
    record_processing_event(
        report_date="2026-04-07",
        branch="waigani",
        report_type="sales",
        outcome="accepted",
        parse_mode="strict",
        parser_used="sales_income_agent",
        confidence=0.95,
        warnings=[],
        received_at_utc="2026-04-07T09:00:00Z",
        completed_at_utc="2026-04-07T09:00:05Z",
        output_root=tmp_path,
    )
    record_export_event(
        report_date="2026-04-07",
        branch="waigani",
        success=True,
        duration_ms=800,
        output_root=tmp_path,
    )

    payload = load_daily_artifact("pipeline_latency", "2026-04-07", output_root=tmp_path)

    assert payload is not None
    assert payload["summary"]["total_events"] == 2
    assert payload["summary"]["event_types"]["processing"] == {
        "count": 1,
        "total_duration_ms": 5000,
        "max_duration_ms": 5000,
        "avg_duration_ms": 5000.0,
    }
    assert payload["summary"]["event_types"]["export"] == {
        "count": 1,
        "total_duration_ms": 800,
        "max_duration_ms": 800,
        "avg_duration_ms": 800.0,
    }
