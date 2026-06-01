"""Regression tests for sales performance alerts that must not block acceptance."""

from __future__ import annotations

import json
from pathlib import Path

from apps.conversation_router import route_conversation_response
from apps.orchestrator_agent.worker import process_work_item as process_orchestrated_work_item
from apps.response_engine import render_whatsapp_response
from apps.sales_income_agent.worker import process_work_item as process_sales_work_item
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from packages.signal_contracts.work_item import WorkItem


def test_sales_low_conversion_emits_performance_alert_and_stays_accepted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_sales_work_item(_sales_work_item(_low_conversion_sales_lines()))
    structured_path = tmp_path / "records" / "structured" / "sales_income" / "lae_5th_street" / "2026-04-27.json"

    assert structured_path.exists()
    assert result.payload["status"] == "accepted"
    assert result.payload["warnings"] == []
    assert result.payload["metrics"]["conversion_rate"] == 0.3423
    assert result.payload["alert_type"] == "low_conversion_rate"
    assert result.payload["alert_level"] == "warning"
    assert result.payload["alert_category"] == "performance_alert"
    assert result.payload["alert_message"] == "Conversion rate is low at 34.23%."
    assert result.payload["performance_alerts"] == [
        {
            "alert_type": "low_conversion_rate",
            "alert_level": "warning",
            "alert_message": "Conversion rate is low at 34.23%.",
            "alert_category": "performance_alert",
        }
    ]

    payload = _read_json(structured_path)
    assert payload["status"] == "accepted"
    assert payload["export_allowed"] is True
    assert payload["warnings"] == []
    assert payload["alert_type"] == "low_conversion_rate"
    assert payload["alert_message"] == "Conversion rate is low at 34.23%."


def test_orchestrator_routes_low_conversion_sales_to_accepted_ack_without_review_guidance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_orchestrated_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": "\n".join(_low_conversion_sales_lines())},
                "metadata": {
                    "received_at": "2026-04-27T09:43:59Z",
                    "sender": "live-sales-low-conversion",
                },
            },
        )
    )
    routed = route_conversation_response(result)
    rendered = render_whatsapp_response(routed)

    assert result.agent_name == "sales_income_agent"
    assert result.payload["status"] == "accepted"
    assert result.payload["governance"]["status"] == "accepted"
    assert result.payload["alert_type"] == "low_conversion_rate"
    assert result.payload["alert_message"] == "Conversion rate is low at 34.23%."
    assert result.metadata.get("review_queue_path") is None

    assert routed["response_type"] == "accepted_ack"
    assert routed["reason"] is None
    assert routed["review_queue_path"] is None

    assert rendered["response_type"] == "accepted_ack"
    assert "TAOP REVIEW REQUIRED" not in rendered["response_text"]
    assert "Correct the TOTALS section" not in rendered["response_text"]


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "DUPLICATES_DIR", records_dir / "duplicates" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))


def _sales_work_item(lines: list[str]) -> WorkItem:
    return WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "sales"},
            "raw_message": {"text": "\n".join(lines)},
        },
    )


def _low_conversion_sales_lines() -> list[str]:
    return [
        "DAY-END SALES REPORT",
        "Branch: lae_5th_street",
        "Date: 2026-04-27",
        "Total Cash: 2550.00",
        "Total Card: 536.00",
        "Total Sales: 3086.00",
        "Traffic: 298",
        "Served: 102",
        "Conversion Rate: 34.23%",
    ]


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
