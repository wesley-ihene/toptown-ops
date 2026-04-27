"""Focused tests for supervisor-control intelligence storage."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from apps.header_normalizer_agent.worker import normalize_headers
from apps.report_family_classifier_agent.worker import classify_report_family
from apps.supervisor_control_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def test_supervisor_control_is_classified_as_intelligence() -> None:
    text = "\n".join(
        [
            "Supervisor Control Report",
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "Cash variance: No",
            "Staffing issues: Yes",
        ]
    )

    classification = classify_report_family(text, normalize_headers(text))

    assert classification.report_family == "intelligence"
    assert classification.report_type == "supervisor_control"


def test_supervisor_control_writes_intelligence_record_without_structured_copy(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "supervisor_control"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "Supervisor Control Summary",
                            "Branch: Waigani Branch",
                            "Date: 07/04/2026",
                            "Supervisor: Francis Ano",
                            "Cash variance: No",
                            "Staffing issues: Yes",
                            "Stock issues affecting sales: No",
                            "Pricing or system issues: Yes",
                            "Exceptions escalated to Ops Manager: No",
                            "Supervisor confirmation: YES",
                            "Notes: Team covered the opening gap.",
                        ]
                    )
                },
                "metadata": {"received_at": "2026-04-07T09:15:00Z"},
                "ingress_envelope": {
                    "payload": {
                        "message_id": "wamid.intel-schema-1",
                        "sender_phone": "67570000000",
                    }
                },
            },
        )
    )

    intelligence_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json"
    structured_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-07.json"

    assert result.payload["status"] == "accepted"
    assert intelligence_path.exists()
    assert not structured_path.exists()

    stored = json.loads(intelligence_path.read_text(encoding="utf-8"))
    assert stored["report_family"] == "intelligence"
    assert stored["report_type"] == "supervisor_control"
    assert stored["branch"] == "waigani"
    assert stored["report_date"] == "2026-04-07"
    assert stored["supervisor"] == "Francis Ano"
    assert stored["cash_variance"] == "NO"
    assert stored["staffing_issues"] == "YES"
    assert stored["stock_issues"] == "NO"
    assert stored["pricing_or_system_issues"] == "YES"
    assert stored["exceptions_escalated"] == "NO"
    assert stored["supervisor_confirmation"] == "YES"
    assert stored["raw_text"] is not None
    assert stored["confidence"] >= 0.0
    assert stored["source_message_id"] == "wamid.intel-schema-1"
    assert stored["sender_phone"] == "67570000000"
    assert stored["created_at"] == "2026-04-07T09:15:00Z"


def _patch_output_paths(tmp_path: Path, monkeypatch) -> None:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    monkeypatch.setattr("apps.supervisor_control_agent.worker.OUTBOX_PATH", outbox_path)
