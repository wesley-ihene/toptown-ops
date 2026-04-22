"""Regression coverage for real rejected WhatsApp staff-performance samples."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.orchestrator_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def test_real_rejected_waigani_staff_performance_sample_is_structured(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    sample_path = (
        Path(__file__).resolve().parents[1]
        / "records"
        / "rejected"
        / "whatsapp"
        / "hr_performance"
        / "20260410T115738579097__staff_performance__parser_failure.txt"
    )

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": sample_path.read_text(encoding="utf-8")},
                "metadata": {
                    "received_at": "2026-04-10T00:00:00Z",
                    "sender": "debug",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert result.payload["signal_subtype"] == "staff_performance"
    assert result.payload["status"] in {"accepted_with_warning", "needs_review"}

    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "waigani" / "2026-04-05.json"
    structured_payload = _read_json(structured_path)
    assert len(structured_payload["items"]) == 10

    items_by_number = {item["record_number"]: item for item in structured_payload["items"]}
    assert items_by_number[1]["staff_name"] == "MILFORD"
    assert items_by_number[1]["raw_section"] == "MANS COTTON PANTS"
    assert items_by_number[1]["assisting_count"] == 7
    assert items_by_number[1]["items_moved"] == 130
    assert items_by_number[1]["arrangement_grade"] == 5
    assert items_by_number[1]["display_grade"] == 5
    assert items_by_number[1]["performance_grade"] == 5
    assert items_by_number[8]["duty_status"] == "off_duty"
    assert items_by_number[9]["duty_status"] == "sent_home"


def test_real_rejected_lae_5th_street_sample_is_structured(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    sample_path = (
        Path(__file__).resolve().parents[1]
        / "records"
        / "rejected"
        / "whatsapp"
        / "hr_performance"
        / "20260410T115722820345__staff_performance__parser_failure.txt"
    )

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": sample_path.read_text(encoding="utf-8")},
                "metadata": {
                    "received_at": "2026-04-10T00:00:00Z",
                    "sender": "debug",
                    "branch_hint": "lae_5th_street",
                },
            },
        )
    )

    assert result.payload["signal_subtype"] == "staff_performance"
    assert result.payload["status"] in {"accepted_with_warning", "needs_review"}

    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "lae_5th_street" / "2026-04-05.json"
    structured_payload = _read_json(structured_path)
    assert len(structured_payload["items"]) == 16

    items_by_number = {item["record_number"]: item for item in structured_payload["items"]}
    assert items_by_number[1]["staff_name"] == "Marryane Sakias"
    assert items_by_number[1]["role"] == "Pricing cluck"
    assert items_by_number[1]["duty_status"] == "off_duty"
    assert items_by_number[2]["staff_name"] == "Imelda Patrick"
    assert items_by_number[2]["role"] == "cashier"
    assert items_by_number[3]["section"] == "pricing_room"
    assert items_by_number[11]["duty_status"] == "absent"


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
