"""Tests for duplicate disposal archive behavior."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.outbound_reply_agent import worker as outbound_reply_worker
from packages.record_store.writer import write_governed_structured, write_json_file
from packages.signal_contracts.agent_result import AgentResult
from scripts import whatsapp_webhook_bridge as bridge


def test_duplicate_whatsapp_report_is_archived_for_disposal_and_uses_current_response_decision(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("WHATSAPP_OUTBOUND_MODE", "live")
    sent_response_types: list[str] = []
    repeated_text = "\n".join(
        [
            "DAY-END SALES REPORT",
            "Branch: Waigani",
            "Date: 07/04/2026",
            "Z Reading: 1200",
            "Cash Sales: 600",
            "Card Sales: 600",
            "Total Sales: 1200",
            "Traffic: 12",
            "Served: 9",
            "Staff On Duty: 4",
            "Cash Variance: 0",
            "Over Short Reason: Nil",
            "Supervisor Confirmed: YES",
        ]
    )

    def fake_dispatch_to_specialist(work_item, *, target_agent):
        return AgentResult(agent_name=target_agent, payload=_sales_payload())

    def fake_send_whatsapp_text(**kwargs):
        sent_response_types.append(str(kwargs["response_type"]))
        return {
            "dispatch_status": "sent",
            "provider_message_id": f"wamid.provider.{kwargs['response_type']}",
            "http_status": 200,
            "error": None,
        }

    monkeypatch.setattr(bridge.orchestrator_worker, "_dispatch_to_specialist", fake_dispatch_to_specialist)
    monkeypatch.setattr(outbound_reply_worker, "send_whatsapp_text", fake_send_whatsapp_text)
    caplog.set_level(logging.INFO)

    first = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.archive-1", text=repeated_text)).encode("utf-8"),
    )
    second = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.archive-2", text=repeated_text)).encode("utf-8"),
    )

    assert first.status_code == 200
    second_body = json.loads(second.body.decode("utf-8"))
    archive_paths = _duplicate_archive_paths(tmp_path)

    assert second.status_code == 200
    assert len(archive_paths) == 1
    assert second_body["duplicate"] is True
    assert second_body["orchestrator_status"] == "accepted"
    assert second_body["conversation_response"]["response_type"] == "accepted_ack"
    assert sent_response_types[-1] == "accepted_ack"
    assert sent_response_types.count("accepted_ack") == 2
    assert not any("/structured/" in output for output in second_body["outputs"])

    archive_record = _read_json(archive_paths[0])
    assert archive_record["source_message_id"] == "wamid.archive-2"
    assert archive_record["disposal_status"] == "ready_for_disposal"
    assert archive_record["disposal_allowed"] is True
    assert archive_record["report_type"] == "sales"
    assert archive_record["report_date"] == "2026-04-07"
    assert archive_record["raw_txt_path"] == second_body["raw_txt_path"]
    assert archive_record["raw_meta_path"] == second_body["raw_meta_path"]
    assert "duplicate_archived_for_disposal" in caplog.text


def test_governed_duplicate_write_creates_duplicate_archive_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_environment(monkeypatch, tmp_path)
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
    second_raw_txt = tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-07__waigani__second.txt"
    second_raw_meta = second_raw_txt.with_suffix(".meta.json")
    write_json_file(
        second_raw_meta,
        {
            "message_id": "wamid.semantic-2",
            "sender_phone": "67570000000",
            "branch_hint": "waigani",
            "specialist_report_type": "sales",
            "resolved_report_date": "2026-04-07",
            "raw_txt_path": str(second_raw_txt),
            "raw_meta_path": str(second_raw_meta),
        },
    )

    first = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {
                "message_id": "wamid.semantic-1",
                "sender_phone": "67570000000",
                "raw_sha256": "raw-semantic-1",
                "raw_txt_path": str(tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-07__waigani__first.txt"),
                "raw_meta_path": str(tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-07__waigani__first.meta.json"),
                "classified_report_type": "sales",
            },
        },
        root=tmp_path,
        colony_root=colony_root,
    )
    duplicate = write_governed_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_payload(),
        metadata={
            "validation": {"accepted": True, "status": "accepted"},
            "governance_context": {
                "message_id": "wamid.semantic-2",
                "sender_phone": "67570000000",
                "raw_sha256": "raw-semantic-2",
                "raw_txt_path": str(second_raw_txt),
                "raw_meta_path": str(second_raw_meta),
                "classified_report_type": "sales",
            },
        },
        root=tmp_path,
        colony_root=colony_root,
    )

    structured_dir = tmp_path / "records" / "structured" / "sales_income" / "waigani"
    structured_paths = sorted(
        path
        for path in structured_dir.glob("*.json")
        if not path.name.endswith(".governance.json") and not path.name.endswith(".validation.json")
    )
    archive_paths = _duplicate_archive_paths(tmp_path)

    assert first.persisted is True
    assert duplicate.persisted is False
    assert duplicate.governance.status == "duplicate"
    assert duplicate.governance.reasons == ["duplicate_semantic"]
    assert len(structured_paths) == 1
    assert len(archive_paths) == 1

    archive_record = _read_json(archive_paths[0])
    assert archive_record["duplicate_reason"] == "duplicate_semantic"
    assert archive_record["duplicate_basis"].startswith("semantic_sha256:")
    assert archive_record["raw_txt_path"] == str(second_raw_txt)
    assert archive_record["raw_meta_path"] == str(second_raw_meta)
    assert archive_record["original_or_duplicate_of"] == str(first.path)


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "DUPLICATES_DIR", records_dir / "duplicates" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(bridge, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", raising=False)
    monkeypatch.delenv("TOPTOWN_ENABLE_REPLAY_RESPONSES", raising=False)


def _duplicate_archive_paths(tmp_path: Path) -> list[Path]:
    archive_root = tmp_path / "records" / "duplicates" / "whatsapp"
    if not archive_root.exists():
        return []
    return sorted(archive_root.glob("*/*.json"))


def _meta_payload(*, message_id: str, text: str) -> dict[str, object]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {
                                "display_phone_number": "15551230000",
                                "phone_number_id": "pnid-1",
                            },
                            "contacts": [
                                {
                                    "profile": {"name": "Alice"},
                                    "wa_id": "67570000000",
                                }
                            ],
                            "messages": [
                                {
                                    "from": "67570000000",
                                    "id": message_id,
                                    "timestamp": "1775563200",
                                    "text": {"body": text},
                                    "type": "text",
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _sales_payload() -> dict[str, object]:
    return {
        "status": "accepted",
        "signal_type": "sales_income",
        "source_agent": "sales_income_agent",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "confidence": 0.95,
        "metrics": {
            "gross_sales": 1200.0,
            "cash_sales": 600.0,
            "eftpos_sales": 600.0,
            "mobile_money_sales": 0.0,
            "traffic": 12,
            "served": 9,
        },
        "items": [],
        "warnings": [],
    }


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
