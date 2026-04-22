"""Lifecycle tests for the upstream orchestrator audit flow."""

from __future__ import annotations

import json
from pathlib import Path

import apps.orchestrator_agent.worker as orchestrator_worker
from apps.orchestrator_agent.worker import process_work_item
import packages.record_store.paths as record_paths
from packages.sop_validation.contracts import Rejection
from packages.sop_validation.router import validate_report
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem


def test_orchestrator_keeps_raw_archive_and_writes_structured_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAY-END SALES REPORT",
                            "Branch: Waigani",
                            "Date: 2026-04-07",
                            "Gross Sales: 1250",
                            "Cash Sales: 750",
                            "Eftpos Sales: 500",
                            "Traffic: 24",
                            "Served: 19",
                            "Cashier: Alice",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "audit-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert not (tmp_path / "records" / "raw" / "whatsapp" / "sales").exists()

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "sales_income"
    assert raw_meta["routing_target"] == "sales_income_agent"
    assert raw_meta["processing_status"] == "processed"
    accepted_provenance_path = (
        tmp_path
        / "records"
        / "provenance"
        / "accepted"
        / "2026_04_07"
        / "waigani"
        / "sales"
        / f"{raw_meta['raw_sha256']}.json"
    )
    assert accepted_provenance_path.exists()
    accepted_provenance = _read_json(accepted_provenance_path)
    assert accepted_provenance["parser_used"] == "sales_income_agent"
    assert accepted_provenance["parse_mode"] == "strict"
    assert accepted_provenance["downstream_references"]["structured_records"] == [
        "records/structured/sales_income/waigani/2026-04-07.json"
    ]

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["signal_type"] == "sales_income"
    assert structured_payload["branch"] == "waigani"
    assert structured_payload["report_date"] == "2026-04-07"

    assert result.payload["signal_type"] == "sales_income"


def test_orchestrator_routes_live_staff_performance_message_with_resolved_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _live_staff_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T09:43:59Z",
                    "sender": "Wesley",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "staff_performance"
    assert raw_meta["branch_hint"] == "lae_malaita"
    assert raw_meta["routing_target"] == "hr_agent"
    assert raw_meta["resolved_report_date"] == "2026-04-07"
    assert raw_meta["raw_report_date"] == "TUESDAY 07 /04/26"
    assert raw_meta["normalized_header_candidates"][:3] == [
        "ttc lae malaita branch",
        "tuesday 07 04 26",
        "staff performance report",
    ]

    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "lae_malaita" / "2026-04-07.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["source_agent"] == "hr_agent"
    assert structured_payload["branch"] == "lae_malaita"
    assert structured_payload["report_date"] == "2026-04-07"
    assert len(structured_payload["items"]) >= 19
    assert structured_payload["status"] == "accepted_with_warning"
    assert structured_payload["metrics"]["price_room_staff_count"] == 5
    assert structured_payload["metrics"]["special_assignment_count"] == 1
    assert structured_payload["items"][3]["section"] == "pricing_room_sales_tally"
    assert len(structured_payload["price_room_staff"]) == 5
    assert len(structured_payload["special_assignments"]) == 1

    assert result.agent_name == "hr_agent"
    assert result.payload["signal_subtype"] == "staff_performance"
    assert result.payload["branch"] == "lae_malaita"
    assert result.payload["report_date"] == "2026-04-07"
    assert result.payload["status"] == "accepted_with_warning"


def test_orchestrator_routes_live_sales_message_with_recoverable_date_formatting(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _live_sales_income_text()},
                "metadata": {
                    "received_at": "2026-04-10T09:43:59Z",
                    "sender": "live-sales",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "sales_income"
    assert raw_meta["branch_hint"] == "lae_5th_street"
    assert raw_meta["routing_target"] == "sales_income_agent"
    assert raw_meta["resolved_report_date"] == "2026-04-10"
    assert raw_meta["processing_status"] == "processed"

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "lae_5th_street" / "2026-04-10.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["source_agent"] == "sales_income_agent"
    assert structured_payload["branch"] == "lae_5th_street"
    assert structured_payload["report_date"] == "2026-04-10"
    assert structured_payload["status"] == "accepted"
    assert structured_payload["metrics"]["gross_sales"] == 1200.0
    assert structured_payload["metrics"]["eftpos_sales"] == 500.0

    assert result.agent_name == "sales_income_agent"
    assert result.payload["signal_type"] == "sales_income"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-10"
    assert result.payload["status"] == "accepted"


def test_orchestrator_routes_recoverable_sales_message_with_messy_money_and_aliases(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAY end sales report",
                            "Shop: TTC LAE 5TH STREET BRANCH",
                            "Date: Friday, 10/04 /26",
                            "Total Sales: K3,489. 00",
                            "Cash Sales: 1 236.00",
                            "Card Sales: 2 253.00",
                            "Main Door: 43",
                            "Customers Served: 20",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-10T09:43:59Z",
                    "sender": "live-sales-normalized",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "sales_income"
    assert raw_meta["branch_hint"] == "lae_5th_street"
    assert raw_meta["resolved_report_date"] == "2026-04-10"
    assert raw_meta["processing_status"] == "processed"

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "lae_5th_street" / "2026-04-10.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["metrics"]["gross_sales"] == 3489.0
    assert structured_payload["metrics"]["cash_sales"] == 1236.0
    assert structured_payload["metrics"]["eftpos_sales"] == 2253.0

    assert result.payload["status"] == "accepted"


def test_orchestrator_unknown_message_writes_rejected_copy_but_keeps_raw_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": "Inventory note from floor team. Please review manually."},
                "metadata": {
                    "received_at": "2026-04-07T11:05:00Z",
                    "sender": "reject-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_text_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.txt")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert len(rejected_text_paths) == 1
    assert len(rejected_meta_paths) == 1

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "unknown"
    assert raw_meta["routing_target"] is None
    assert raw_meta["processing_status"] == "rejected"

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "unknown_report_type"
    assert rejected_meta["attempted_report_type"] == "unknown"
    rejected_provenance_path = (
        tmp_path
        / "records"
        / "provenance"
        / "rejected"
        / "2026_04_07"
        / "waigani"
        / "unknown"
        / f"{raw_meta['raw_sha256']}.json"
    )
    assert rejected_provenance_path.exists()
    rejected_provenance = _read_json(rejected_provenance_path)
    assert rejected_provenance["parser_used"] == "orchestrator_agent"
    assert rejected_provenance["parse_mode"] == "strict"
    assert rejected_provenance["downstream_references"]["rejected_text_path"].endswith(".txt")
    assert rejected_provenance["downstream_references"]["rejected_meta_path"].endswith(".meta.json")

    assert result.payload["status"] == "rejected"
    assert not (tmp_path / "records" / "structured").exists()


def test_orchestrator_rejects_invalid_bale_pricing_card_message_upstream(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("TOPTOWN_IOI_COLONY_ROOT", str(tmp_path / "ioi-colony"))

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _invalid_bale_pricing_card_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:05:00Z",
                    "sender": "pricing-card-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_text_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.txt")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert len(rejected_text_paths) == 1
    assert len(rejected_meta_paths) == 1

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "invalid_pricing_card_format"
    assert raw_meta["routing_target"] is None
    assert raw_meta["processing_status"] == "rejected"

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "invalid_pricing_card_format"
    assert rejected_meta["attempted_report_type"] == "invalid_pricing_card_format"
    assert rejected_meta["attempted_agent"] is None

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "rejected"
    assert result.payload["classification"]["report_type"] == "invalid_pricing_card_format"
    assert result.payload["routing"]["route_reason"] == "invalid_pricing_card_format"
    assert result.payload["warnings"][0]["code"] == "invalid_pricing_card_format"

    assert not (tmp_path / "records" / "structured").exists()
    assert not (tmp_path / "analytics").exists()
    assert not (tmp_path / "ioi-colony").exists()


def test_orchestrator_routes_stylized_unicode_attendance_sample_to_structured_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _unknown_attendance_text()},
                "metadata": {
                    "received_at": "2026-04-06T02:32:38Z",
                    "sender": "Wesley",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "attendance"
    assert raw_meta["routing_target"] == "hr_agent"
    assert raw_meta["branch_hint"] == "waigani"
    assert raw_meta["processing_status"] == "processed"
    assert raw_meta["governance_status"] == "needs_review"

    structured_path = tmp_path / "records" / "structured" / "hr_attendance" / "waigani" / "2026-04-06.json"
    assert not structured_path.exists()
    review_paths = _paths(tmp_path / "records" / "review" / "2026_04_06" / "waigani" / "staff_attendance", "*.json")
    assert len(review_paths) == 1

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "needs_review"
    assert result.payload["fallback"]["acceptance"]["decision"] == "review"


def test_orchestrator_routes_current_unknown_staff_performance_sample_to_structured_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _unknown_waigani_stuff_performance_text()},
                "metadata": {
                    "received_at": "2026-04-06T01:28:20Z",
                    "sender": "Wesley",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "staff_performance"
    assert raw_meta["routing_target"] == "hr_agent"
    assert raw_meta["branch_hint"] == "waigani"

    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "waigani" / "2026-04-05.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["branch"] == "waigani"
    assert structured_payload["report_date"] == "2026-04-05"
    assert len(structured_payload["items"]) >= 8
    assert structured_payload["status"] in {"accepted_with_warning", "needs_review"}

    assert result.agent_name == "hr_agent"
    assert result.payload["signal_subtype"] == "staff_performance"


def test_orchestrator_routes_current_hr_performance_parser_failure_sample_to_structured_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _lae_5th_street_parser_failure_text()},
                "metadata": {
                    "received_at": "2026-04-06T01:36:15Z",
                    "sender": "Wesley",
                    "branch_hint": "lae_5th_street",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "staff_performance"
    assert raw_meta["routing_target"] == "hr_agent"
    assert raw_meta["branch_hint"] == "lae_5th_street"

    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "lae_5th_street" / "2026-04-05.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["branch"] == "lae_5th_street"
    assert structured_payload["report_date"] == "2026-04-05"
    assert len(structured_payload["items"]) == 16

    items_by_number = {item["record_number"]: item for item in structured_payload["items"]}
    assert items_by_number[1]["duty_status"] == "off_duty"
    assert items_by_number[2]["role"] == "cashier"
    assert items_by_number[3]["section"] == "pricing_room"
    assert items_by_number[11]["duty_status"] == "absent"
    assert items_by_number[15]["role"] == "Supervisor"
    assert items_by_number[15]["duty_status"] == "off_duty"
    assert structured_payload["metrics"]["total_staff_records"] == 16
    assert structured_payload["metrics"]["total_items_moved"] == 140
    assert structured_payload["metrics"]["total_assisting_count"] == 46

    assert result.agent_name == "hr_agent"
    assert result.payload["signal_subtype"] == "staff_performance"


def test_orchestrator_invalid_input_archives_raw_before_quarantine(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": ""},
                "metadata": {
                    "received_at": "2026-04-07T11:10:00Z",
                    "sender": "invalid-smoke",
                    "branch_hint": "blank",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_text_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.txt")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert raw_text_paths[0].read_text(encoding="utf-8") == ""
    assert len(rejected_text_paths) == 1
    assert len(rejected_meta_paths) == 1

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["processing_status"] == "rejected"
    assert raw_meta["detected_report_type"] == "unknown"
    assert raw_meta["routing_target"] is None

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "missing_raw_text"
    assert rejected_meta["attempted_agent"] is None

    assert result.payload["status"] == "invalid_input"


def test_orchestrator_replay_skips_raw_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": "Shift note only. Please call me when you arrive."},
                "metadata": {
                    "received_at": "2026-04-07T11:15:00Z",
                    "sender": "replay-smoke",
                    "branch_hint": "waigani",
                },
                "replay": {
                    "is_replay": True,
                    "source": "rejected",
                    "original_path": "records/rejected/whatsapp/unknown/example.txt",
                    "replayed_at": "2026-04-07T12:00:00Z",
                },
            },
        )
    )

    assert _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt") == []
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")
    assert len(rejected_meta_paths) == 1

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["replay"] is True
    assert rejected_meta["replay_source"] == "rejected"
    assert rejected_meta["replay_original_path"] == "records/rejected/whatsapp/unknown/example.txt"
    assert result.payload["status"] == "rejected"


def test_orchestrator_mixed_report_fans_out_to_multiple_specialists(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _mixed_sales_and_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T13:00:00Z",
                    "sender": "mixed-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "mixed"
    assert raw_meta["routing_target"] == "fan_out"
    assert raw_meta["split_child_count"] == 2
    assert raw_meta["split_child_report_types"] == ["sales_income", "staff_performance"]
    assert raw_meta["processing_status"] == "processed"

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    performance_path = tmp_path / "records" / "structured" / "hr_performance" / "waigani" / "2026-04-07.json"
    assert sales_path.exists()
    assert performance_path.exists()
    assert not (
        tmp_path / "records" / "structured" / "sales_income" / "ttc_waigani_branch" / "2026-04-07.json"
    ).exists()

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["output_path"] == "records/structured/sales_income/waigani/2026-04-07.json"
    assert result.payload["output_paths"] == [
        "records/structured/sales_income/waigani/2026-04-07.json",
        "records/structured/hr_performance/waigani/2026-04-07.json",
    ]
    assert result.payload["derived_output_paths"] == result.payload["output_paths"]
    assert result.payload["segment_count"] == 2
    assert result.payload["written_count"] == 2
    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["routing"]["target_agent"] == "fan_out"
    assert result.payload["routing"]["split_strategy"] == "explicit_report_headers"
    assert result.payload["status"] == "accepted_with_warning"
    assert len(result.payload["outputs"]) == 2
    assert result.payload["lineage"]["message_role"] == "split_parent"
    assert len(result.payload["fanout"]["children"]) == 2
    assert [child["report_family"] for child in result.payload["fanout"]["children"]] == [
        "sales_income",
        "staff_performance",
    ]
    assert result.payload["fanout"]["children"][0]["branch"] == "waigani"
    assert result.payload["fanout"]["children"][1]["branch"] == "waigani"


def test_orchestrator_rejects_mixed_report_when_ingress_policy_requires_single_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    specialist_calls = 0

    def fake_dispatch(*args, **kwargs):
        nonlocal specialist_calls
        specialist_calls += 1
        raise AssertionError("specialist dispatch should not run for rejected mixed reports")

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker._should_attempt_specialist_fallback", lambda **kwargs: False)
    monkeypatch.setattr("apps.orchestrator_agent.worker._should_attempt_specialist_fallback", lambda **kwargs: False)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _mixed_sales_and_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T13:00:00Z",
                    "sender": "mixed-smoke",
                    "branch_hint": "waigani",
                },
                "ingress_policy": {
                    "reject_mixed_reports": True,
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_text_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.txt")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert len(raw_meta_paths) == 1
    assert len(rejected_text_paths) == 1
    assert len(rejected_meta_paths) == 1
    assert not (tmp_path / "records" / "structured").exists()

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "mixed"
    assert raw_meta["routing_target"] is None
    assert raw_meta["processing_status"] == "rejected"
    assert raw_meta["routing_review_reason"] == "mixed_reports_rejected_upstream"

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "mixed_report"
    assert rejected_meta["attempted_report_type"] == "mixed"
    assert rejected_meta["policy_guard"]["action"] == "reject"
    assert rejected_meta["policy_guard"]["reason"] == "mixed_report_rejected"

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["routing"]["route_reason"] == "mixed_report_rejected"
    assert result.payload["status"] == "needs_review"
    assert result.payload["warnings"][0]["code"] == "mixed_report"
    assert result.payload["policy_guard"]["action"] == "reject"
    assert specialist_calls == 0


def test_orchestrator_mixed_report_goes_to_review_when_safe_split_is_not_possible(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    class FakeSplitResult:
        def __init__(self) -> None:
            self.segments = []
            self.common_prefix_lines = []
            self.split_confidence = 0.4

    monkeypatch.setattr(
        "apps.orchestrator_agent.worker.split_report",
        lambda text, detection: FakeSplitResult(),
    )

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _mixed_sales_and_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T13:00:00Z",
                    "sender": "mixed-review-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert len(raw_meta_paths) == 1
    assert len(rejected_meta_paths) == 0
    assert not (tmp_path / "records" / "structured").exists()

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "mixed"
    assert raw_meta["routing_target"] is None
    assert raw_meta["processing_status"] == "rejected"
    assert raw_meta["routing_review_reason"] == "mixed_report_split_not_safe"
    assert raw_meta["split_child_count"] == 0

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["routing"]["route_reason"] == "mixed_report_requires_review"
    assert result.payload["status"] == "needs_review"
    assert result.payload["warnings"][0]["code"] == "mixed_split_review"


def test_orchestrator_rejects_duplicate_message_before_specialist_processing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    specialist_calls = 0

    def fake_dispatch(work_item, *, target_agent):
        nonlocal specialist_calls
        specialist_calls += 1
        return AgentResult(
            agent_name=target_agent,
            payload=_valid_sales_candidate_payload(),
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)

    first_result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "duplicate-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )
    second_result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "duplicate-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "sales", "*.meta.json")

    assert first_result.agent_name == "sales_income_agent"
    assert specialist_calls == 1
    assert len(raw_meta_paths) == 1
    assert len(rejected_meta_paths) == 1

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["processing_status"] == "duplicate"
    assert raw_meta["policy_guard"]["action"] == "reject"
    assert raw_meta["policy_guard"]["reason"] == "duplicate_message"
    assert raw_meta["policy_guard"]["duplicate"] is True

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "duplicate_message"
    assert rejected_meta["policy_guard"]["duplicate"] is True
    assert rejected_meta["policy_guard"]["duplicate_basis"] == "policy_guard:passed"

    assert second_result.agent_name == "orchestrator_agent"
    assert second_result.payload["status"] == "duplicate"
    assert second_result.payload["warnings"][0]["code"] == "duplicate_message"
    assert second_result.payload["policy_guard"]["reason"] == "duplicate_message"


def test_orchestrator_rejects_same_raw_sha256_within_24_hours_across_raw_file_boundaries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    specialist_calls = 0

    def fake_dispatch(work_item, *, target_agent):
        nonlocal specialist_calls
        specialist_calls += 1
        return AgentResult(
            agent_name=target_agent,
            payload=_valid_sales_candidate_payload(),
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)

    first_result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T23:30:00Z",
                    "sender": "duplicate-hash-smoke-1",
                    "branch_hint": "waigani",
                },
            },
        )
    )
    second_result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-08T10:00:00Z",
                    "sender": "duplicate-hash-smoke-2",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "sales", "*.meta.json")

    assert first_result.agent_name == "sales_income_agent"
    assert second_result.agent_name == "orchestrator_agent"
    assert specialist_calls == 1
    assert len(raw_meta_paths) == 2
    assert len(rejected_meta_paths) == 1

    raw_metas = [_read_json(path) for path in raw_meta_paths]
    duplicate_raw_meta = next(meta for meta in raw_metas if meta.get("policy_guard", {}).get("reason") == "duplicate_message")
    assert duplicate_raw_meta["raw_sha256"] == raw_metas[0]["raw_sha256"] == raw_metas[1]["raw_sha256"]
    assert duplicate_raw_meta["processing_status"] == "duplicate"
    assert duplicate_raw_meta["policy_guard"]["duplicate"] is True
    assert duplicate_raw_meta["policy_guard"]["duplicate_basis"] == "policy_guard:passed"

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "duplicate_message"
    assert rejected_meta["policy_guard"]["duplicate"] is True

    assert second_result.payload["status"] == "duplicate"
    assert second_result.payload["warnings"][0]["code"] == "duplicate_message"
    assert second_result.payload["policy_guard"]["duplicate"] is True
    assert (tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json").exists()


def test_orchestrator_runs_validation_and_acceptance_before_final_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    call_order: list[str] = []

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(agent_name=target_agent, payload=_valid_sales_candidate_payload())

    class DummyValidationResult:
        accepted = True
        normalized_payload = {"branch": "waigani", "report_date": "2026-04-07"}
        rejections = []

        def to_payload(self):
            return {"accepted": True, "status": "accepted", "report_type": "sales", "rejections": [], "normalization": {}}

    class DummyAcceptanceResult:
        decision = "accept"
        reason = "test_accept"
        confidence = 0.95

        def to_payload(self):
            return {
                "report_type": "sales",
                "decision": "accept",
                "status": "accept",
                "reason": "test_accept",
                "confidence": 0.95,
                "thresholds": {},
            }

        def governed_status(self, *, warning_codes=None):
            return "accepted"

    class DummyGovernance:
        status = "accepted"
        export_allowed = True

        def to_payload(self):
            return {"status": "accepted", "export_allowed": True, "reasons": []}

    class DummyWriteResult:
        governance = DummyGovernance()

    def fake_validate(report_type, payload):
        call_order.append("validate")
        return DummyValidationResult()

    def fake_accept(report_type, *, validation_result, work_item_payload):
        call_order.append("accept")
        return DummyAcceptanceResult()

    def fake_write(payload, *, metadata=None):
        call_order.append("write")
        return DummyWriteResult()

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.validate_report", fake_validate)
    monkeypatch.setattr("apps.orchestrator_agent.worker.decide_acceptance", fake_accept)
    monkeypatch.setattr("apps.orchestrator_agent.worker.sales_record_store.write_structured_record", fake_write)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {"received_at": "2026-04-07T11:00:00Z", "branch_hint": "waigani"},
            },
        )
    )

    assert call_order == ["validate", "accept", "write"]
    assert result.payload["status"] == "accepted"


def test_orchestrator_conflict_blocked_result_enters_review_queue(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    structured_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    structured_path.parent.mkdir(parents=True, exist_ok=True)
    structured_path.write_text(
        json.dumps(_valid_sales_candidate_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    structured_path.with_suffix(".governance.json").write_text(
        json.dumps(
            {
                "version": "v1",
                "status": "accepted",
                "export_allowed": True,
                "report_family": "sales",
                "signal_type": "sales_income",
                "branch": "waigani",
                "report_date": "2026-04-07",
                "message_id": "seed-1",
                "raw_sha256": "seed-raw-1",
                "normalized_scope": "sales:waigani:2026-04-07",
                "semantic_sha256": "seed-semantic",
                "reasons": [],
                "warnings": [],
                "source_status": "accepted",
                "duplicate_of": None,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_dispatch(work_item, *, target_agent):
        payload = _valid_sales_candidate_payload()
        payload["metrics"]["gross_sales"] = 1800.0
        payload["metrics"]["cash_sales"] = 900.0
        payload["metrics"]["eftpos_sales"] = 900.0
        return AgentResult(agent_name=target_agent, payload=payload)

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker._should_attempt_specialist_fallback", lambda **kwargs: False)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {"received_at": "2026-04-07T11:00:00Z", "branch_hint": "waigani"},
            },
        )
    )

    review_paths = _paths(tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales", "*.json")
    assert result.payload["status"] == "conflict_blocked"
    assert len(review_paths) == 1
    review_payload = _read_json(review_paths[0])
    assert review_payload["governance"]["status"] == "conflict_blocked"
    assert review_payload["governance"]["reasons"] == ["conflicting_record_same_scope"]


def test_orchestrator_parser_failure_is_recorded_as_governance_reason(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "signal_type": "sales_income",
                "source_agent": "sales_income_agent",
                "branch": None,
                "report_date": None,
                "confidence": 0.0,
                "metrics": {},
                "items": [],
                "warnings": [{"code": "parser_failure", "severity": "error", "message": "parse failed"}],
                "status": "invalid_input",
            },
            metadata={
                "validation": {
                    "accepted": False,
                    "details": {
                        "parser_failure": True,
                        "final_status": "invalid_input",
                    },
                }
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker._should_attempt_specialist_fallback", lambda **kwargs: False)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {"received_at": "2026-04-07T11:00:00Z", "branch_hint": "waigani"},
            },
        )
    )

    assert result.payload["status"] == "rejected"
    assert "parser_failure" in result.payload["governance"]["reasons"]


def test_orchestrator_routes_actual_backlog_attendance_message(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _backlog_attendance_text()},
                "metadata": {
                    "received_at": "2026-04-06T09:43:59Z",
                    "sender": "backlog-attendance",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "attendance"
    assert raw_meta["branch_hint"] == "lae_5th_street"
    assert raw_meta["routing_target"] == "hr_agent"
    assert raw_meta["resolved_report_date"] == "2026-04-05"
    assert raw_meta["processing_status"] == "processed"

    rejected_unknown_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp", "**/*.meta.json")
    assert rejected_unknown_paths == []
    structured_path = tmp_path / "records" / "structured" / "hr_attendance" / "lae_5th_street" / "2026-04-05.json"
    assert not structured_path.exists()

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "needs_review"
    assert result.payload["fallback"]["validation"]["accepted"] is True


def test_orchestrator_routes_actual_backlog_staff_performance_message(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _backlog_staff_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T09:43:59Z",
                    "sender": "backlog-performance",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "staff_performance"
    assert raw_meta["branch_hint"] == "lae_malaita"
    assert raw_meta["routing_target"] == "hr_agent"
    assert raw_meta["resolved_report_date"] == "2026-04-07"

    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "lae_malaita" / "2026-04-07.json"
    assert structured_path.exists()

    structured_payload = _read_json(structured_path)
    assert structured_payload["branch"] == "lae_malaita"
    assert structured_payload["report_date"] == "2026-04-07"
    assert len(structured_payload["items"]) >= 19
    assert structured_payload["status"] in {"accepted_with_warning", "accepted", "needs_review"}

    assert result.agent_name == "hr_agent"
    assert result.payload["branch"] == "lae_malaita"
    assert result.payload["report_date"] == "2026-04-07"


def test_orchestrator_routes_actual_backlog_pricing_stock_release_message(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _backlog_bale_release_text()},
                "metadata": {
                    "received_at": "2026-04-06T09:43:59Z",
                    "sender": "backlog-bale-release",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "pricing_stock_release"
    assert raw_meta["branch_hint"] == "waigani"
    assert raw_meta["routing_target"] == "pricing_stock_release_agent"
    assert raw_meta["resolved_report_date"] == "2026-04-06"

    structured_path = tmp_path / "records" / "structured" / "pricing_stock_release" / "waigani" / "2026-04-06.json"
    assert structured_path.exists()
    assert not (tmp_path / "records" / "structured" / "pricing_stock_release" / "ttc_pom_waigani_branch" / "2026-04-06.json").exists()

    structured_payload = _read_json(structured_path)
    assert structured_payload["branch"] == "waigani"
    assert structured_payload["report_date"] == "2026-04-06"
    assert structured_payload["metrics"]["total_qty"] == 428
    assert structured_payload["metrics"]["total_amount"] == 3282.0

    assert result.agent_name == "pricing_stock_release_agent"
    assert result.payload["branch"] == "waigani"
    assert result.payload["report_date"] == "2026-04-06"


def test_orchestrator_splits_actual_backlog_sales_and_supervisor_message(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _backlog_mixed_sales_and_supervisor_text()},
                "metadata": {
                    "received_at": "2026-04-10T09:43:59Z",
                    "sender": "backlog-mixed",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "mixed"
    assert raw_meta["routing_target"] == "fan_out"
    assert raw_meta["split_child_count"] == 2
    assert raw_meta["split_child_report_types"] == ["sales_income", "supervisor_control"]
    assert raw_meta["processing_status"] == "rejected"

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-10.json"
    supervisor_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-10.json"
    assert not sales_path.exists()
    assert not supervisor_path.exists()
    assert not (tmp_path / "records" / "structured" / "sales_income" / "ttc_waigani_branch" / "2026-04-10.json").exists()

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["status"] == "needs_review"
    assert len(result.payload["fanout"]["children"]) == 2
    assert result.payload["output_paths"] == []


def test_orchestrator_persists_fallback_eligibility_by_report_type_in_raw_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        payload = work_item.payload
        report_type = payload["classification"]["report_type"]
        branch_hint = payload["routing"]["branch_hint"]
        report_date = "2026-04-07"
        if report_type == "supervisor_control":
            return AgentResult(
                agent_name=target_agent,
                payload={
                    "status": "accepted",
                    "signal_type": "supervisor_control",
                    "branch": branch_hint,
                    "report_date": report_date,
                },
            )
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "accepted",
                "signal_type": "sales_income",
                "branch": branch_hint,
                "report_date": report_date,
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)

    process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "sales-fallback-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )
    process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _supervisor_control_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:05:00Z",
                    "sender": "supervisor-fallback-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 2

    raw_meta_by_type = {
        _read_json(path)["policy_guard"]["report_type"]: _read_json(path)
        for path in raw_meta_paths
    }
    assert raw_meta_by_type["sales"]["policy_guard"]["fallback_eligible"] is True
    assert raw_meta_by_type["supervisor_control"]["policy_guard"]["fallback_eligible"] is True


def test_orchestrator_strict_parse_success_bypasses_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    fallback_calls = 0

    def fake_fallback(work_item):
        nonlocal fallback_calls
        fallback_calls += 1
        return AgentResult(agent_name="fallback_extraction_agent", payload={"status": "extracted"})

    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "strict-success",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert result.agent_name == "sales_income_agent"
    assert fallback_calls == 0


def test_orchestrator_strict_parse_failure_invokes_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    fallback_calls = 0

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    def fake_fallback(work_item):
        nonlocal fallback_calls
        fallback_calls += 1
        return AgentResult(
            agent_name="fallback_extraction_agent",
            payload={
                "signal_type": "fallback_extraction",
                "source_agent": "fallback_extraction_agent",
                "report_type": "sales",
                "parse_mode": "fallback",
                "confidence": 0.65,
                "warnings": [],
                "provenance": {"source": "test"},
                "normalized_report": {
                    "branch": "waigani",
                    "report_date": "2026-04-07",
                    "metrics": {
                        "gross_sales": 1200.0,
                        "cash_sales": 700.0,
                        "eftpos_sales": 500.0,
                        "traffic": 12,
                        "served": 10,
                    },
                    "items": [],
                },
                "status": "extracted",
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "strict-failure",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert fallback_calls == 1
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["fallback"]["parse_mode"] == "fallback"


def test_orchestrator_strict_needs_review_invokes_fallback_before_final_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    fallback_calls = 0

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload=_valid_sales_candidate_payload(
                status="needs_review",
                warnings=[{"code": "needs_review", "severity": "warning", "message": "strict parse needs review"}],
            ),
        )

    def fake_fallback(work_item):
        nonlocal fallback_calls
        fallback_calls += 1
        return AgentResult(
            agent_name="fallback_extraction_agent",
            payload={
                "signal_type": "fallback_extraction",
                "source_agent": "fallback_extraction_agent",
                "report_type": "sales",
                "parse_mode": "fallback",
                "confidence": 0.95,
                "warnings": [],
                "provenance": {"source": "test"},
                "normalized_report": {
                    "branch": "waigani",
                    "report_date": "2026-04-07",
                    "metrics": {
                        "gross_sales": 1200.0,
                        "cash_sales": 700.0,
                        "eftpos_sales": 500.0,
                        "traffic": 12,
                        "served": 10,
                    },
                    "items": [],
                },
                "status": "extracted",
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "strict-needs-review",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert fallback_calls == 1
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "accepted"
    assert result.payload["fallback"]["acceptance"]["decision"] == "accept"


def test_orchestrator_valid_fallback_output_proceeds_to_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "fallback-valid",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    review_paths = _paths(tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales", "*.json")

    assert result.payload["status"] == "needs_review"
    assert result.payload["fallback"]["validation"]["accepted"] is True
    assert result.payload["fallback"]["acceptance"]["decision"] == "review"
    assert len(review_paths) == 1

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["fallback_parse_mode"] == "fallback"
    assert raw_meta["fallback_validation"]["accepted"] is True


def test_orchestrator_fallback_validation_uses_normalized_report_date(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAY end sales report",
                            "Shop: TTC LAE 5TH STREET BRANCH",
                            "Date: Friday, 10/04 /26",
                            "Total Sales: 1200",
                            "Cash Sales: 700",
                            "Card Sales: 500",
                            "Main Door: 15",
                            "Customers Served: 12",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-10T09:43:59Z",
                    "sender": "fallback-date-normalized",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")

    assert result.payload["status"] == "needs_review"
    assert result.payload["fallback"]["validation"]["accepted"] is True
    assert result.payload["fallback"]["normalized_report"]["report_date"] == "2026-04-10"

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["normalized_report_date"] == "2026-04-10"
    assert raw_meta["raw_report_date"] == "10/04 /26"
    assert raw_meta["fallback_validation"]["accepted"] is True


def test_orchestrator_fallback_downgrades_resolvable_invalid_report_date_to_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    def fake_validate(report_type, payload):
        result = validate_report(report_type, payload)
        result.accepted = False
        result.rejections = [
            Rejection(
                code="invalid_report_date",
                message="The payload report_date must use YYYY-MM-DD format.",
                field="report_date",
            )
        ]
        result.normalized_payload["report_date"] = "2026-04-10"
        result.normalization["report_date"] = {
            "raw": "10/04 /26",
            "normalized": "2026-04-10",
            "confidence": 1.0,
        }
        return result

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.validate_report", fake_validate)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAY end sales report",
                            "Shop: TTC LAE 5TH STREET BRANCH",
                            "Date: Friday, 10/04 /26",
                            "Total Sales: 1200",
                            "Cash Sales: 700",
                            "Card Sales: 500",
                            "Main Door: 15",
                            "Customers Served: 12",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-10T09:43:59Z",
                    "sender": "fallback-date-warning",
                },
            },
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["fallback"]["warnings"]}
    assert "invalid_report_date" in warning_codes
    assert result.payload["fallback"]["validation"]["accepted"] is True


def test_orchestrator_strict_needs_review_is_not_rejected_when_fallback_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    fallback_calls = 0

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload=_valid_sales_candidate_payload(
                status="needs_review",
                warnings=[{"code": "needs_review", "severity": "warning", "message": "strict parse needs review"}],
            ),
        )

    def fake_fallback(work_item):
        nonlocal fallback_calls
        fallback_calls += 1
        return AgentResult(
            agent_name="fallback_extraction_agent",
            payload={
                "signal_type": "fallback_extraction",
                "source_agent": "fallback_extraction_agent",
                "report_type": "sales",
                "parse_mode": "fallback",
                "confidence": 0.2,
                "warnings": [],
                "provenance": {"source": "test"},
                "normalized_report": {
                    "branch": "",
                    "report_date": "bad-date",
                    "metrics": {},
                    "items": [],
                },
                "status": "extracted",
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "strict-review-fallback-fails",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "sales", "*.meta.json")

    assert fallback_calls == 1
    assert result.agent_name == "sales_income_agent"
    assert result.payload["status"] == "needs_review"
    assert len(rejected_meta_paths) == 0

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["processing_status"] == "processed"


def test_orchestrator_high_confidence_fallback_is_accepted_for_allowed_report_type(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    def fake_fallback(work_item):
        return AgentResult(
            agent_name="fallback_extraction_agent",
            payload={
                "signal_type": "fallback_extraction",
                "source_agent": "fallback_extraction_agent",
                "report_type": "sales",
                "parse_mode": "fallback",
                "confidence": 0.95,
                "warnings": [],
                "provenance": {"source": "test"},
                "normalized_report": {
                    "branch": "waigani",
                    "report_date": "2026-04-07",
                    "metrics": {
                        "gross_sales": 1200.0,
                        "cash_sales": 700.0,
                        "eftpos_sales": 500.0,
                        "traffic": 12,
                        "served": 10,
                    },
                    "items": [],
                },
                "status": "extracted",
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "fallback-high-confidence",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["fallback"]["acceptance"]["decision"] == "accept"
    assert _paths(tmp_path / "records" / "review", "*.json") == []


def test_orchestrator_invalid_fallback_output_is_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    def fake_fallback(work_item):
        return AgentResult(
            agent_name="fallback_extraction_agent",
            payload={
                "signal_type": "fallback_extraction",
                "source_agent": "fallback_extraction_agent",
                "report_type": "sales",
                "parse_mode": "fallback",
                "confidence": 0.4,
                "warnings": [],
                "provenance": {"source": "test"},
                "normalized_report": {
                    "branch": "",
                    "report_date": "bad-date",
                    "metrics": {},
                    "items": [],
                },
                "status": "extracted",
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "fallback-invalid",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "sales", "*.meta.json")

    assert result.payload["status"] == "invalid_input"
    assert result.payload["fallback"]["validation"]["accepted"] is False
    assert len(rejected_meta_paths) == 1
    assert _paths(tmp_path / "records" / "review", "*.json") == []


def test_orchestrator_low_confidence_fallback_is_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    def fake_fallback(work_item):
        return AgentResult(
            agent_name="fallback_extraction_agent",
            payload={
                "signal_type": "fallback_extraction",
                "source_agent": "fallback_extraction_agent",
                "report_type": "sales",
                "parse_mode": "fallback",
                "confidence": 0.2,
                "warnings": [],
                "provenance": {"source": "test"},
                "normalized_report": {
                    "branch": "waigani",
                    "report_date": "2026-04-07",
                    "metrics": {
                        "gross_sales": 1200.0,
                        "cash_sales": 700.0,
                        "eftpos_sales": 500.0,
                        "traffic": 12,
                        "served": 10,
                    },
                    "items": [],
                },
                "status": "extracted",
            },
        )

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "fallback-low-confidence",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "sales", "*.meta.json")

    assert result.payload["status"] == "invalid_input"
    assert result.payload["fallback"]["acceptance"]["decision"] == "reject"
    assert result.payload["fallback"]["acceptance"]["reason"] == "confidence_below_reject_threshold"
    assert len(rejected_meta_paths) == 1


def test_orchestrator_fallback_disabled_report_type_does_not_invoke_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    fallback_calls = 0

    def fake_dispatch(work_item, *, target_agent):
        return AgentResult(
            agent_name=target_agent,
            payload={
                "status": "invalid_input",
                "warnings": [{"code": "missing_fields", "severity": "error", "message": "strict parse failed"}],
            },
        )

    def fake_fallback(work_item):
        nonlocal fallback_calls
        fallback_calls += 1
        return AgentResult(agent_name="fallback_extraction_agent", payload={"status": "extracted"})

    monkeypatch.setattr("apps.orchestrator_agent.worker._dispatch_to_specialist", fake_dispatch)
    monkeypatch.setattr("apps.orchestrator_agent.worker.process_fallback_extraction_work_item", fake_fallback)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _live_staff_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T09:43:59Z",
                    "sender": "fallback-disabled",
                },
            },
        )
    )

    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "hr_performance", "*.meta.json")

    assert fallback_calls == 0
    assert result.payload["status"] == "rejected"
    assert len(rejected_meta_paths) == 1


def test_orchestrator_uses_cleaned_text_for_routing_and_specialist_processing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    normalized_inputs: list[str] = []
    specialist_inputs: list[str] = []
    original_normalize_headers = orchestrator_worker.normalize_headers
    original_dispatch_to_specialist = orchestrator_worker._dispatch_to_specialist
    raw_text = "  DAY-END SALES REPORT\r\nBranch: Waigani\r\nDate: 2026-04-07\r\nGross Sales: 1250\r\nCash Sales: 750\r\nEftpos Sales: 500\r\nTraffic: 24\r\nServed: 19\r\nCashier: Alice  "
    cleaned_text = "DAY-END SALES REPORT\nBranch: Waigani\nDate: 2026-04-07\nGross Sales: 1250\nCash Sales: 750\nEftpos Sales: 500\nTraffic: 24\nServed: 19\nCashier: Alice"

    def capture_normalize_headers(text: str):
        normalized_inputs.append(text)
        return original_normalize_headers(text)

    def capture_dispatch_to_specialist(work_item, *, target_agent):
        specialist_inputs.append(work_item.payload["raw_message"]["text"])
        return original_dispatch_to_specialist(work_item, target_agent=target_agent)

    monkeypatch.setattr(orchestrator_worker, "normalize_headers", capture_normalize_headers)
    monkeypatch.setattr(orchestrator_worker, "_dispatch_to_specialist", capture_dispatch_to_specialist)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": raw_text},
                "cleaned_text": cleaned_text,
                "pre_ingestion_validation": {
                    "status": "cleaned",
                    "cleaned_text": cleaned_text,
                    "reasons": [{"code": "normalized_line_endings", "message": "normalized"}],
                    "warnings": [],
                    "detected_risks": [],
                    "suggested_report_family": "sales",
                    "validator_version": "v1",
                },
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "validator-smoke",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")

    assert result.payload["status"] == "accepted"
    assert normalized_inputs[0] == cleaned_text
    assert specialist_inputs == [cleaned_text]
    assert len(raw_text_paths) == 1
    assert raw_text_paths[0].read_text(encoding="utf-8") == raw_text.replace("\r\n", "\n")


def test_orchestrator_passes_pre_ingestion_validation_metadata_through_safely(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    captured_validation: list[dict[str, object]] = []
    original_dispatch_to_specialist = orchestrator_worker._dispatch_to_specialist
    validation_payload = {
        "status": "accepted",
        "cleaned_text": "DAY-END SALES REPORT\nBranch: Waigani\nDate: 2026-04-07\nGross Sales: 1250\nCash Sales: 750\nEftpos Sales: 500\nTraffic: 24\nServed: 19\nCashier: Alice",
        "reasons": [],
        "warnings": [],
        "detected_risks": ["mixed_report_risk"],
        "suggested_report_family": "sales",
        "validator_version": "v1",
    }

    def capture_dispatch_to_specialist(work_item, *, target_agent):
        captured_validation.append(work_item.payload["pre_ingestion_validation"])
        return original_dispatch_to_specialist(work_item, target_agent=target_agent)

    monkeypatch.setattr(orchestrator_worker, "_dispatch_to_specialist", capture_dispatch_to_specialist)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": validation_payload["cleaned_text"]},
                "cleaned_text": validation_payload["cleaned_text"],
                "pre_ingestion_validation": validation_payload,
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "validator-pass-through",
                },
            },
        )
    )

    assert result.payload["status"] == "accepted"
    assert captured_validation == [validation_payload]


def test_orchestrator_keeps_existing_behavior_when_validator_metadata_is_absent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    specialist_inputs: list[str] = []
    original_dispatch_to_specialist = orchestrator_worker._dispatch_to_specialist
    raw_text = "\n".join(
        [
            "DAY-END SALES REPORT",
            "Branch: Waigani",
            "Date: 2026-04-07",
            "Gross Sales: 1250",
            "Cash Sales: 750",
            "Eftpos Sales: 500",
            "Traffic: 24",
            "Served: 19",
            "Cashier: Alice",
        ]
    )

    def capture_dispatch_to_specialist(work_item, *, target_agent):
        specialist_inputs.append(work_item.payload["raw_message"]["text"])
        return original_dispatch_to_specialist(work_item, target_agent=target_agent)

    monkeypatch.setattr(orchestrator_worker, "_dispatch_to_specialist", capture_dispatch_to_specialist)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": raw_text},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "baseline-behavior",
                },
            },
        )
    )

    assert result.payload["status"] == "accepted"
    assert specialist_inputs == [raw_text]


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")


def _paths(directory: Path, pattern: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob(pattern))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _valid_sales_candidate_payload(
    *,
    status: str = "accepted",
    warnings: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "signal_type": "sales_income",
        "source_agent": "sales_income_agent",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "confidence": 0.95,
        "metrics": {
            "gross_sales": 1200.0,
            "cash_sales": 700.0,
            "eftpos_sales": 500.0,
            "mobile_money_sales": 0.0,
            "traffic": 12,
            "served": 10,
        },
        "items": [],
        "warnings": warnings or [],
        "status": status,
    }


def _live_staff_performance_text() -> str:
    return "\n".join(
        [
            "TTC LAE MALAITA BRANCH",
            "TUESDAY 07 /04/26",
            "",
            "➡️STAFF PERFORMANCE REPORT ",
            "",
            "1..Debra Aegobi -Off",
            "SECTION.. Kids Girl shirt, Baby Overall, kids Girls and Baby pants.  ",
            "🔹Total items moved (-) ",
            "🔹Assist  (-) ",
            "",
            "2..Rodah Paku - 4",
            "SECTION..Kids Girls Dress, Jumpsuit, kids Polo t-shirt ",
            "🔹Total items Moved (51) ",
            "🔹Assist (07) ",
            "",
            "3..Julie Yorkie- 5 (Cashier )",
            "SECTION (Vacant).. Shoe Shop- Shoes, Handbags, Shopping bags ",
            "🔹Total items moved (-) ",
            "🔹Assist(-)",
            "",
            "4..Rodah  Frank - 5",
            "SECTION.. Price Room - Sales Tally ",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "5..Jesina Poknga -5",
            "SECTION.. Ladies Jeans, Rip Jeans,Skinny Jeans",
            "🔹Total items moved (37)",
            "🔹Assist(14)",
            "",
            "6..Nathan Moti - Sick",
            "SECTION.. , Beach wear sports wear,Jackets",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "7.Matthew Manu -4",
            "SECTION.. Reflectors, workwear, Men's button shirt, Socks ",
            "🔹Total items moved (24) ",
            "🔹Assist (19) ",
            "",
            "8.. Pison Orie -Off",
            "SECTION.. Ladies Tshirt, Ladies Long Dress, Crop Top, Singlet",
            "🔹Total items moved (-) ",
            "🔹Assist(-) ",
            "",
            "9...Herish Waizepa - 4",
            "SECTION.. Men's Jeans, Camouflage, Kids Girls Jeans ",
            "🔹Total items moved (30) ",
            "🔹Assist (12) ",
            "",
            "10..Medlyn Sehamo - Off",
            "SECTION.. Men's T-shirt, Household Rummage ",
            "🔹Total items moved (-) ",
            "🔹Assist(-) ",
            "",
            "11.Movzii Tuwasa -Off",
            "SECTION.. Kids Boy Pants, Kids Shorts, Comforter",
            "🔹Total items moved (-) ",
            "🔹Assist (-)",
            "",
            "",
            "12 ..Amos Waizepo - 4",
            "SECTION.. Ladies Jackets,Teregal Dress, HHR",
            "🔹Total items moved (31) ",
            "🔹Assist (11) ",
            "",
            "13 . Samson Billy - 5",
            "SECTION.. Door Man (Made sales for items on display at doorway)",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "14..Raymond Koyem -Off",
            "SECTION.. Doorman (Made sales for items on display at doorway)",
            "Items: -",
            "Assist: -",
            "",
            "15. Tabitha Lonobin -5",
            " SECTION - Men's Tshirt,Jackets",
            "Items: 29",
            "Asssist: 07",
            "",
            "16.Shandy Essau - Off",
            "SECTION. Ladies Silk Blouse, Ladies T-Shirt,Crop top, Ladies Skirt ",
            "Item:-",
            "Assist: -",
            "",
            "17.Dorish Molong -3",
            "SECTION. Ladies Cotton Capri, Ladies Colour Jeans, Cotton Pants ",
            "Items:31",
            "Assist: 13",
            "",
            "18.Gizard Joe - 4",
            "SECTION, Men's Shorts ",
            "Items: 25",
            "Assist: 12",
            "",
            "19.Nason Mapia -5",
            "SECTION. Ladies Jeans, Men's Cotton Pants, Ladies Leggings ",
            "Items: 11",
            "Assists:07",
            "",
            "20.Julie Yorkie (Cashier)(Slow moving bale- special price) Pricing- Rhoda Frank",
            "Items Sold: -",
            "",
            "Staff who work in price room:",
            "1.Kerry Iki ",
            "2.Abilen Yawano ",
            "3.Willmah Langa ",
            "4.Rhoda Frank (Work on slow moving bale)",
            "5.Renate Norman-- Till Assistant ",
            "",
            "THANKS...",
        ]
    )


def _backlog_attendance_text() -> str:
    return "\n".join(
        [
            "TTC LAE ",
            "5th Street branch ",
            "Monday 05/04/2026",
            "",
            "Staffs  Attendance",
            "",
            "1. Marryane Sakias =✔️",
            "2. ⁠Imelda Patrick = ✔️",
            "3. ⁠Merolyne Tobby = Off",
            "4. ⁠George Andau = ✔️",
            "5. ⁠Cloe Wofinga =On leave",
            "6. ⁠ Doil Wai-ah=  Off",
            "7. ⁠Donock Levi = ✔️",
            "8. ⁠Joyice Andrew = Off",
            "9. ⁠Jackson Kuri = ✔️",
            "10. ⁠Jennifer Golomb = ✔️",
            "11. ⁠Sheeba I=✔️",
            "12. ⁠Lieb Yawano = ✔️ ",
            "13. ⁠Hazel Arumbu = ✔️",
            "14. Anuty Mina = ✔️",
            "15. Joyce Lovave =✔️",
            "16. Sandra Daniel = Absent with notice",
            "17. ⁠Joycelyn Alu = ✔️",
            "",
            "Total Staffs present = 12",
            " Staffs day off = 3",
            "Staffs on leavebreak =1",
            "Staffs suspend =Nill ",
            "Staffs late = Nill",
            "Staffs Absent with notice = 1",
            "Staffs Absent without =Nill ",
            "Staffs sick =Nill ",
            "Staffs Lay off =Nill ",
            "",
            "Total Staffs =17",
            "",
            "Note ",
            "1. Cleo Wofinga on her annual leave break ",
            "2. ⁠Sandra Daniel: Absent with notice because of some personal/family problem.",
            "",
            "Thanks",
        ]
    )


def _backlog_bale_release_text() -> str:
    return "\n".join(
        [
            "📦 DAILY BALE SUMMARY – RELEASED TO RAIL",
            "",
            "Branch: TTC POM Waigani Branch.",
            " ",
            "Date: 06/04/26",
            "Monday.",
            "",
            "Bale #\tItem Name\tTotal Qty (pcs)\tTotal Amount (K)",
            "",
            "#01.OSH 45kg",
            "(Qty:229) ",
            " Amt:K2,089.00",
            "",
            "#02.Soft Toys 10kg",
            "(Qty:46)",
            "Amt:K392.00",
            "",
            "#03.Ladies t-shirt XL creme 39kg",
            "(Qty.153)",
            "Amt:K801.00",
            "",
            "",
            "Total bales break today Five(05)",
            "Three(03) bales released for sales,",
            "Two (02) bales waiting for approval.",
            "",
            "Total quantity:428pcs",
            "",
            "Total Amount: K3,281.00",
            "",
            "Prepared by:Moviyo Alex(Pricing Clerk)",
            "",
            "Thanks.",
        ]
    )


def _backlog_staff_performance_text() -> str:
    return "\n".join(
        [
            "TTC LAE MALAITA BRANCH",
            "TUESDAY 07 /04/26",
            "",
            "➡️STAFF PEFORMANCE RATING, CUSTOMER ASSISTS &  ITEMS SOLD... ",
            "",
            "1..Debra Aegobi -Off",
            "SECTION.. Kids Girl shirt, Baby Overall, kids Girls and Baby pants.  ",
            "🔹Total items moved (-) ",
            "🔹Assist  (-) ",
            "",
            "2..Rodah Paku - 4",
            "SECTION..Kids Girls Dress, Jumpsuit, kids Polo t-shirt ",
            "🔹Total items Moved (51) ",
            "🔹Assist (07) ",
            "",
            "3..Julie Yorkie- 5 (Cashier )",
            "SECTION (Vacant).. Shoe Shop- Shoes, Handbags, Shopping bags ",
            "🔹Total items moved (-) ",
            "🔹Assist(-)",
            "",
            "4..Rodah  Frank - 5",
            "SECTION.. Price Room - Sales Tally ",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "5..Jesina Poknga -5",
            "SECTION.. Ladies Jeans, Rip Jeans,Skinny Jeans",
            "🔹Total items moved (37)",
            "🔹Assist(14)",
            "",
            "6..Nathan Moti - Sick",
            "SECTION.. , Beach wear sports wear,Jackets",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "7.Matthew Manu -4",
            "SECTION.. Reflectors, workwear, Men's button shirt, Socks ",
            "🔹Total items moved (24) ",
            "🔹Assist (19) ",
            "",
            "8.. Pison Orie -Off",
            "SECTION.. Ladies Tshirt, Ladies Long Dress, Crop Top, Singlet",
            "🔹Total items moved (-) ",
            "🔹Assist(-) ",
            "",
            "9...Herish Waizepa - 4",
            "SECTION.. Men's Jeans, Camouflage, Kids Girls Jeans ",
            "🔹Total items moved (30) ",
            "🔹Assist (12) ",
            "",
            "10..Medlyn Sehamo - Off",
            "SECTION.. Men's T-shirt, Household Rummage ",
            "🔹Total items moved (-) ",
            "🔹Assist(-) ",
            "",
            "11.Movzii Tuwasa -Off",
            "SECTION.. Kids Boy Pants, Kids Shorts, Comforter",
            "🔹Total items moved (-) ",
            "🔹Assist (-)",
            "",
            "",
            "12 ..Amos Waizepo - 4",
            "SECTION.. Ladies Jackets,Teregal Dress, HHR",
            "🔹Total items moved (31) ",
            "🔹Assist (11) ",
            "",
            "13 . Samson Billy - 5",
            "SECTION.. Door Man (Made sales for items on display at doorway)",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "14..Raymond Koyem -Off",
            "SECTION.. Doorman (Made sales for items on display at doorway)",
            "Items: -",
            "Assist: -",
            "",
            "15. Tabitha Lonobin -5",
            " SECTION - Men's Tshirt,Jackets",
            "Items: 29",
            "Asssist: 07",
            "",
            "16.Shandy Essau - Off",
            "SECTION. Ladies Silk Blouse, Ladies T-Shirt,Crop top, Ladies Skirt ",
            "Item:-",
            "Assist: -",
            "",
            "17.Dorish Molong -3",
            "SECTION. Ladies Cotton Capri, Ladies Colour Jeans, Cotton Pants ",
            "Items:31",
            "Assist: 13",
            "",
            "18.Gizard Joe - 4",
            "SECTION, Men's Shorts ",
            "Items: 25",
            "Assist: 12",
            "",
            "19.Nason Mapia -5",
            "SECTION. Ladies Jeans, Men's Cotton Pants, Ladies Leggings ",
            "Items: 11",
            "Assists:07",
            "",
            "20.Julie Yorkie (Cashier)(Slow moving bale- special price) Pricing- Rhoda Frank",
            "Items Sold: -",
            "",
            "Staff who work in price room:",
            "1.Kerry Iki ",
            "2.Abilen Yawano ",
            "3.Willmah Langa ",
            "4.Rhoda Frank (Work on slow moving bale)",
            "5.Renate Norman-- Till Assistant ",
            "",
            "THANKS...",
        ]
    )


def _backlog_mixed_sales_and_supervisor_text() -> str:
    return "\n".join(
        [
            "TTC WAIGANI ",
            "Date: Friday 10/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Till#1: Main Shop",
            "Cashier: Fidelma Wobiro",
            "Assistant: Privien/Anita",
            "",
            "T/Cash:K3,005.00",
            "T/Card: K841.00",
            "Z/Reading: K3,846.00",
            "Balance: Yes",
            "",
            "DAY-END SALES REPORT",
            "Till#2: ",
            "Cashier: Dorothy Morofa",
            "Assistant: David",
            "",
            "T/Cash:K2,845.80",
            "T/Card: K929.80",
            "Z/Reading: K3,775.60",
            "Balance: Yes",
            "",
            "TOTALS",
            "Total Cash: K5,850.80",
            "Total Card: K1,770.80",
            "Total Sales: K7,620.60",
            "",
            "CUSTOMER COUNT",
            "Main Door: 355",
            "Guest/customer serve: 192",
            "",
            "Balanced by: Privien",
            "",
            "ADDITIONAL INFORMATION. ",
            "",
            "*Sales per labor hours =K229.50",
            "",
            "*Sale per customer:= K39.70",
            "",
            "*Conversion rate:= 54%",
            "",
            "--------------------------------",
            "",
            "Supervisor Control Summary",
            "Date: Friday 10/04/26",
            "Branch: Waigani",
            "Supervisor: Privien (acting)",
            "",
            "Cash variance: No",
            "Staffing issues: No",
            "Stock issues affecting sales: No",
            "Pricing or system issues:NO ",
            "",
            "Exceptions escalated to Ops Manager: ",
            "",
            "Supervisor confirmation:",
            "All material issues have been escalated.",
        ]
    )


def _mixed_sales_and_performance_text() -> str:
    return "\n".join(
        [
            "TTC WAIGANI BRANCH",
            "Date: 07/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "Eftpos Sales: 600",
            "Till Total: 600",
            "Deposit Total: 600",
            "Traffic: 12",
            "Served: 9",
            "Labor Hours: 4",
            "",
            "STAFF PERFORMANCE REPORT",
            "1.Alice Demo - 5",
            "SECTION. Men's Tshirt",
            "Items: 10",
            "Assist: 2",
        ]
    )


def _sales_report_text() -> str:
    return "\n".join(
        [
            "DAY-END SALES REPORT",
            "Branch: Waigani",
            "Date: 2026-04-07",
            "Gross Sales: 1250",
            "Cash Sales: 750",
            "Eftpos Sales: 500",
            "Traffic: 24",
            "Served: 19",
            "Cashier: Alice",
        ]
    )


def _live_sales_income_text() -> str:
    return "\n".join(
        [
            "TTC LAE 5TH STREET BRANCH",
            "Friday, 10/04 /26",
            "",
            "DAY end sales report",
            "Shop: Lae 5th Street Branch",
            "Date: Friday, 10/04 /26",
            "Total Sales: 1200",
            "Cash Sales: 700",
            "Card Sales: 500",
            "Main Door: 15",
            "Customers Served: 12",
            "Remark: Balanced and checked",
        ]
    )


def _invalid_bale_pricing_card_text() -> str:
    return "\n".join(
        [
            "BALE PRICING CARD",
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "Bale # 01",
            "Item: Ladies Mixed Tops",
            "Wt: 45kg",
            "A: 60 pcs",
            "B: 40 pcs",
            "C: 15 pcs",
            "Sales: K1,150.00",
            "Pricer: Maria Sine",
        ]
    )


def _unknown_attendance_text() -> str:
    return "\n".join(
        [
            "𝕋𝕋ℂ ℙ𝕆𝕄",
            "𝕎𝔸𝕀𝔾𝔸ℕ𝕀 𝔹ℝ𝔸ℕℂℍ",
            "",
            "MONDAY:06/04/26",
            "",
            "STAFFS ATTENDANCE.",
            "",
            "1.Alice KOKO = ✔️ ",
            "2.Grace MASSON= ✔️ ",
            "3.Fidelma WOBILO= Off ",
            "4.David YARO = ✔️ ",
            "5.Agnes TIMOTHY= ✔️ ",
            "6.Moviyo Alex= ✔️ ",
            "7.Sabila Seka= ✔️ ",
            "8.Mitege Laiken= ✔️ ",
            "9.Xeena Moris= ✔️ ",
            "10.Anita Tangoi = ✔️ ",
            "11.Pricilla Billy = ✔️ ",
            "12.Dorothy Morofa = ✔️ ",
            "13.Micheal Peter = Off ",
            "14.Nim Jonnah= ✔️ ",
            "15.Kahy Magi= ✔️ ",
            "16.Rebeca Bob= Off ",
            "17.Mathew Gene= ✔️ ",
            "18.Privien Keiby = Off ",
            "19.Milford Timothy = ✔️ ",
            "20.Bethsien  Ken= ✔️ ",
            "21.Stanly Mathias = ✔️ ",
            "22.Debra Wotavo = ✔️ ",
            "23.Kimson David= ✔️ ",
            "24.Hendry Ambiu = ✔️ ",
            "25.Francis Ano = ✔️ ",
            "26.Gassy Panagu= Off ",
            "27.Ricky Lomutopa= ✔️ ",
            "38.Epu Yasa= Off",
            "39.Florida Dava= ✔️ ",
            "",
            "Total staffs Press/=23",
            "",
            "Day off/=06",
            "",
            "Total staff: 29",
            "",
            "Note:",
            "",
            "1. Handry Ambui and Nim Jonnah continue to work this morning.",
            "",
            "Thanks",
        ]
    )


def _unknown_waigani_stuff_performance_text() -> str:
    return "\n".join(
        [
            "➡️STUFF PERFORMANCE REPORT... ",
            "",
            "BRANCH::WAIGANI   ",
            "",
            "Date:: 05/04/2026",
            "            Sunday",
            "",
            "☑️Prepared by :: DAVID YAILO",
            "",
            "➡️STUFF 1...",
            "",
            "Staff Name > MILFORD ",
            "",
            "Section > MANS COTTON PANTS",
            "",
            "Item Assisting >  7",
            "",
            "Items sold assisting > 130",
            "",
            "🔸Arrangements  > 5",
            "",
            "🔸DISPLAY > 5",
            "",
            "🔸Performance > 5",
            "",
            "     ➡️STUFF 2.",
            "",
            "Name.. KIMSON",
            "",
            "section.. M6PKTS   / PANTS ",
            "",
            "Item assisting.. 13",
            "",
            "Items sold assisting.. 24",
            "",
            "🔹Arrangements... 5",
            "",
            "🔹Display.. 5",
            "",
            "🔹Performance.. 5",
            "",
            "➡️STUFF 3.",
            "",
            "Name.. GRACE",
            "",
            "Section.. PACIFIC SHIRTS, SHOE",
            "",
            "Item assisting.. 6",
            "",
            "Item sold assisting.. 55",
            "",
            "🔹Arrangements. 5",
            "🔹Display. 5",
            "🔹Performance. 5",
            "",
            "➡️STUFF 4.",
            "",
            "Name.. PRICILLA",
            "",
            "Section.. MANS MTSH",
            "",
            "Item assisting.. 9",
            "",
            "Item sold assisting.. 84",
            "",
            "🔹Arrangements. 5",
            "",
            "🔹Display. 5",
            "",
            "🔹Performance. 5",
            "",
            "➡️STUFF 5.",
            "",
            "Name.. DOROTHY",
            "",
            "Section.. BOYS SECTION",
            "",
            "Item assisting.. 17",
            "",
            "Item sold assisting.. 63",
            "",
            "🔹Arrangements",
            "",
            "🔹Display",
            "",
            "🔹Performance.. 5",
            "",
            "➡️STUFF 6.",
            "",
            "Name.. EPU",
            "",
            "Section.. MANS SHORTS",
            "",
            "Item assisting.. 11",
            "",
            "Item sold assisting..77",
            "",
            "🔹Arrangements.. 4",
            "",
            "🔹Display. 5",
            "",
            "🔹Performance. 5",
            "",
            "➡️STUFF 7.",
            "",
            "Name... GASSY",
            "",
            "Section.. MANS BUTTON SHIRTS",
            "",
            "Item assisting.. 12",
            "",
            "Item sold assisting.. 44",
            "",
            "🔹Arrangements.. 5",
            "🔹Display.. 4",
            "🔹Performance. 5",
            "",
            "➡️STUFF 8.",
            "",
            "Name.. MATTHEW",
            "",
            "Section.. MANS BUTTON SHIRTS",
            "",
            "Item assisting.. ",
            "Item sold assist..",
            "",
            "🔹Arrangements. ",
            "🔹Display. ",
            "🔹Performance.",
            "🔸OFF",
            "",
            "➡️STUFF 9.",
            "",
            "Name.. NIM",
            "",
            "Section.. BEACH WEARS",
            "",
            "Item assist.. ",
            "",
            "Item sold assist.. ",
            "",
            "🔹Arrangement. ",
            "",
            "🔹Display. ",
            "",
            "🔹Performance. ",
            "🔸SENT HOME. ",
            "",
            "➡️STUFF 10.",
            "",
            "Name.. FLORIDA",
            "",
            "Section.. SOCCER SHORTS",
            "",
            "Item assist.. 5",
            "Item assist section.. 21",
            "",
            "🔹Arrangements. 4",
            "🔹Display. 5",
            "🔹Performance. 5",
        ]
    )


def _lae_5th_street_parser_failure_text() -> str:
    return "\n".join(
        [
            "TTC LAE ",
            "5th Street branch ",
            "Sunday 05/04/2026",
            "",
            "Update For Staff",
            "Performance Assisting",
            "Customers and doing sales for today ",
            "",
            "1.Marryane Sakias ={ Pricing cluck}",
            "=Items sold=OFF ",
            "=Customers assist=OFF ",
            "",
            "2.Imelda Patrick. ={Cashier}",
            "=Items sold=cashier ",
            "=Customers assist=cashier",
            "",
            "3.Merolyne Tobby. ",
            "={Pricing room}",
            "=Items sold=13 ",
            "=Customers assist=8",
            "",
            "4.George Andau .",
            "{Door Man}",
            "=Items sold=6",
            "=Customers assist=4",
            "",
            "5.Cloe Wofinga ",
            "{Skirt and dress section}",
            "=Items sold=Leave",
            "=Customers assist=Leave",
            "",
            "6.Jacksion Kuri",
            ".{Beach were sports were , tawel and HHR section}",
            "=Items sold=13",
            "=Customers assist=8",
            "",
            "7.Doil Wai-ah",
            "={Mans jeans,  T-shirt,collar  shirt and jacket section}",
            "=Items sold=33",
            "=Customers assist=4",
            "",
            "8.Jannefer  Golomb.",
            "{Reflector ,comfles  man's cotton pants and baby blanket section}",
            "=Items sold=11",
            "=Customers assist=5",
            "",
            "9.Donok Levi",
            "{Mans shorts and kids girls trecshut section}",
            "=Items sold=19",
            "=Customers assist=4",
            "",
            "10.Joyce Andrew ",
            "{Ladies tops , Ladies silk blouse and ladies T-shirt section}",
            "=Items sold=18",
            "=Customers assist=6",
            "",
            "11.Hazel Arumbu",
            "{Kids girls jeans, dress, T-shirt and kids boys Jeans, cotton pants, shorts T-shirt section",
            "=Items sold=AWN",
            "=Customers assist=AWN",
            "",
            "12.Sheebah Ikiso ",
            "{Ladies Shorts  ,Colour jeans, and ladies Capri section}",
            "=Items sold=OFF ",
            "=Customers assist=OFF ",
            "",
            "13.Anuty Mina",
            "{Pricing room}",
            "Items sold=Absent ",
            "Customers assist=Absent ",
            "",
            "14.Lieb Yawano",
            "{Ladies jeans section}",
            "=Items sold =12",
            "=Customers assist=4",
            "",
            "15.Joyce Lovave ",
            "{Supervisor )",
            "=OFF ",
            "",
            "16.Sandra Daniel",
            "{Ladies dress and skirt section}",
            "=Items sold=15",
            "=Customers assist=3",
            "",
            "TOTAL ITEMS SOLD OUT=152",
            "",
            "TOTAL CUSTOMERS ASSIST=41",
            "",
            "The total numbers of items above are different items sold out by individual Staff with there number of customers assist.",
            "",
            "Thank you.",
        ]
    )


def _supervisor_control_report_text() -> str:
    return "\n".join(
        [
            "Supervisor Control Report",
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "Exception Type: STAFF_ISSUE",
            "Details: Late opening",
            "Action Taken: Resolved",
            "Escalated By: Francis",
            "Time: 08:30",
            "Supervisor Confirmed: YES",
            "Notes: Transport delay",
        ]
    )
