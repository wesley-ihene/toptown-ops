"""Phase 2 mixed-report detection and splitting tests."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.mixed_content_detector_agent.worker import BoundaryHint, MixedContentDetection, detect_mixed_content
from apps.orchestra.classifier import classify_work_item
from apps.orchestra.intake import intake_raw_message
from apps.orchestra.splitter import split_work_item as split_orchestra_work_item
from apps.orchestrator_agent.worker import process_work_item
from apps.report_splitter_agent.worker import split_report
from packages.signal_contracts.work_item import WorkItem


def test_detector_identifies_sales_and_supervisor_control_as_mixed_report() -> None:
    detection = detect_mixed_content(_sales_and_supervisor_control_text())

    assert detection.classification == "mixed_report"
    assert detection.is_mixed is True
    assert detection.detected_families == ["sales_income", "supervisor_control"]
    assert len(detection.boundary_hints) == 2
    assert detection.confidence >= 0.9


def test_detector_keeps_sales_with_trailing_operational_notes_as_single_report() -> None:
    detection = detect_mixed_content(_sales_with_operational_notes_text())

    assert detection.classification == "single_report_with_noncritical_trailing_notes"
    assert detection.is_mixed is False
    assert detection.detected_families == ["sales_income"]


def test_detector_keeps_staff_performance_with_staff_room_note_as_single_report() -> None:
    detection = detect_mixed_content(_performance_with_staff_room_note_text())

    assert detection.classification == "single_report_with_noncritical_trailing_notes"
    assert detection.is_mixed is False
    assert detection.detected_families == ["staff_performance"]


def test_splitter_handles_messy_title_variants_without_overlapping_segments() -> None:
    detection = detect_mixed_content(_messy_mixed_title_text())
    split_result = split_report(_messy_mixed_title_text(), detection)

    assert detection.is_mixed is True
    assert [segment.detected_report_family for segment in split_result.segments] == [
        "sales_income",
        "supervisor_control",
    ]
    assert split_result.segments[0].segment_index == 0
    assert split_result.segments[1].segment_index == 1
    assert split_result.segments[0].end_line < split_result.segments[1].start_line
    assert split_result.segments[0].segment_id != split_result.segments[1].segment_id


def test_splitter_forces_confidence_for_real_world_sales_and_supervisor_summary_sections() -> None:
    text = _real_world_sales_and_supervisor_control_summary_text()
    detection = MixedContentDetection(
        classification="mixed_report",
        is_mixed=True,
        detected_families=["sales_income", "supervisor_control"],
        confidence=0.4,
        boundary_hints=[
            BoundaryHint(
                report_family="sales_income",
                line_number=4,
                raw_line="DAY-END SALES REPORT",
                normalized_line="day end sales report",
            ),
            BoundaryHint(
                report_family="supervisor_control",
                line_number=16,
                raw_line="Supervisor Control Summary",
                normalized_line="supervisor control summary",
            ),
        ],
    )

    split_result = split_report(text, detection)

    assert [segment.detected_report_family for segment in split_result.segments] == [
        "sales_income",
        "supervisor_control",
    ]
    assert split_result.split_confidence == 0.95
    assert all(segment.split_confidence == 0.95 for segment in split_result.segments)


def test_orchestra_splitter_recognizes_supervisor_control_summary_sections() -> None:
    work_item = intake_raw_message(
        {"text": _sales_and_supervisor_control_summary_text()},
        received_at_utc="2026-04-07T13:00:00Z",
    )

    classification = classify_work_item(work_item)
    split_result = split_orchestra_work_item(work_item)

    assert classification.report_type == "mixed"
    assert split_result.was_split is True
    assert [child.payload["classification"]["report_type"] for child in split_result.child_work_items] == [
        "sales",
        "supervisor_control",
    ]
    assert split_result.child_work_items[0].payload["raw_message"]["text"].startswith("Total Sales:")
    assert split_result.child_work_items[1].payload["raw_message"]["text"].startswith("Floor Check:")


def test_orchestra_splitter_recognizes_supervisor_summary_alias_after_separator() -> None:
    work_item = intake_raw_message(
        {"text": _sales_and_supervisor_summary_alias_text()},
        received_at_utc="2026-04-07T13:00:00Z",
    )

    classification = classify_work_item(work_item)
    split_result = split_orchestra_work_item(work_item)

    assert classification.report_type == "mixed"
    assert split_result.was_split is True
    assert [child.payload["classification"]["report_type"] for child in split_result.child_work_items] == [
        "sales",
        "supervisor_control",
    ]
    assert split_result.child_work_items[0].payload["raw_message"]["text"].startswith("Total Sales:")
    assert split_result.child_work_items[1].payload["raw_message"]["text"].startswith("Floor Check:")


def test_orchestrator_single_report_fast_path_remains_single_specialist_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_report_text()},
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "fast-path",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert result.agent_name == "sales_income_agent"
    assert result.payload["signal_type"] == "sales_income"


def test_orchestrator_mixed_sales_and_supervisor_control_writes_two_structured_records(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_and_supervisor_control_text()},
                "metadata": {
                    "received_at": "2026-04-07T13:00:00Z",
                    "sender": "mixed-supervisor",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    supervisor_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-07.json"
    assert sales_path.exists()
    assert supervisor_path.exists()

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["status"] in {"accepted_split", "accepted_with_warning"}
    assert len(result.payload["fanout"]["children"]) == 2

    child_one, child_two = result.payload["fanout"]["children"]
    assert child_one["report_family"] == "sales_income"
    assert child_two["report_family"] == "supervisor_control"
    assert child_one["lineage"]["derived_from_mixed_report"] is True
    assert child_two["lineage"]["split_source_agent"] == "orchestrator_agent"
    assert child_one["lineage"]["parent_raw_txt_path"].endswith(".txt")
    assert child_two["lineage"]["parent_raw_sha256"]

    supervisor_payload = json.loads(supervisor_path.read_text(encoding="utf-8"))
    assert supervisor_payload["signal_type"] == "supervisor_control"
    assert supervisor_payload["branch"] == "waigani"
    assert supervisor_payload["report_date"] == "2026-04-07"


def test_orchestrator_accepts_split_for_sales_and_supervisor_control_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _real_world_sales_and_supervisor_control_summary_text()},
                "metadata": {
                    "received_at": "2026-04-07T13:00:00Z",
                    "sender": "mixed-summary",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    supervisor_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-07.json"

    assert sales_path.exists()
    assert supervisor_path.exists()
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "accepted_split"
    assert len(result.payload["fanout"]["children"]) == 2

    child_one, child_two = result.payload["fanout"]["children"]
    assert child_one["report_family"] == "sales_income"
    assert child_one["agent_name"] == "sales_income_agent"
    assert child_one["status"] == "accepted"
    assert child_two["report_family"] == "supervisor_control"
    assert child_two["agent_name"] == "supervisor_control_agent"
    assert child_two["status"] == "accepted"


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")


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


def _sales_and_supervisor_control_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
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
            "SUPERVISOR CONTROL REPORT",
            "Floor Check: Passed",
            "Cashier Reconciled: Yes",
            "- Front door display checked",
        ]
    )


def _sales_and_supervisor_control_summary_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Total Sales: 1200",
            "Cash Sales: 700",
            "Card Sales: 500",
            "Main Door: 15",
            "Customers Served: 12",
            "",
            "--------------------------------",
            "",
            "Supervisor Control Summary",
            "Floor Check: Passed",
            "Cashier Reconciled: Yes",
            "Store Locked: Yes",
        ]
    )


def _real_world_sales_and_supervisor_control_summary_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Till#1: Main Shop",
            "Cashier: Alice",
            "Assistant: Bob",
            "",
            "T/Cash: K700",
            "T/Card: K500",
            "Z/Reading: K1200",
            "Guest/customer serve: 12",
            "",
            "--------------------------------",
            "",
            "Supervisor Control Summary",
            "Floor Check: Passed",
            "Cashier Reconciled: Yes",
            "Store Locked: Yes",
        ]
    )


def _sales_and_supervisor_summary_alias_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Total Sales: 1200",
            "Cash Sales: 700",
            "Card Sales: 500",
            "",
            "--------------------------------",
            "",
            "Supervisor Summary Acting Shift",
            "Floor Check: Passed",
            "Cashier Reconciled: Yes",
            "Store Locked: Yes",
        ]
    )


def _sales_with_operational_notes_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "Eftpos Sales: 600",
            "Traffic: 12",
            "Served: 9",
            "",
            "Operational Notes:",
            "Generator fuel is low for tomorrow.",
        ]
    )


def _performance_with_staff_room_note_text() -> str:
    return "\n".join(
        [
            "TTC WAIGANI BRANCH",
            "Monday 30/03/26",
            "STAFF PERFORMANCE REPORT",
            "1.Alice Demo - 5",
            "SECTION. Men's Tshirt",
            "Items: 10",
            "Assist: 2",
            "",
            "Staff Room Note:",
            "Two lockers need repair.",
        ]
    )


def _messy_mixed_title_text() -> str:
    return "\n".join(
        [
            "BRANCH : WAIGANI BRANCH",
            "DATE : 07/04/2026",
            "",
            "➡️ DAY-END SALES REPORT!!!",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "",
            "--- supervisor control report ---",
            "Store Locked: Yes",
            "Checklist Signed: Yes",
        ]
    )
