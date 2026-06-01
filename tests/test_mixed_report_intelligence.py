"""Phase 2 mixed-report detection and splitting tests."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.mixed_content_detector_agent.worker import BoundaryHint, MixedContentDetection, detect_mixed_content
from apps.orchestra.classifier import classify_work_item
from apps.orchestra.intake import intake_raw_message
from apps.orchestra.splitter import split_work_item as split_orchestra_work_item
from apps.orchestrator_agent.worker import _mixed_parent_decision, process_work_item
from apps.report_splitter_agent.worker import split_report
from packages.signal_contracts.work_item import WorkItem


def test_mixed_parent_decision_accepts_sales_and_supervisor_pair_when_both_children_accept() -> None:
    status, reason = _mixed_parent_decision(
        child_results=[],
        child_summaries=[
            _mixed_child_summary("sales_income", "accepted"),
            _mixed_child_summary("supervisor_control", "accepted"),
        ],
    )

    assert (status, reason) == ("accepted", None)


def test_mixed_parent_decision_keeps_sales_when_supervisor_child_needs_review() -> None:
    status, reason = _mixed_parent_decision(
        child_results=[],
        child_summaries=[
            _mixed_child_summary("sales_income", "accepted"),
            _mixed_child_summary("supervisor_control", "needs_review"),
        ],
    )

    assert (status, reason) == ("accepted_with_warning", None)


def test_mixed_parent_decision_reviews_pair_when_sales_child_needs_review() -> None:
    status, reason = _mixed_parent_decision(
        child_results=[],
        child_summaries=[
            _mixed_child_summary("sales_income", "needs_review"),
            _mixed_child_summary("supervisor_control", "accepted"),
        ],
    )

    assert (status, reason) == ("needs_review", "mixed_child_requires_review")


def test_mixed_parent_decision_preserves_warning_status_when_both_children_warn() -> None:
    status, reason = _mixed_parent_decision(
        child_results=[],
        child_summaries=[
            _mixed_child_summary("sales_income", "accepted_with_warning"),
            _mixed_child_summary("supervisor_control", "accepted_with_warning"),
        ],
    )

    assert (status, reason) == ("accepted_with_warning", None)


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
    assert split_result.segments[1].report_family_label == "intelligence"
    assert split_result.segments[1].blocks_transactional_processing is False


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
    assert split_result.child_work_items[1].payload["classification"]["report_family"] == "intelligence"
    assert split_result.child_work_items[1].payload["classification"]["blocks_transactional_processing"] is False
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
    assert split_result.child_work_items[1].payload["classification"]["report_family"] == "intelligence"
    assert split_result.child_work_items[1].payload["classification"]["blocks_transactional_processing"] is False
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
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json"
    assert sales_path.exists()
    assert supervisor_path.exists()

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["status"] in {"accepted", "accepted_with_warning"}
    assert result.payload["routing"]["review_reason"] is None
    assert "mixed_child_requires_review" not in result.payload["governance"]["reasons"]
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
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json"

    assert sales_path.exists()
    assert supervisor_path.exists()
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] in {"accepted", "accepted_with_warning"}
    assert result.payload["routing"]["review_reason"] is None
    assert len(result.payload["fanout"]["children"]) == 2

    child_one, child_two = result.payload["fanout"]["children"]
    assert child_one["report_family"] == "sales_income"
    assert child_one["agent_name"] == "sales_income_agent"
    assert child_one["status"] in {"accepted", "accepted_with_warning"}
    assert child_two["report_family"] == "supervisor_control"
    assert child_two["agent_name"] == "supervisor_control_agent"
    assert child_two["status"] == "accepted"


def test_orchestrator_keeps_incomplete_supervisor_control_as_non_blocking_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_and_incomplete_supervisor_control_text()},
                "metadata": {
                    "received_at": "2026-04-28T13:00:00Z",
                    "sender": "mixed-incomplete-supervisor",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "waigani.json"
    supervisor_validation_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "waigani.validation.json"
    legacy_structured_supervisor_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-28.json"

    assert sales_path.exists()
    assert supervisor_path.exists()
    assert not legacy_structured_supervisor_path.exists()
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["routing"]["review_reason"] is None
    assert result.payload["governance"]["reasons"] == []

    sales_child, supervisor_child = result.payload["fanout"]["children"]
    assert sales_child["report_family"] == "sales_income"
    assert sales_child["status"] == "accepted"
    assert supervisor_child["report_family"] == "supervisor_control"
    assert supervisor_child["report_family_label"] == "intelligence"
    assert supervisor_child["blocks_transactional_processing"] is False
    assert supervisor_child["status"] == "accepted"
    supervisor_warning_codes = {warning["code"] for warning in supervisor_child["payload"]["warnings"]}
    assert "missing_fields" in supervisor_warning_codes

    supervisor_validation = json.loads(supervisor_validation_path.read_text(encoding="utf-8"))
    assert supervisor_validation["validation"]["accepted"] is True
    assert supervisor_validation["validation"]["details"]["orchestrator_validation_bypassed"] is True
    assert "acceptance" not in supervisor_validation


def test_orchestrator_reviews_truncated_supervisor_summary_without_blocking_accepted_sales(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_and_truncated_supervisor_control_summary_text()},
                "metadata": {
                    "received_at": "2026-04-28T13:20:00Z",
                    "sender": "mixed-truncated-supervisor",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "waigani.json"

    assert sales_path.exists()
    assert not supervisor_path.exists()
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["routing"]["review_reason"] is None
    assert result.payload["governance"]["reasons"] == []

    sales_child, supervisor_child = result.payload["fanout"]["children"]
    assert sales_child["report_family"] == "sales_income"
    assert sales_child["status"] == "accepted"
    assert sales_child["child_index"] == 1
    assert sales_child["response_report_type"] == "day_end_sales"
    assert sales_child["reason"] == "totals_reconciled"

    assert supervisor_child["report_family"] == "supervisor_control"
    assert supervisor_child["status"] == "needs_review"
    assert supervisor_child["child_index"] == 2
    assert supervisor_child["response_report_type"] == "supervisor_control_summary"
    assert supervisor_child["response_status"] == "review"
    assert supervisor_child["validation_error_code"] == "child_report_incomplete_or_truncated"
    assert supervisor_child["validation_error_message"] == 'incomplete after "Exceptions es\u2026"'
    assert supervisor_child["output_paths"] == []


def test_orchestrator_records_supervisor_control_date_mismatch_without_blocking_sales(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_and_mismatched_supervisor_control_text()},
                "metadata": {
                    "received_at": "2026-04-28T13:15:00Z",
                    "sender": "mixed-supervisor-date-mismatch",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-22" / "waigani.json"
    legacy_structured_supervisor_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-22.json"

    assert sales_path.exists()
    assert supervisor_path.exists()
    assert not legacy_structured_supervisor_path.exists()
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["routing"]["review_reason"] is None
    assert result.payload["governance"]["reasons"] == []

    sales_child, supervisor_child = result.payload["fanout"]["children"]
    assert sales_child["status"] == "accepted"
    assert supervisor_child["status"] == "accepted"
    supervisor_warning_codes = {warning["code"] for warning in supervisor_child["payload"]["warnings"]}
    assert "supervisor_control_date_mismatch" in supervisor_warning_codes

    supervisor_payload = json.loads(supervisor_path.read_text(encoding="utf-8"))
    stored_warning_codes = {warning["code"] for warning in supervisor_payload["warnings"]}
    assert "supervisor_control_date_mismatch" in stored_warning_codes


def test_orchestrator_reviews_mixed_pair_when_transactional_child_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _invalid_sales_and_supervisor_control_text()},
                "metadata": {
                    "received_at": "2026-04-07T13:30:00Z",
                    "sender": "mixed-intelligence-transactional-failure",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-07" / "waigani.json"

    assert not sales_path.exists()
    assert supervisor_path.exists()
    assert result.agent_name == "orchestrator_agent"
    assert result.payload["status"] == "needs_review"
    assert result.payload["routing"]["review_reason"] == "mixed_child_requires_review"
    assert result.payload["governance"]["reasons"] == ["mixed_child_requires_review"]

    child_one, child_two = result.payload["fanout"]["children"]
    assert child_one["report_family"] == "sales_income"
    assert child_one["status"] == "rejected"
    assert child_two["report_family"] == "supervisor_control"
    assert child_two["status"] == "accepted"


def test_orchestrator_exposes_blocking_sales_child_details_for_mixed_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _sales_totals_mismatch_and_supervisor_control_text()},
                "metadata": {
                    "received_at": "2026-04-28T13:45:00Z",
                    "sender": "mixed-sales-totals-review",
                    "branch_hint": "bena_road",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "bena_road" / "2026-04-28.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "bena_road.json"

    assert not sales_path.exists()
    assert supervisor_path.exists()
    assert result.payload["status"] == "needs_review"
    assert result.payload["routing"]["review_reason"] == "mixed_child_requires_review"
    assert result.payload["governance"]["reasons"] == ["mixed_child_requires_review"]

    sales_child, supervisor_child = result.payload["fanout"]["children"]
    assert sales_child["report_type"] == "sales_income"
    assert sales_child["blocks_transactional_processing"] is True
    assert sales_child["metrics"]["cash_sales"] == 2205.0
    assert sales_child["metrics"]["gross_sales"] == 2575.0
    assert sales_child["metrics"]["eftpos_sales"] == 370.0
    assert sales_child["warnings"] == []

    rejection = sales_child["validation"]["rejections"][0]
    assert rejection["reason_code"] == "sales_totals_mismatch"
    assert rejection["declared_total_cash"] == 2205.0
    assert rejection["expected_total_cash"] == 2640.0
    assert rejection["declared_total_card"] == 370.0
    assert rejection["expected_total_card"] == 370.0
    assert rejection["declared_total_sales"] == 2575.0
    assert rejection["expected_total_sales"] == 3010.0
    assert supervisor_child["report_type"] == "supervisor_control"
    assert supervisor_child["blocks_transactional_processing"] is False
    assert supervisor_child["status"] == "accepted"


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")


def _mixed_child_summary(report_family: str, status: str) -> dict[str, object]:
    return {
        "report_family": report_family,
        "report_type": report_family,
        "status": status,
        "blocks_transactional_processing": report_family != "supervisor_control",
        "payload": {"warnings": []},
    }


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


def _invalid_sales_and_supervisor_control_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "Eftpos Sales: 600",
            "Traffic: 10",
            "Served: 12",
            "",
            "SUPERVISOR CONTROL REPORT",
            "Exception Type: STAFF_ISSUE",
            "Details: Late opening",
            "Action Taken: Escalated",
            "Escalated By: Francis",
            "Time: 08:30",
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


def _sales_and_incomplete_supervisor_control_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 28/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "Eftpos Sales: 600",
            "Traffic: 12",
            "Served: 9",
            "",
            "Supervisor Control Summary",
            "Notes: Skeleton team only",
        ]
    )


def _sales_and_mismatched_supervisor_control_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 28/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "Eftpos Sales: 600",
            "Traffic: 12",
            "Served: 9",
            "",
            "Supervisor Control Summary",
            "Date: 22/04/2026",
            "Cashier Reconciled: Yes",
            "Floor Check: Passed",
        ]
    )


def _sales_and_truncated_supervisor_control_summary_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
            "Date: 28/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 700",
            "Eftpos Sales: 500",
            "Traffic: 12",
            "Served: 9",
            "",
            "Supervisor Control Summary",
            "Cash variance: No",
            "Staffing issues: No",
            "Stock issues affecting sales: No",
            "Pricing or system issues: No",
            "Exceptions es\u2026",
        ]
    )


def _sales_totals_mismatch_and_supervisor_control_text() -> str:
    return "\n".join(
        [
            "Branch: Bena Road Branch",
            "Date: 28/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Till #1: Main Shop",
            "Cashier: Alice",
            "T/Cash: 2205",
            "T/Card: 345",
            "Z/Reading: 2550",
            "",
            "Till #3: Side Counter",
            "Cashier: Bob",
            "T/Cash: 435",
            "T/Card: 25",
            "Z/Reading: 460",
            "",
            "TOTALS",
            "Total Sales: 2575",
            "Total Cash: 2205",
            "Total Card: 370",
            "Traffic: 20",
            "Served: 18",
            "",
            "Supervisor Control Summary",
            "Floor Check: Passed",
            "Cashier Reconciled: Yes",
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
