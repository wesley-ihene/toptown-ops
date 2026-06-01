"""Targeted mixed-report auto-split coverage."""

from __future__ import annotations

from pathlib import Path

import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from apps.orchestrator_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def test_mixed_report_with_approved_titles_splits_sales_and_supervisor_control(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
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
                            "SUPERVISOR CONTROL REPORT",
                            "Cash variance: No",
                            "Staffing issues: No",
                            "Stock issues affecting sales: No",
                            "Pricing or system issues: No",
                            "Supervisor confirmation: Checked and closed.",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-28T12:00:00Z",
                    "sender": "mixed-auto-split-clear",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json"
    supervisor_path = tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "waigani.json"

    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["status"] in {"accepted", "accepted_with_warning"}
    assert result.payload["routing"]["review_reason"] is None
    assert sales_path.exists()
    assert supervisor_path.exists()


def test_mixed_report_without_approved_titles_stays_in_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "Branch: Waigani Branch",
                            "Date: 28/04/2026",
                            "",
                            "Sales Report",
                            "Gross Sales: 1200",
                            "Cash Sales: 700",
                            "Eftpos Sales: 500",
                            "Traffic: 12",
                            "Served: 9",
                            "",
                            "Supervisor Summary",
                            "Cash variance: No",
                            "Staffing issues: No",
                            "Stock issues affecting sales: No",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-28T12:15:00Z",
                    "sender": "mixed-auto-split-ambiguous",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["status"] == "needs_review"
    assert result.payload["routing"]["route_reason"] == "mixed_report_requires_review"
    assert result.payload["warnings"][0]["code"] == "mixed_split_review"
    assert not (tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json").exists()
    assert not (tmp_path / "records" / "intelligence" / "supervisor_control" / "2026-04-28" / "waigani.json").exists()


def test_valid_single_sales_report_still_writes_structured_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAY-END SALES REPORT",
                            "Branch: Waigani Branch",
                            "Date: 28/04/2026",
                            "Z Reading: 1200",
                            "Cash Sales: 700",
                            "Card Sales: 500",
                            "Total Sales: 1200",
                            "Traffic: 12",
                            "Served: 9",
                            "Staff On Duty: 4",
                            "Cash Variance: 0",
                            "Over Short Reason: Nil",
                            "Supervisor Confirmed: YES",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-28T12:30:00Z",
                    "sender": "single-sales-still-valid",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    assert result.payload["status"] == "accepted"
    assert (tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-28.json").exists()


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
